/* fast_batch_arm.c – high‑throughput checkpoint engine tuned for AArch64
 *
 * Build (GCC ≥ 13 / Clang 17):
 *   gcc fast_batch_arm.c -o fast_batch_arm \
 *       -Ofast -mcpu=native -march=armv8-a+sve2 \
 *       -flax-vector-conversions -flto -fopenmp \
 *       -fno-math-errno -fno-trapping-math -ffast-math -lpthread
 */

#define _GNU_SOURCE
#define _POSIX_C_SOURCE 200809L

#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>
#include <omp.h>

#ifdef __aarch64__
#include <arm_neon.h>
#endif

/* -------------------------------------------------------------------------- */
/*  Constants & Macros                                                        */
/* -------------------------------------------------------------------------- */

#define BATCH_SIZE             (1u << 16)       /* 65 536 tx per batch */
#define SMALL_ACCOUNT_COUNT    2000000UL
#define RING_SIZE              30u              /* number of state chunks */
#define STATE_CHUNK_COUNT      (SMALL_ACCOUNT_COUNT / RING_SIZE)
#define STATE_CHUNK_SIZE       (STATE_CHUNK_COUNT * sizeof(int64_t))

#define MAX_TX_COUNT           BATCH_SIZE       /* worst‑case per chunk */
#define NUMBER_OF_BATCHES      5000u

#define CHECKPOINT_MAGIC       0xC0CAC01Au
#define CYCLES                 2u               /* two cycles */
#define TOTAL_SNAPSHOT_SLOTS   (RING_SIZE * CYCLES)
#define TOTAL_TX_SLOTS         (RING_SIZE * CYCLES)
#define TOTAL_CHECKPOINT_SIZE  (sizeof(CheckpointHeader) + \
                               TOTAL_SNAPSHOT_SLOTS * sizeof(SnapshotSlot) + \
                               TOTAL_TX_SLOTS       * sizeof(TxSlot))

#define FUNC_MASK              0xF000000000000000UL
#define DATA_MASK              0x0FFFFFFFFFFFFFFFUL
#define GET_FUNC(x)            ((uint8_t)((x) >> 60))
#define GET_DATA(x)            ((x) & DATA_MASK)

/* -------------------------------------------------------------------------- */
/*  Types                                                                     */
/* -------------------------------------------------------------------------- */

typedef struct {
    uint64_t sender;
    uint64_t receiver;
    uint32_t amount;
} Transaction;

typedef struct {
    uint32_t batch_num;
    uint32_t chunk_offset;
    int64_t  state[STATE_CHUNK_COUNT];
} SnapshotSlot;

typedef struct {
    uint32_t base_snapshot_slot;
    uint32_t batch_num;
    uint32_t tx_count;
    Transaction transactions[MAX_TX_COUNT];
} TxSlot;

typedef struct {
    uint32_t magic;
    uint32_t version;
    uint32_t oldest_cycle;
} CheckpointHeader;

/* -------------------------------------------------------------------------- */
/*  Utility Functions                                                         */
/* -------------------------------------------------------------------------- */

static double now_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec * 1000.0 + ts.tv_nsec / 1e6;
}

static void *alloc_aligned(size_t bytes) {
    void *p;
    if (posix_memalign(&p, 64, bytes)) {
        perror("posix_memalign");
        exit(EXIT_FAILURE);
    }
    return p;
}

static void prepare_log_file(const char *path) {
    int fd = open(path, O_RDWR | O_CREAT, 0666);
    if (fd < 0) { perror("open log"); exit(EXIT_FAILURE); }
    struct stat st;
    if (fstat(fd, &st)) { perror("fstat"); exit(EXIT_FAILURE); }
    if ((size_t)st.st_size < TOTAL_CHECKPOINT_SIZE) {
        if (ftruncate(fd, TOTAL_CHECKPOINT_SIZE)) { perror("ftruncate"); exit(EXIT_FAILURE); }
        if (posix_fallocate(fd, 0, TOTAL_CHECKPOINT_SIZE)) { perror("fallocate"); exit(EXIT_FAILURE); }
        CheckpointHeader hdr = { CHECKPOINT_MAGIC, 1, 0 };
        if (pwrite(fd, &hdr, sizeof hdr, 0) != (ssize_t)sizeof hdr) { perror("hdr write"); exit(EXIT_FAILURE); }
        fsync(fd);
    }
    close(fd);
}

/* -------------------------------------------------------------------------- */
/*  Async msync thread                                                        */
/* -------------------------------------------------------------------------- */

typedef struct {
    void    *addr;
    size_t   len;
    volatile int keep_running;
} msync_arg_t;

static void *msync_thread(void *arg) {
    msync_arg_t *d = arg;
    struct timespec ts = {0, 10 * 1000000};
    while (d->keep_running) {
        msync(d->addr, d->len, MS_ASYNC);
        nanosleep(&ts, NULL);
    }
    return NULL;
}

/* -------------------------------------------------------------------------- */
/*  SIMD helpers                                                              */
/* -------------------------------------------------------------------------- */

#ifdef __aarch64__
static inline void range_set(int64_t *state, uint64_t start, uint64_t len, int64_t v) {
    int64x2_t vv = vdupq_n_s64(v);
    uint64_t i = 0;
    for (; i < len && ((start + i) & 1); ++i) state[start + i] = v;
    for (; i + 2 <= len; i += 2) vst1q_s64(state + start + i, vv);
    for (; i < len; ++i) state[start + i] = v;
}
#else
static inline void range_set(int64_t *state, uint64_t start, uint64_t len, int64_t v) {
    for (uint64_t i = 0; i < len; ++i) state[start + i] = v;
}
#endif

static inline void memcpy_stream(void *dst, const void *src, size_t n) {
#if defined(__aarch64__) && defined(__ARM_NEON)
    size_t off = 0;
    for (; off + 16 <= n; off += 16) {
        uint8x16_t x = vld1q_u8((const uint8_t *)src + off);
        vst1q_u8((uint8_t *)dst + off, x);
    }
    if (off < n) memcpy((uint8_t *)dst + off, (const uint8_t *)src + off, n - off);
#else
    memcpy(dst, src, n);
#endif
}

/* -------------------------------------------------------------------------- */
/*  Core logic                                                                */
/* -------------------------------------------------------------------------- */

static inline void apply_tx(const Transaction *tx, int64_t *state, Transaction **tx_accum, int *tx_count) {
    uint8_t sf = GET_FUNC(tx->sender), rf = GET_FUNC(tx->receiver);
    uint64_t si = GET_DATA(tx->sender), ri = GET_DATA(tx->receiver);
    if (sf == 0 && rf == 0) {
        if (state[si] > tx->amount) state[si] -= tx->amount;
        if (state[ri] > tx->amount) state[ri] += tx->amount;
        if (tx_accum) {
            uint32_t cs = si / STATE_CHUNK_COUNT;
            uint32_t cr = ri / STATE_CHUNK_COUNT;
            if (tx_count[cs] < MAX_TX_COUNT) tx_accum[cs][tx_count[cs]++] = *tx;
            if (cr != cs && tx_count[cr] < MAX_TX_COUNT) tx_accum[cr][tx_count[cr]++] = *tx;
        }
        return;
    }
    if (sf == 1 && rf == 1) {
        uint64_t st = si, ln = ri;
        range_set(state, st, ln, tx->amount);
        if (tx_accum) for (uint64_t k = st; k < st + ln; ++k) {
            uint32_t c = k / STATE_CHUNK_COUNT;
            if (tx_count[c] < MAX_TX_COUNT) tx_accum[c][tx_count[c]++] = *tx;
        }
    }
}

static void commit_chunk_v2(uint32_t cyc, uint32_t chunk, uint32_t batch,
                            int64_t *state, Transaction **tx_accum, int *tx_count,
                            void *map, TxSlot *buf) {
    size_t idx = cyc * RING_SIZE + chunk;
    size_t so = sizeof(CheckpointHeader) + idx * sizeof(SnapshotSlot);
    size_t to = sizeof(CheckpointHeader) + TOTAL_SNAPSHOT_SLOTS * sizeof(SnapshotSlot) + idx * sizeof(TxSlot);
    SnapshotSlot ss = {batch, chunk * STATE_CHUNK_COUNT, {0}};
    memcpy_stream(ss.state, state + chunk * STATE_CHUNK_COUNT, STATE_CHUNK_SIZE);
    memcpy((uint8_t *)map + so, &ss, sizeof ss);
    buf->base_snapshot_slot = chunk;
    buf->batch_num = batch;
    buf->tx_count = tx_count[chunk];
    memcpy(buf->transactions, tx_accum[chunk], buf->tx_count * sizeof(Transaction));
    memcpy((uint8_t *)map + to, buf, sizeof(TxSlot));
    tx_count[chunk] = 0;
}

static int recover_state(int fd, int64_t *state, int *last_batch) {
    CheckpointHeader h;
    if (pread(fd, &h, sizeof h, 0) != (ssize_t)sizeof h || h.magic != CHECKPOINT_MAGIC)
        return -1;
    uint32_t old = h.oldest_cycle, neu = (old + 1) % CYCLES;
    *last_batch = -1;
    SnapshotSlot *sn = malloc(sizeof *sn);
    TxSlot *txs = malloc(sizeof *txs);
    for (uint32_t c = 0; c < RING_SIZE; ++c) {
        off_t o = sizeof(CheckpointHeader) + (old * RING_SIZE + c) * sizeof(*sn);
        if (pread(fd, sn, sizeof *sn, o) == (ssize_t)sizeof *sn) {
            memcpy(state + c * STATE_CHUNK_COUNT, sn->state, STATE_CHUNK_SIZE);
            if ((int)sn->batch_num > *last_batch) *last_batch = sn->batch_num;
        }
    }
    for (uint32_t c = 0; c < RING_SIZE; ++c) {
        off_t o = sizeof(CheckpointHeader) + TOTAL_SNAPSHOT_SLOTS * sizeof(*sn)
                + (neu * RING_SIZE + c) * sizeof(*txs);
        if (pread(fd, txs, sizeof *txs, o) != (ssize_t)sizeof *txs || txs->tx_count > MAX_TX_COUNT)
            continue;
        for (uint32_t j = 0; j < txs->tx_count; ++j)
            apply_tx(&txs->transactions[j], state, NULL, NULL);
        if ((int)txs->batch_num > *last_batch) *last_batch = txs->batch_num;
    }
    free(txs);
    free(sn);
    return 0;
}

#define LOG_FILE "checkpoint_log_v2.dat"
#define TX_FILE  "transactions.bin"

int main(void) {
    printf("[+] starting\n");
    int64_t *state = alloc_aligned(SMALL_ACCOUNT_COUNT * sizeof *state);
    for (size_t i = 0; i < SMALL_ACCOUNT_COUNT; ++i) state[i] = 1000000;
    madvise(state, SMALL_ACCOUNT_COUNT * sizeof *state, MADV_HUGEPAGE);

    Transaction **tx_accum = malloc(RING_SIZE * sizeof *tx_accum);
    TxSlot **scratch = malloc(RING_SIZE * sizeof *scratch);
    int tx_count[RING_SIZE] = {0};
    for (uint32_t c = 0; c < RING_SIZE; ++c) {
        tx_accum[c] = malloc(MAX_TX_COUNT * sizeof(Transaction));
        scratch[c] = malloc(sizeof(TxSlot));
    }

    int log_fd = open(LOG_FILE, O_RDWR), last_batch;
    if (log_fd >= 0) {
        recover_state(log_fd, state, &last_batch);
        close(log_fd);
    }
    prepare_log_file(LOG_FILE);

    log_fd = open(LOG_FILE, O_RDWR);
    void *map = mmap(NULL, TOTAL_CHECKPOINT_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED, log_fd, 0);
    CheckpointHeader *hdr = (CheckpointHeader *)map;
    if (hdr->magic != CHECKPOINT_MAGIC) { fprintf(stderr, "bad log\n"); return 1; }

    msync_arg_t ma = { map, TOTAL_CHECKPOINT_SIZE, 1 };
    pthread_t tid;
    pthread_create(&tid, NULL, msync_thread, &ma);

    int txfd = open(TX_FILE, O_RDONLY);
    size_t txsz = lseek(txfd, 0, SEEK_END);
    Transaction *txmm = mmap(NULL, txsz, PROT_READ, MAP_PRIVATE, txfd, 0);

    double t0 = now_ms(), sum_ms = 0.0;
    #pragma omp parallel num_threads(RING_SIZE) shared(sum_ms)
    {
        int chunk_id = omp_get_thread_num();
        for (uint32_t batch = 0; batch < NUMBER_OF_BATCHES; ++batch) {
            double s = now_ms();
            Transaction *base = txmm + (size_t)batch * BATCH_SIZE;
            for (uint32_t i = 0; i < BATCH_SIZE; ++i) {
                Transaction *tx = &base[i];
                uint32_t cs = GET_DATA(tx->sender) / STATE_CHUNK_COUNT;
                uint32_t cr = GET_DATA(tx->receiver) / STATE_CHUNK_COUNT;
                if (cs != chunk_id && cr != chunk_id) continue;
                apply_tx(tx, state, tx_accum, tx_count);
            }
            uint32_t cyc = (batch / RING_SIZE) % CYCLES;
            commit_chunk_v2(cyc, chunk_id, batch, state, tx_accum, tx_count, map, scratch[chunk_id]);
            #pragma omp barrier
            if (chunk_id == 0 && ((batch + 1) % RING_SIZE) == 0)
                hdr->oldest_cycle = cyc;
            #pragma omp barrier
            if (chunk_id == 0) {
                double e = now_ms(); sum_ms += e - s;
                if ((batch % 10) == 0) printf("batch %u: %.3f ms\n", batch, e - s);
            }
        }
    }
    printf("avg batch: %.3f ms (total %.3f ms)\n", sum_ms / NUMBER_OF_BATCHES, now_ms() - t0);

    ma.keep_running = 0;
    pthread_join(tid, NULL);
    munmap(map, TOTAL_CHECKPOINT_SIZE);
    munmap(txmm, txsz);
    close(log_fd);
    close(txfd);
    return 0;
}