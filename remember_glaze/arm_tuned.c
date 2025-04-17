/* fast_batch_arm.c – high‑throughput checkpoint engine tuned for AArch64
 *
 * Build (GCC ≥ 13 or Clang 17):
 *   gcc fast_batch_arm.c -o fast_batch_arm \
 *       -O3 -mcpu=native -flto -fopenmp \
 *       -fno-math-errno -fno-trapping-math -ffast-math
 */

#define _GNU_SOURCE                 /* for MADV_HUGEPAGE */
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
/*  Constants & macros                                                        */
/* -------------------------------------------------------------------------- */

#define BATCH_SIZE             (1u << 16)       /* 65 536 tx per batch */
#define SMALL_ACCOUNT_COUNT    2000000UL
#define RING_SIZE              30u              /* chunks */
#define STATE_CHUNK_COUNT      (SMALL_ACCOUNT_COUNT / RING_SIZE)
#define STATE_CHUNK_SIZE       (STATE_CHUNK_COUNT * sizeof(int64_t))

#define MAX_TX_COUNT           BATCH_SIZE       /* per chunk worst-case */
#define NUMBER_OF_BATCHES      5000u

#define CHECKPOINT_MAGIC       0xC0CAC01Au
#define CYCLES                 2u
#define TOTAL_SNAPSHOT_SLOTS   (RING_SIZE * CYCLES)
#define TOTAL_TX_SLOTS         (RING_SIZE * CYCLES)
#define TOTAL_CHECKPOINT_SIZE  (sizeof(CheckpointHeader) + \
                               TOTAL_SNAPSHOT_SLOTS * sizeof(SnapshotSlot) + \
                               TOTAL_TX_SLOTS       * sizeof(TxSlot))

#define GET_FUNC(x)            ((uint8_t)((x) >> 60))
#define GET_DATA(x)            ((x) & 0x0FFFFFFFFFFFFFFFUL)

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
/*  Utility                                                                   */
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
    struct timespec sleep_ts = { 0, 10 * 1000000 };
    while (d->keep_running) {
        msync(d->addr, d->len, MS_ASYNC);
        nanosleep(&sleep_ts, NULL);
    }
    return NULL;
}

/* -------------------------------------------------------------------------- */
/*  SIMD helper for range-set                                                 */
/* -------------------------------------------------------------------------- */

#ifdef __aarch64__
static inline void range_set(int64_t *state, uint64_t start, uint64_t len, int64_t value) {
    int64x2_t vv = vdupq_n_s64(value);
    uint64_t i = 0;
    for (; i < len && ((start + i) & 1); ++i) state[start + i] = value;
    for (; i + 2 <= len; i += 2) vst1q_s64(state + start + i, vv);
    for (; i < len; ++i) state[start + i] = value;
}
#else
static inline void range_set(int64_t *state, uint64_t start, uint64_t len, int64_t value) {
    for (uint64_t i = 0; i < len; ++i) state[start + i] = value;
}
#endif

static inline void memcpy_stream(void *dst, const void *src, size_t n) {
#if defined(__aarch64__) && defined(__ARM_NEON)
    size_t off = 0;
    for (; off + 16 <= n; off += 16) {
        uint8x16_t v = vld1q_u8((const uint8_t *)src + off);
        vst1q_u8((uint8_t *)dst + off, v);
    }
    if (off < n) memcpy((uint8_t *)dst + off, (const uint8_t *)src + off, n - off);
#else
    memcpy(dst, src, n);
#endif
}

/* -------------------------------------------------------------------------- */
/*  Per-chunk locks (avoid false sharing)                                      */
/* -------------------------------------------------------------------------- */

typedef struct {
    pthread_mutex_t m;
    char pad[64 - sizeof(pthread_mutex_t)];
} mutex64_t;

static mutex64_t chunk_lock[RING_SIZE] = { [0 ... RING_SIZE-1] = { PTHREAD_MUTEX_INITIALIZER } };

/* -------------------------------------------------------------------------- */
/*  Core logic: apply_tx & commit_chunk                                       */
/* -------------------------------------------------------------------------- */

static inline void apply_tx(const Transaction *tx,
                            int64_t           *state,
                            Transaction      **tx_accum,
                            int               *tx_count) {
    uint8_t sf = GET_FUNC(tx->sender), rf = GET_FUNC(tx->receiver);
    uint64_t si = GET_DATA(tx->sender), ri = GET_DATA(tx->receiver);
    if (sf == 0 && rf == 0) {
        if (state[si] > tx->amount) state[si] -= tx->amount;
        if (state[ri] > tx->amount) state[ri] += tx->amount;
        if (tx_accum) {
            uint32_t cs = si/STATE_CHUNK_COUNT;
            uint32_t cr = ri/STATE_CHUNK_COUNT;
            if (tx_count[cs] < MAX_TX_COUNT) tx_accum[cs][tx_count[cs]++] = *tx;
            if (cr!=cs && tx_count[cr] < MAX_TX_COUNT) tx_accum[cr][tx_count[cr]++] = *tx;
        }
        return;
    }
    if (sf == 1 && rf == 1) {
        uint64_t start=si, len=ri;
        range_set(state,start,len,tx->amount);
        if (tx_accum) for (uint64_t k=start; k<start+len; ++k) {
            uint32_t c = k/STATE_CHUNK_COUNT;
            if (tx_count[c] < MAX_TX_COUNT) tx_accum[c][tx_count[c]++] = *tx;
        }
    }
}

static void commit_chunk(uint32_t cycle, uint32_t chunk, uint32_t batch,
                         int64_t *state, Transaction **tx_accum, int *tx_count,
                         void *map, TxSlot *scratch) {
    size_t idx    = cycle*RING_SIZE + chunk;
    size_t snap_off = sizeof(CheckpointHeader) + idx*sizeof(SnapshotSlot);
    size_t tx_off   = sizeof(CheckpointHeader)
                      + TOTAL_SNAPSHOT_SLOTS*sizeof(SnapshotSlot)
                      + idx*sizeof(TxSlot);

    SnapshotSlot ss = { batch, chunk*STATE_CHUNK_COUNT, {0} };
    memcpy_stream(ss.state, state + chunk*STATE_CHUNK_COUNT, STATE_CHUNK_SIZE);
    memcpy((uint8_t*)map + snap_off, &ss, sizeof ss);

    scratch->base_snapshot_slot = chunk;
    scratch->batch_num          = batch;
    scratch->tx_count           = tx_count[chunk];
    memcpy(scratch->transactions, tx_accum[chunk], scratch->tx_count*sizeof(Transaction));
    memcpy((uint8_t*)map + tx_off, scratch, sizeof(TxSlot));

    tx_count[chunk] = 0;
}

/* -------------------------------------------------------------------------- */
/*  Recovery (serial)                                                         */
/* -------------------------------------------------------------------------- */

static int recover_state(int fd, int64_t *state, int *last_batch) {
    CheckpointHeader hdr;
    if (pread(fd, &hdr, sizeof hdr, 0)!=(ssize_t)sizeof hdr || hdr.magic!=CHECKPOINT_MAGIC)
        return -1;
    uint32_t old = hdr.oldest_cycle;
    uint32_t neu = (old+1)%CYCLES;
    *last_batch=-1;

    SnapshotSlot *snap = malloc(sizeof* snap);
    TxSlot       *txs  = malloc(sizeof* txs);
    for (uint32_t c=0; c<RING_SIZE; ++c) {
        off_t o = sizeof(CheckpointHeader)+(old*RING_SIZE+c)*sizeof* snap;
        if (pread(fd, snap, sizeof* snap, o)==(ssize_t)sizeof* snap) {
            memcpy(state + c*STATE_CHUNK_COUNT, snap->state, STATE_CHUNK_SIZE);
            if ((int)snap->batch_num > *last_batch) *last_batch=snap->batch_num;
        }
    }
    for (uint32_t c=0; c<RING_SIZE; ++c) {
        off_t o = sizeof(CheckpointHeader)
                + TOTAL_SNAPSHOT_SLOTS*sizeof* snap
                + (neu*RING_SIZE+c)*sizeof* txs;
        if (pread(fd, txs, sizeof* txs, o)!=(ssize_t)sizeof* txs || txs->tx_count>MAX_TX_COUNT)
            continue;
        for (uint32_t j=0; j<txs->tx_count; ++j)
            apply_tx(&txs->transactions[j], state, NULL, NULL);
        if ((int)txs->batch_num > *last_batch) *last_batch=txs->batch_num;
    }
    free(txs); free(snap);
    return 0;
}

/* -------------------------------------------------------------------------- */
/*  Main                                                                      */
/* -------------------------------------------------------------------------- */

#define LOG_FILE "checkpoint_log_v2.dat"
#define TX_FILE  "transactions.bin"

int main(void) {
    printf("[+] starting\n");

    /* 1. allocate state and advise huge pages */
    int64_t *state = alloc_aligned(SMALL_ACCOUNT_COUNT*sizeof *state);
    for (size_t i=0; i<SMALL_ACCOUNT_COUNT; ++i) state[i]=1000000;
    madvise(state, SMALL_ACCOUNT_COUNT*sizeof *state, MADV_HUGEPAGE);

    /* 2. alloc per-chunk accumulators*/
    Transaction **tx_accum = malloc(RING_SIZE*sizeof *tx_accum);
    TxSlot      **scratch = malloc(RING_SIZE*sizeof *scratch);
    int tx_count[RING_SIZE] = {0};
    for (uint32_t c=0; c<RING_SIZE; ++c) {
        tx_accum[c] = malloc(MAX_TX_COUNT*sizeof(Transaction));
        scratch[c]  = malloc(sizeof(TxSlot));
    }

    /* 3. recover */
    int log_fd = open(LOG_FILE,O_RDWR), last_batch=-1;
    if (log_fd>=0) { recover_state(log_fd,state,&last_batch); close(log_fd); }
    prepare_log_file(LOG_FILE);

    /* 4. mmap checkpoint log */
    log_fd = open(LOG_FILE,O_RDWR);
    void *map = mmap(NULL, TOTAL_CHECKPOINT_SIZE, PROT_READ|PROT_WRITE, MAP_SHARED, log_fd, 0);
    CheckpointHeader *hdr = map;
    if (hdr->magic!=CHECKPOINT_MAGIC) { fprintf(stderr,"bad log\n"); return 1; }

    /* 5. start msync thread */
    msync_arg_t ma = { map, TOTAL_CHECKPOINT_SIZE, 1 };
    pthread_t tid; pthread_create(&tid,NULL,msync_thread,&ma);

    /* 6. mmap transactions */
    int txfd = open(TX_FILE,O_RDONLY);
    size_t txsz = lseek(txfd,0,SEEK_END);
    Transaction *txmm = mmap(NULL,txsz,PROT_READ,MAP_PRIVATE,txfd,0);

    /* 7. batch loop */
    double t0=now_ms(), sum_ms=0;
    for (uint32_t b=0; b<NUMBER_OF_BATCHES; ++b) {
        double s=now_ms();
        Transaction *base = txmm + (size_t)b*BATCH_SIZE;
        #pragma omp parallel for schedule(static)
        for (uint32_t i=0;i<BATCH_SIZE;++i) {
            const Transaction *tx=&base[i];
            uint32_t cs = GET_DATA(tx->sender)/STATE_CHUNK_COUNT;
            uint32_t cr = GET_DATA(tx->receiver)/STATE_CHUNK_COUNT;
            uint32_t a = cs<cr?cs:cr;
            uint32_t d = cs^cr^a;
            pthread_mutex_lock(&chunk_lock[a].m);
            if (d!=a) pthread_mutex_lock(&chunk_lock[d].m);
            apply_tx(tx,state,tx_accum,tx_count);
            pthread_mutex_unlock(&chunk_lock[d].m);
            if (d!=a) pthread_mutex_unlock(&chunk_lock[a].m);
        }
        uint32_t ch=b%RING_SIZE;
        uint32_t cyc=(b/RING_SIZE)%CYCLES;
        commit_chunk(cyc,ch,b,state,tx_accum,tx_count,map,scratch[ch]);
        if ((b+1)%RING_SIZE==0) hdr->oldest_cycle=cyc;
        double e=now_ms(); sum_ms += e-s;
        if ((b%10)==0) printf("batch %u: %.3f ms\n",b,e-s);
    }

    /* 8. shutdown */
    ma.keep_running=0; pthread_join(tid,NULL);
    munmap(map,TOTAL_CHECKPOINT_SIZE);
    munmap(txmm,txsz);
    close(log_fd); close(txfd);

    printf("avg batch: %.3f ms (total %.3f)\n", sum_ms/NUMBER_OF_BATCHES, now_ms()-t0);
    return 0;
}