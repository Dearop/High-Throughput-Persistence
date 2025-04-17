#define _POSIX_C_SOURCE 200809L
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <errno.h>
#include <pthread.h>
#include <inttypes.h>
#include <omp.h>
#ifdef __aarch64__
#include <arm_neon.h>
#endif

// --- Definitions and Constants ------------------------------------------------

#define BATCH_SIZE          (1 << 16)      // 65 536 transactions per batch
#define SMALL_ACCOUNT_COUNT 2000000UL
#define RING_SIZE           30             // number of state chunks
#define STATE_CHUNK_COUNT   (SMALL_ACCOUNT_COUNT / RING_SIZE)
#define STATE_CHUNK_SIZE    (STATE_CHUNK_COUNT * sizeof(int64_t))
#define MAX_TX_COUNT        (BATCH_SIZE)   // worst‑case per chunk
#define NUMBER_OF_BATCHES   5000         // total to process (file must hold ≥ this)

#define CHECKPOINT_MAGIC    0xC0CAC01A
#define CYCLES              2
#define TOTAL_SNAPSHOT_SLOTS (RING_SIZE * CYCLES)
#define TOTAL_TX_SLOTS       (RING_SIZE * CYCLES)
#define TOTAL_CHECKPOINT_SIZE (sizeof(CheckpointHeader) + \
                               TOTAL_SNAPSHOT_SLOTS * sizeof(struct SnapshotSlot) + \
                               TOTAL_TX_SLOTS       * sizeof(struct TxSlot))

#define FUNC_MASK 0xF000000000000000UL
#define DATA_MASK 0x0FFFFFFFFFFFFFFFUL
#define GET_FUNC(x) ((uint8_t)((x) >> 60))
#define GET_DATA(x) ((x) & DATA_MASK)

// --- Data Structures ----------------------------------------------------------

typedef struct { uint64_t sender, receiver; uint32_t amount; } Transaction;

typedef struct SnapshotSlot {
    uint32_t batch_num;
    uint32_t chunk_offset;
    int64_t  state[STATE_CHUNK_COUNT];
} SnapshotSlot;

typedef struct TxSlot {
    uint32_t base_snapshot_slot;
    uint32_t batch_num;
    uint32_t tx_count;
    Transaction transactions[MAX_TX_COUNT];
} TxSlot;

typedef struct { uint32_t magic, version, oldest_cycle; } CheckpointHeader;

// --- Utility ------------------------------------------------------------------
static double get_time_ms(void) {
    struct timespec ts; clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec * 1000.0 + ts.tv_nsec / 1e6;
}

static void preallocate_log_file(const char *filename) {
    int fd = open(filename, O_RDWR | O_CREAT, 0666);
    if(fd < 0) { perror("open log"); exit(EXIT_FAILURE);}    
    struct stat st; if(fstat(fd, &st)) { perror("fstat"); exit(EXIT_FAILURE);}    
    if(st.st_size < TOTAL_CHECKPOINT_SIZE) {
        if(ftruncate(fd, TOTAL_CHECKPOINT_SIZE)) { perror("ftruncate"); exit(EXIT_FAILURE);}        
        if(posix_fallocate(fd, 0, TOTAL_CHECKPOINT_SIZE)) { perror("fallocate"); exit(EXIT_FAILURE);}        
        CheckpointHeader hdr = { CHECKPOINT_MAGIC, 1, 0 };
        if(pwrite(fd, &hdr, sizeof hdr, 0) != (ssize_t)sizeof hdr) { perror("hdr write"); exit(EXIT_FAILURE);}        
        fsync(fd);
    }
    close(fd);
}

static inline void *aligned_alloc64(size_t bytes) {
    void *p; if(posix_memalign(&p, 64, bytes)) { perror("posix_memalign"); exit(EXIT_FAILURE);} return p;
}

// --- Asynchronous msync thread ------------------------------------------------

typedef struct { void *addr; size_t len; volatile int running; } msync_thr_data;
static void *msync_thread(void *arg) {
    msync_thr_data *d = arg;
    struct timespec ts = {0, 10*1000*1000}; // 10 ms
    while(d->running) { msync(d->addr, d->len, MS_ASYNC); nanosleep(&ts, NULL);}    
    return NULL;
}

// --- SIMD helpers -------------------------------------------------------------
#ifdef __aarch64__
static inline void range_set_vec(int64_t *state, uint64_t start, uint64_t len, int64_t value) {
    uint64x2_t vv = vdupq_n_s64(value);
    uint64_t i = 0;
    // prologue – align to 16‑byte boundary (two int64_t)
    for(; i < len && ((start+i) & 1); ++i) state[start+i] = value;
    // main loop – 2 accounts per iteration
    for(; i + 2 <= len; i += 2) vst1q_s64(state + start + i, vv);
    // epilogue
    for(; i < len; ++i) state[start+i] = value;
}
#else
static inline void range_set_vec(int64_t *state, uint64_t start, uint64_t len, int64_t value) {
    for(uint64_t i = 0; i < len; ++i) state[start+i] = value;
}
#endif

// Streaming memcpy (fallbacks to memcpy if intrinsics unavailable) ------------
static inline void memcpy_stream(void *dst, const void *src, size_t n) {
#if defined(__aarch64__) && defined(__ARM_NEON)
    size_t off = 0; for(; off + 16 <= n; off += 16) {
        uint8x16_t v = vld1q_u8((const uint8_t*)src + off);
        vst1q_u8((uint8_t*)dst + off, v); // not truly NT but avoids function call
    }
    if(off < n) memcpy((uint8_t*)dst + off, (const uint8_t*)src + off, n - off);
#else
    memcpy(dst, src, n);
#endif
}

// --- Global per‑chunk locks ---------------------------------------------------

typedef struct { pthread_mutex_t m; char pad[64 - sizeof(pthread_mutex_t)]; } mutex64_t;
static mutex64_t chunk_lock[RING_SIZE] = { [0 ... RING_SIZE-1] = { PTHREAD_MUTEX_INITIALIZER } };

// --- Transaction application --------------------------------------------------
static inline void apply_tx(const Transaction *tx,
                            int64_t *restrict state,
                            Transaction **restrict tx_accum,
                            int *restrict tx_count)
{
    uint8_t sfunc = GET_FUNC(tx->sender);
    uint8_t rfunc = GET_FUNC(tx->receiver);
    uint64_t sidx = GET_DATA(tx->sender);
    uint64_t ridx = GET_DATA(tx->receiver);

    if(sfunc == 0 && rfunc == 0) {
        if(state[sidx] > tx->amount) state[sidx] -= tx->amount;
        if(state[ridx] > tx->amount) state[ridx] += tx->amount;
        uint32_t cs = sidx / STATE_CHUNK_COUNT;
        uint32_t cr = ridx / STATE_CHUNK_COUNT;
        if(tx_count[cs] < MAX_TX_COUNT) tx_accum[cs][tx_count[cs]++] = *tx;
        if(cr != cs && tx_count[cr] < MAX_TX_COUNT) tx_accum[cr][tx_count[cr]++] = *tx;
    } else if(sfunc == 1 && rfunc == 1) {
        uint64_t start = sidx, len = ridx;
        range_set_vec(state, start, len, tx->amount);
        for(uint64_t k = start; k < start + len; ++k) {
            uint32_t c = k / STATE_CHUNK_COUNT;
            if(tx_count[c] < MAX_TX_COUNT) tx_accum[c][tx_count[c]++] = *tx;
        }
    }
}

// --- Commit -------------------------------------------------------------------
static void commit_chunk(uint32_t cycle, uint32_t chunk_idx, uint32_t batch_num,
                         int64_t *state, Transaction **tx_accum, int *tx_count,
                         void *map, TxSlot *buf)
{
    size_t slot = cycle * RING_SIZE + chunk_idx;
    size_t snap_off = sizeof(CheckpointHeader) + slot * sizeof(SnapshotSlot);
    size_t tx_off   = sizeof(CheckpointHeader) + TOTAL_SNAPSHOT_SLOTS * sizeof(SnapshotSlot) + slot * sizeof(TxSlot);

    SnapshotSlot snap = { batch_num, chunk_idx * STATE_CHUNK_COUNT, {0} };
    memcpy_stream(snap.state, state + chunk_idx * STATE_CHUNK_COUNT, STATE_CHUNK_SIZE);
    memcpy(map + snap_off, &snap, sizeof snap);

    buf->base_snapshot_slot = chunk_idx;
    buf->batch_num = batch_num;
    buf->tx_count  = tx_count[chunk_idx];
    memcpy(buf->transactions, tx_accum[chunk_idx], buf->tx_count * sizeof(Transaction));
    memcpy(map + tx_off, buf, sizeof(TxSlot));

    tx_count[chunk_idx] = 0;
}

// --- Recovery (serial) – unchanged from previous answer -----------------------
static int recover_state(int fd, int64_t *state, int *last_batch);

// --- File names ---------------------------------------------------------------
#define LOG_FILE "checkpoint_log_v2.dat"
#define TX_FILE  "transactions.bin"

int main(void)
{
    printf("[+] start\n");

    // ------------------- 1. allocate state ----------------------------------
    int64_t *state = aligned_alloc64(SMALL_ACCOUNT_COUNT * sizeof *state);
    for(uint64_t i = 0; i < SMALL_ACCOUNT_COUNT; ++i) state[i] = 1'000'000;
    madvise(state, SMALL_ACCOUNT_COUNT * sizeof *state, MADV_HUGEPAGE);

    // ------------- 2. per‑chunk accumulators + TxSlot buffers ---------------
    Transaction **tx_accum = malloc(RING_SIZE * sizeof *tx_accum);
    TxSlot      **pre_tx   = malloc(RING_SIZE * sizeof *pre_tx);
    int           tx_count[RING_SIZE] = {0};
    for(uint32_t c=0;c<RING_SIZE;++c) {
        tx_accum[c] = malloc(MAX_TX_COUNT * sizeof(Transaction));
        pre_tx  [c] = malloc(sizeof(TxSlot));
    }

    // ------------------- 3. recovery ----------------------------------------
    int log_fd = open(LOG_FILE, O_RDWR); int recovered = -1;
    if(log_fd >= 0) { recover_state(log_fd, state, &recovered); close(log_fd);}    

    // ------------------- 4. open / mmap log ---------------------------------
    preallocate_log_file(LOG_FILE);
    log_fd = open(LOG_FILE, O_RDWR);
    void *map = mmap(NULL, TOTAL_CHECKPOINT_SIZE, PROT_READ|PROT_WRITE, MAP_SHARED, log_fd, 0);

    CheckpointHeader *hdr = map; if(hdr->magic != CHECKPOINT_MAGIC) { fprintf(stderr,"bad log file\n"); exit(EXIT_FAILURE);}    

    // ------------------- 5. msync thread ------------------------------------
    msync_thr_data mt = { map, TOTAL_CHECKPOINT_SIZE, 1 }; pthread_t mtid;
    pthread_create(&mtid, NULL, msync_thread, &mt);

    // ------------------- 6. mmap transactions file --------------------------
    int tfd = open(TX_FILE, O_RDONLY); if(tfd < 0) { perror("open tx file"); exit(EXIT_FAILURE);}    
    size_t tx_size = lseek(tfd, 0, SEEK_END);
    Transaction *tx_mm = mmap(NULL, tx_size, PROT_READ, MAP_PRIVATE, tfd, 0);
    size_t total_batches = tx_size / (BATCH_SIZE * sizeof(Transaction));
    if(total_batches < NUMBER_OF_BATCHES) {
        fprintf(stderr,"transactions.bin too small (have %zu, need %u)\n", total_batches, NUMBER_OF_BATCHES); exit(EXIT_FAILURE);
    }

    // ------------------- 7. batch loop --------------------------------------
    double start_total = get_time_ms(), sum_batch_ms = 0.0;
    for(uint32_t batch=0; batch<NUMBER_OF_BATCHES; ++batch) {
        double t0=get_time_ms();
        Transaction *base = tx_mm + batch*BATCH_SIZE;

        // (a) parallel apply --------------------------------------------------
        #pragma omp parallel for schedule(static)
        for(uint32_t i=0;i<BATCH_SIZE;++i) {
            const Transaction *tx = &base[i];
            uint32_t cs = GET_DATA(tx->sender)   / STATE_CHUNK_COUNT;
            uint32_t cr = GET_DATA(tx->receiver) / STATE_CHUNK_COUNT;
            uint32_t first  = cs < cr ? cs : cr;
            uint32_t second = cs ^ cr ^ first;   // same when cs==cr

            pthread_mutex_lock(&chunk_lock[first].m);
            if(second!=first) pthread_mutex_lock(&chunk_lock[second].m);

            apply_tx(tx, state, tx_accum, tx_count);

            pthread_mutex_unlock(&chunk_lock[second].m);
            if(second!=first) pthread_mutex_unlock(&chunk_lock[first].m);
        }

        // (b) commit the one dirty chunk -------------------------------------
        uint32_t chunk = batch % RING_SIZE;
        uint32_t cycle = (batch / RING_SIZE) % CYCLES;
        commit_chunk(cycle, chunk, batch, state, tx_accum, tx_count, map, pre_tx[chunk]);
        if(((batch+1) % RING_SIZE)==0) hdr->oldest_cycle = cycle;

        double t1=get_time_ms(); sum_batch_ms += (t1-t0);
        if(batch % 10==0) printf("batch %u \t %.3f ms\n", batch, t1-t0);
    }

    // ------------------- 8. shutdown ----------------------------------------
    mt.running = 0; pthread_join(mtid,NULL);

    munmap(map, TOTAL_CHECKPOINT_SIZE); close(log_fd);
    munmap(tx_mm, tx_size); close(tfd);

    printf("average batch: %.3f ms (total %.3f)\n", sum_batch_ms/NUMBER_OF_BATCHES, get_time_ms()-start_total);

    // free memory ------------------------------------------------------------
    for(uint32_t c=0;c<RING_SIZE;++c){ free(tx_accum[c]); free(pre_tx[c]); }
    free(tx_accum); free(pre_tx); free(state);
    return 0;
}

// -----------------------------------------------------------------------------
//  Recovery (unchanged from earlier serial version).  Moved to bottom so main
//  can call it without a forward declaration clutter.
// -----------------------------------------------------------------------------
static int recover_state(int fd, int64_t *state, int *last_batch)
{
    CheckpointHeader hdr; if(pread(fd,&hdr,sizeof hdr,0)!=(ssize_t)sizeof hdr||hdr.magic!=CHECKPOINT_MAGIC) return -1;
    uint32_t old = hdr.oldest_cycle, new = (old+1)%CYCLES; *last_batch=-1;

    SnapshotSlot *snap = malloc(sizeof *snap); TxSlot *txs = malloc(sizeof *txs);
    for(uint32_t ch=0; ch<RING_SIZE;++ch){ off_t o = sizeof(CheckpointHeader)+(old*RING_SIZE+ch)*sizeof*snap;
        if(pread(fd,snap,sizeof*snap,o)==(ssize_t)sizeof*snap){
            memcpy(state+ch*STATE_CHUNK_COUNT,snap->state,STATE_CHUNK_SIZE);
            if((int)snap->batch_num>*last_batch) *last_batch=snap->batch_num;
        }}
    for(uint32_t ch=0; ch<RING_SIZE;++ch){ off_t o = sizeof(CheckpointHeader)+TOTAL_SNAPSHOT_SLOTS*sizeof*snap+(new*RING_SIZE+ch)*sizeof*txs;
        if(pread(fd,txs,sizeof*txs,o)!=(ssize_t)sizeof*txs||txs->tx_count>MAX_TX_COUNT) continue;
        for(uint32_t j=0;j<txs->tx_count;++j) apply_tx(&txs->transactions[j],state,NULL,NULL);
        if((int)txs->batch_num>*last_batch) *last_batch=txs->batch_num; }
    free(txs); free(snap); return 0;
}
