#define _GNU_SOURCE 1
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <pthread.h>
#include <errno.h>
#include <inttypes.h>
#include <sys/types.h>

/* ----------------------------- parameters ----------------------------- */
#define BATCH_SIZE            (1 << 16)           /* 65 536 tx per batch             */
#define SMALL_ACCOUNT_COUNT   5000000UL           /* logical accounts               */
#define ACCOUNT_SIZE_BYTES    8
#define PREFETCH_DISTANCE     8                   /* Prefetch 8 transactions ahead   */
#define WRITE_BUFFER_SIZE     (1 << 20)          /* 1MB write buffer               */

#define STATE_CHUNK_BYTES     (512 * 1024)        /* 512 KiB                          */
#define ACC_PER_CHUNK         (STATE_CHUNK_BYTES / ACCOUNT_SIZE_BYTES)
#define NUM_CHUNKS            ((SMALL_ACCOUNT_COUNT + ACC_PER_CHUNK - 1) / ACC_PER_CHUNK)

// Maximum write-set size with safety margin (4x batch size to handle worst case)
#define MAX_WRITE_SET_SIZE    (BATCH_SIZE * 4)

/* ----------------------------- file paths ----------------------------- */
#define LOG_FILE              "checkpoint_log.dat"
#define TX_FILE               "transactions.bin"
#define STATE_HASH_FILE       "state_hash.dat"

// Pre-calculate sizes to ensure proper alignment
#define HEADER_SIZE           (sizeof(uint32_t) * 2)
#define STATE_ARRAY_SIZE      (ACC_PER_CHUNK * sizeof(int64_t))
#define MAX_WS_BYTES         (MAX_WRITE_SET_SIZE * sizeof(WriteSetEntry))

static size_t SLOT_BYTES;  // Will be initialized in main()
static size_t LOG_BYTES;   // Will be initialized in main()

/* operation helpers  --------------------------------------------------- */
#define FUNC_MASK   0xF000000000000000ULL
#define DATA_MASK   0x0FFFFFFFFFFFFFFFULL
#define GET_FUNC(x) ((uint8_t)((x) >> 60))
#define GET_DATA(x) ((x) & DATA_MASK)

typedef struct { 
    uint64_t sender, receiver, amount; 
} __attribute__((packed)) Transaction;

typedef struct { 
    uint64_t addr; 
    int64_t bal; 
} __attribute__((packed)) WriteSetEntry;

typedef struct {
    uint32_t magic;
    uint32_t version;
    uint32_t chunks;
    uint32_t acc_per_chunk;
} __attribute__((packed)) LogHeader;

// Ensure 8-byte alignment for all members
typedef struct {
    uint32_t batch;
    uint32_t ws_count;
    int64_t state[ACC_PER_CHUNK];
    char padding[8 - (((sizeof(uint32_t) * 2 + ACC_PER_CHUNK * sizeof(int64_t)) % 8) % 8)];
} __attribute__((packed, aligned(8))) ChunkSlot;

#define LOG_MAGIC   0xC0CAC01B
#define LOG_VERSION 1

/* timing util (coarse = ~1 µs vs 30 ns for MONOTONIC) ------------------ */
static inline double now_ms(void)
{ struct timespec ts; clock_gettime(CLOCK_MONOTONIC,&ts); return ts.tv_sec*1e3 + ts.tv_nsec/1e6; }

/* ---------------- state hash (FNV-1a variant) ------------------------- */
static inline uint64_t fnv1a_hash(const int64_t *a, size_t n)
{
    if (!a) return 0;
    uint64_t h = 14695981039346656037ULL;
    for(size_t i=0; i<n; ++i) { 
        h ^= (uint64_t)a[i]; 
        h *= 1099511628211ULL; 
    }
    return h;
}

/* ------------------- apply_tx with safety checks ------------------- */
static inline void apply_tx(const Transaction *__restrict tx,
                          int64_t *__restrict state,
                          WriteSetEntry *__restrict ws,
                          uint32_t *ws_cnt,
                          uint32_t ws_cap)
{
    if (!tx || !state || !ws || !ws_cnt || *ws_cnt >= ws_cap) {
        return;
    }
    
    const uint8_t sf = GET_FUNC(tx->sender), rf = GET_FUNC(tx->receiver);
    const uint64_t si = GET_DATA(tx->sender), ri = GET_DATA(tx->receiver);
    
    if (sf == 0 && rf == 0) {  // Transfer operation
        if (si >= SMALL_ACCOUNT_COUNT || ri >= SMALL_ACCOUNT_COUNT || *ws_cnt + 2 > ws_cap) {
            return;
        }
        
        state[si] -= (int64_t)tx->amount; 
        state[ri] += (int64_t)tx->amount;
        ws[*ws_cnt] = (WriteSetEntry){si, state[si]}; 
        (*ws_cnt)++;
        ws[*ws_cnt] = (WriteSetEntry){ri, state[ri]}; 
        (*ws_cnt)++;
        return;
    }
    
    if (sf == 1 && rf == 1) {  // Memset operation
        uint64_t start = si, len = ri;
        if (!len || start >= SMALL_ACCOUNT_COUNT) {
            return;
        }
        
        // Adjust length to not exceed account bounds
        if (start + len > SMALL_ACCOUNT_COUNT) {
            len = SMALL_ACCOUNT_COUNT - start;
        }
        
        // For memset, we only need one write-set entry with a special encoding
        if (*ws_cnt + 1 > ws_cap) {
            return;
        }
        
        // Apply memset
        for (uint64_t i = 0; i < len; ++i) {
            state[start + i] = (int64_t)tx->amount;
        }
        
        // Store the memset operation as a single write-set entry with special encoding
        // Use the top 4 bits to indicate memset (1), and the remaining 60 bits for start address
        ws[*ws_cnt] = (WriteSetEntry){(1ULL << 60) | start, (int64_t)tx->amount};
        (*ws_cnt)++;
    }
}

/* ------------------- optimized commit thread ------------------- */
static pthread_mutex_t mt = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t cv_new = PTHREAD_COND_INITIALIZER, cv_done = PTHREAD_COND_INITIALIZER;
static int ready = 0, quit = 0;
static struct task_data {
    uint32_t slot;
    uint32_t batch;
    uint32_t ws_cnt;
    const int64_t *state_src;
    WriteSetEntry *ws;
    void *map;
} task;

/* ------------------- commit thread with enhanced error checking ------------------- */
static void *commit_thread(void *arg)
{ 
    (void)arg;
    while(1){ 
        pthread_mutex_lock(&mt); 
        while(!ready && !quit) pthread_cond_wait(&cv_new, &mt); 
        if(quit) { pthread_mutex_unlock(&mt); break; } 
        struct task_data t = task; 
        ready = 0; 
        pthread_mutex_unlock(&mt);
        
        if (!t.map || !t.state_src || !t.ws || t.ws_cnt > MAX_WRITE_SET_SIZE) {
            fprintf(stderr, "Invalid task data or write-set too large (%u)\n", t.ws_cnt);
            continue;
        }
        
        // Calculate offsets and verify bounds
        size_t slot_offset = sizeof(LogHeader) + (t.slot * SLOT_BYTES);
        if (slot_offset + SLOT_BYTES > sizeof(LogHeader) + LOG_BYTES) {
            fprintf(stderr, "Slot offset out of bounds: %zu > %zu\n", 
                    slot_offset + SLOT_BYTES, sizeof(LogHeader) + LOG_BYTES);
            continue;
        }
        
        ChunkSlot *cs = (ChunkSlot*)((char*)t.map + slot_offset);
        
        // Set header fields
        cs->batch = t.batch;
        cs->ws_count = t.ws_cnt;
        
        // Copy state data with explicit size
        memcpy(cs->state, t.state_src, STATE_ARRAY_SIZE);
        
        // Calculate write-set destination with proper alignment
        WriteSetEntry *ws_dest = (WriteSetEntry*)((char*)cs + sizeof(ChunkSlot));
        size_t ws_bytes = t.ws_cnt * sizeof(WriteSetEntry);
        
        // Verify write-set bounds
        if (ws_bytes > MAX_WS_BYTES) {
            fprintf(stderr, "Write-set too large: %zu > %zu bytes\n", ws_bytes, MAX_WS_BYTES);
            continue;
        }
        
        // Copy write-set
        memcpy(ws_dest, t.ws, ws_bytes);
        
        // Sync the used portion
        size_t sync_size = sizeof(ChunkSlot) + ws_bytes;
        msync(cs, sync_size, MS_ASYNC);
        
        pthread_mutex_lock(&mt); 
        pthread_cond_signal(&cv_done); 
        pthread_mutex_unlock(&mt);
    } 
    return NULL; 
}

static int compare_doubles(const void *a, const void *b) {
    double diff = *(const double*)a - *(const double*)b;
    return (diff < 0) ? -1 : (diff > 0) ? 1 : 0;
}

int main(void)
{
    /* geometry */
    SLOT_BYTES = sizeof(ChunkSlot) + MAX_WS_BYTES;
    LOG_BYTES = NUM_CHUNKS * SLOT_BYTES;
    
    printf("=== Memory Layout ===\n");
    printf("BATCH_SIZE: %d\n", BATCH_SIZE);
    printf("SMALL_ACCOUNT_COUNT: %lu\n", SMALL_ACCOUNT_COUNT);
    printf("ACC_PER_CHUNK: %u\n", (unsigned int)ACC_PER_CHUNK);
    printf("NUM_CHUNKS: %lu\n", NUM_CHUNKS);
    printf("\n=== Buffer Sizes ===\n");
    printf("ChunkSlot size: %zu bytes\n", sizeof(ChunkSlot));
    printf("State array size: %zu bytes\n", STATE_ARRAY_SIZE);
    printf("WriteSetEntry size: %zu bytes\n", sizeof(WriteSetEntry));
    printf("Max write-set size: %u entries (%zu bytes)\n", 
           MAX_WRITE_SET_SIZE, MAX_WS_BYTES);
    printf("Total slot size: %zu bytes\n", SLOT_BYTES);
    printf("Total log size: %zu bytes\n", LOG_BYTES);
    printf("\n=== Starting Processing ===\n");

    // Pre-allocate and initialize all resources
    int fd = open(LOG_FILE, O_RDWR|O_CREAT, 0666);
    if (fd < 0) { perror("log open"); return 1; }
    
    if (ftruncate(fd, sizeof(LogHeader) + LOG_BYTES) != 0) {
        perror("ftruncate"); close(fd); return 1;
    }

    void *map = mmap(NULL, sizeof(LogHeader) + LOG_BYTES, 
                    PROT_READ|PROT_WRITE, MAP_SHARED, fd, 0);
    if (map == MAP_FAILED) { perror("mmap"); close(fd); return 1; }

    // Advise the kernel about our access pattern
    madvise(map, sizeof(LogHeader) + LOG_BYTES, MADV_SEQUENTIAL);

    // Initialize state with aligned allocation
    int64_t *state;
    if (posix_memalign((void**)&state, 64, SMALL_ACCOUNT_COUNT * sizeof(int64_t)) != 0) {
        perror("state alloc"); munmap(map, sizeof(LogHeader) + LOG_BYTES); close(fd); return 1;
    }
    memset(state, 0, SMALL_ACCOUNT_COUNT * sizeof(int64_t));

    // Allocate write-set buffer
    WriteSetEntry *ws = aligned_alloc(64, MAX_WRITE_SET_SIZE * sizeof(WriteSetEntry));
    if (!ws) {
        perror("ws alloc"); free(state); munmap(map, sizeof(LogHeader) + LOG_BYTES); close(fd); return 1;
    }

    // Start commit thread
    pthread_t th;
    if (pthread_create(&th, NULL, commit_thread, NULL) != 0) {
        perror("thread create"); free(ws); free(state); 
        munmap(map, sizeof(LogHeader) + LOG_BYTES); close(fd); return 1;
    }

    // Process transactions
    FILE *fp = fopen(TX_FILE, "rb");
    if (!fp) {
        perror("tx open"); free(ws); free(state);
        munmap(map, sizeof(LogHeader) + LOG_BYTES); close(fd); return 1;
    }

    // Allocate transaction buffer
    Transaction *tx_buf = aligned_alloc(64, BATCH_SIZE * sizeof(Transaction));
    if (!tx_buf) {
        perror("tx buf alloc"); fclose(fp); free(ws); free(state);
        munmap(map, sizeof(LogHeader) + LOG_BYTES); close(fd); return 1;
    }

    // Process batches
    uint32_t batch = 0;
    double t0 = now_ms();
    double *times = malloc(100000 * sizeof(double));
    size_t tcnt = 0;

    while (1) {
        size_t n = fread(tx_buf, sizeof(Transaction), BATCH_SIZE, fp);
        if (!n) break;

        double t1 = now_ms();
        uint32_t ws_cnt = 0;
        
        // Process entire batch at once
        for (size_t i = 0; i < n; i++) {
            apply_tx(&tx_buf[i], state, ws, &ws_cnt, MAX_WRITE_SET_SIZE);
        }
        
        double t2 = now_ms();

        // Wait for previous commit and submit new task
        pthread_mutex_lock(&mt);
        while (ready) pthread_cond_wait(&cv_done, &mt);
        uint32_t slot = batch % NUM_CHUNKS;
        task = (struct task_data){slot, batch, ws_cnt, state + slot * ACC_PER_CHUNK, ws, map};
        ready = 1;
        pthread_cond_signal(&cv_new);
        pthread_mutex_unlock(&mt);

        if (tcnt < 100000) times[tcnt++] = t2 - t1;
        if ((batch & 127) == 0) {
            printf("batch %u app %.3f ms ws %u\n", batch, t2 - t1, ws_cnt);
        }
        batch++;
    }

    // Wait for final commit and cleanup
    pthread_mutex_lock(&mt);
    while (ready) pthread_cond_wait(&cv_done, &mt);
    quit = 1;
    pthread_cond_signal(&cv_new);
    pthread_mutex_unlock(&mt);
    pthread_join(th, NULL);

    // Performance summary
    if (tcnt) {
        qsort(times, tcnt, sizeof(double), compare_doubles);
        double avg = 0;
        for (size_t i = 0; i < tcnt; i++) avg += times[i];
        avg /= tcnt;
        printf("avg %.3f ms  med %.3f  p99 %.3f (app only)\n",
               avg, times[tcnt/2], times[(size_t)(0.99*tcnt)]);
    }

    // Cleanup
    uint64_t h = fnv1a_hash(state, SMALL_ACCOUNT_COUNT);
    FILE *hf = fopen(STATE_HASH_FILE, "wb");
    if (hf) {
        fwrite(&batch, sizeof(batch), 1, hf);
        fwrite(&h, sizeof(h), 1, hf);
        fclose(hf);
    }
    printf("saved hash 0x%016"PRIx64" for batch %u\n", h, batch-1);

    free(times);
    free(tx_buf);
    free(ws);
    free(state);
    munmap(map, sizeof(LogHeader) + LOG_BYTES);
    close(fd);
    fclose(fp);

    printf("processed %u batches in %.1f s\n", batch, (now_ms()-t0)/1e3);
    return 0;
}