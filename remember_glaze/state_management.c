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

// --- Definitions and Constants ---

#define BATCH_SIZE          (1 << 16)      // 65,536 transactions per batch
#define NUMBER_OF_BATCHES   50
#define SMALL_ACCOUNT_COUNT 2000000UL

// We split the full state into RING_SIZE chunks.
#define RING_SIZE           10
#define STATE_CHUNK_COUNT   (SMALL_ACCOUNT_COUNT / RING_SIZE)
#define STATE_CHUNK_SIZE    (STATE_CHUNK_COUNT * sizeof(int64_t))

// Maximum number of transactions per chunk (worst-case)
#define MAX_TX_COUNT        (BATCH_SIZE)

// Magic number for identification.
#define CHECKPOINT_MAGIC    0xC0CAC01A

// --- Checkpoint Header Structure ---
typedef struct {
    uint32_t magic;
    uint32_t version;
    uint32_t oldest_cycle; // Cycle (0 or 1) that is the older copy.
} CheckpointHeader;
#define CHECKPOINT_HEADER_SIZE (sizeof(CheckpointHeader))

// --- Ring Log Layout ---
#define CYCLES 2
#define TOTAL_SNAPSHOT_SLOTS (RING_SIZE * CYCLES)
#define TOTAL_TX_SLOTS       (RING_SIZE * CYCLES)
#define TOTAL_CHECKPOINT_SIZE (CHECKPOINT_HEADER_SIZE + TOTAL_SNAPSHOT_SLOTS * sizeof(struct SnapshotSlot) + TOTAL_TX_SLOTS * sizeof(struct TxSlot))

// --- Operation Encoding ---
#define FUNC_MASK   0xF000000000000000UL
#define DATA_MASK   0x0FFFFFFFFFFFFFFFUL
#define GET_FUNC(x) ((uint8_t)((x) >> 60))
#define GET_DATA(x) ((x) & DATA_MASK)

// --- Likely/Unlikely Macros ---
#define likely(x)       __builtin_expect((x),1)
#define unlikely(x)     __builtin_expect((x),0)

// --- Data Structures ---

// Transaction structure.
typedef struct {
    uint64_t sender;
    uint64_t receiver;
    uint32_t amount;
} Transaction;

// Snapshot slot stores a chunk's state.
typedef struct SnapshotSlot {
    uint32_t batch_num;             // Batch when snapshot was taken
    uint32_t chunk_offset;          // Starting index in full state
    int64_t state[STATE_CHUNK_COUNT]; // The state values for this chunk
} SnapshotSlot;

// Transaction slot stores the transactions applied since the snapshot.
typedef struct TxSlot {
    uint32_t base_snapshot_slot;    // Index into the snapshot ring (0 to RING_SIZE-1)
    uint32_t batch_num;             // Batch when recorded
    uint32_t tx_count;              // Number of transactions recorded
    Transaction transactions[MAX_TX_COUNT]; // The transactions
} TxSlot;

// --- Utility Functions ---

// Return current time in milliseconds.
double get_time_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec * 1000.0 + ts.tv_nsec / 1e6;
}

// Preallocate the checkpoint file and write header if needed.
void preallocate_log_file(const char *filename) {
    int fd = open(filename, O_RDWR | O_CREAT, 0666);
    if(fd < 0) {
        perror("Error opening log file for pre-allocation");
        exit(EXIT_FAILURE);
    }
    struct stat st;
    if(fstat(fd, &st) != 0) {
        perror("fstat error");
        close(fd);
        exit(EXIT_FAILURE);
    }
    off_t fsize = st.st_size;
    off_t expected = TOTAL_CHECKPOINT_SIZE;
    if(fsize < expected) {
        if(ftruncate(fd, expected) != 0) {
            perror("ftruncate error");
            close(fd);
            exit(EXIT_FAILURE);
        }
        if(posix_fallocate(fd, 0, expected) != 0) {
            perror("posix_fallocate error");
            close(fd);
            exit(EXIT_FAILURE);
        }
        CheckpointHeader header = { CHECKPOINT_MAGIC, 1, 0 };
        if(write(fd, &header, sizeof(header)) != sizeof(header)) {
            perror("Failed to write checkpoint header");
            close(fd);
            exit(EXIT_FAILURE);
        }
        fsync(fd);
    }
    close(fd);
}

// Simple memcpy wrapper.
static inline void nt_memcpy(void *dest, const void *src, size_t n) {
    memcpy(dest, src, n);
}

// --- Recovery Function ---
// Recovers the state from the checkpoint file.
int recover_state(int fd, int64_t *restrict state, int *last_batch) {
    CheckpointHeader header;
    ssize_t bytes = pread(fd, &header, sizeof(header), 0);
    if(bytes != sizeof(header) || header.magic != CHECKPOINT_MAGIC) {
        fprintf(stderr, "Invalid or missing checkpoint header; skipping recovery.\n");
        return 0;
    }
    uint32_t oldest_cycle = header.oldest_cycle;
    uint32_t newest_cycle = (oldest_cycle + 1) % CYCLES;

    TxSlot *tx_slot = malloc(sizeof(TxSlot));
    if(!tx_slot) { perror("malloc tx_slot failed"); exit(EXIT_FAILURE); }
    SnapshotSlot *snap_slot = malloc(sizeof(SnapshotSlot));
    if(!snap_slot) { perror("malloc snap_slot failed"); free(tx_slot); exit(EXIT_FAILURE); }

    #pragma omp parallel for
    for(uint32_t chunk = 0; chunk < RING_SIZE; chunk++) {
        off_t snap_offset = CHECKPOINT_HEADER_SIZE + ((oldest_cycle * RING_SIZE + chunk) * sizeof(SnapshotSlot));
        ssize_t b = pread(fd, snap_slot, sizeof(SnapshotSlot), snap_offset);
        if(b != sizeof(SnapshotSlot)) {
            fprintf(stderr, "Failed to read snapshot slot %u from cycle %u\n", chunk, oldest_cycle);
            continue;
        }
        memcpy(state + chunk * STATE_CHUNK_COUNT, snap_slot->state, STATE_CHUNK_SIZE);
        #pragma omp critical
        {
            if(snap_slot->batch_num > (uint32_t)(*last_batch))
                *last_batch = snap_slot->batch_num;
        }
    }
    
    #pragma omp parallel for
    for(uint32_t chunk = 0; chunk < RING_SIZE; chunk++) {
        off_t tx_offset = CHECKPOINT_HEADER_SIZE + TOTAL_SNAPSHOT_SLOTS * sizeof(SnapshotSlot) +
                          ((newest_cycle * RING_SIZE + chunk) * sizeof(TxSlot));
        ssize_t b = pread(fd, tx_slot, sizeof(TxSlot), tx_offset);
        if(b != sizeof(TxSlot)) {
            fprintf(stderr, "Failed to read tx slot %u from cycle %u\n", chunk, newest_cycle);
            continue;
        }
        if(tx_slot->tx_count > MAX_TX_COUNT) {
            fprintf(stderr, "Invalid tx_count %u in slot %u from cycle %u\n", tx_slot->tx_count, chunk, newest_cycle);
            continue;
        }
        for(uint32_t j = 0; j < tx_slot->tx_count; j++) {
            Transaction *tx = &tx_slot->transactions[j];
            uint64_t sender_idx = GET_DATA(tx->sender);
            uint64_t receiver_idx = GET_DATA(tx->receiver);
            int applies = 0;
            if(sender_idx >= chunk * STATE_CHUNK_COUNT && sender_idx < (chunk + 1) * STATE_CHUNK_COUNT)
                applies = 1;
            if(receiver_idx >= chunk * STATE_CHUNK_COUNT && receiver_idx < (chunk + 1) * STATE_CHUNK_COUNT)
                applies = 1;
            if(!applies) continue;
            
            uint8_t sfunc = GET_FUNC(tx->sender);
            uint8_t rfunc = GET_FUNC(tx->receiver);
            if(sfunc == 0 && rfunc == 0) {
                if(state[sender_idx] > tx->amount)
                    state[sender_idx] -= tx->amount;
                if(state[receiver_idx] > tx->amount)
                    state[receiver_idx] += tx->amount;
            } else if(sfunc == 1 && rfunc == 1) {
                uint64_t start = GET_DATA(tx->sender);
                uint64_t len   = GET_DATA(tx->receiver);
                for(uint64_t k = start; k < start + len; k++) {
                    if(k >= chunk * STATE_CHUNK_COUNT && k < (chunk + 1) * STATE_CHUNK_COUNT)
                        state[k] = tx->amount;
                }
            }
        }
        #pragma omp critical
        {
            if(tx_slot->batch_num > (uint32_t)(*last_batch))
                *last_batch = tx_slot->batch_num;
        }
    }
    
    free(tx_slot);
    free(snap_slot);
    return 0;
}

// --- Synchronous Commit Function ---
// This version flushes the affected portions immediately with msync (MS_SYNC) and fsync.
static void commit_chunk_v2(uint32_t cycle, uint32_t chunk_index,
                            uint32_t batch_num,
                            int64_t *restrict state,
                            Transaction **restrict tx_accum,
                            int *restrict tx_count,
                            void *restrict mapped_region,
                            TxSlot *restrict prealloc_tx_slot,
                            int log_fd)  // now passed log_fd for fsync
{
    size_t slot_index = cycle * RING_SIZE + chunk_index;
    size_t snap_offset = CHECKPOINT_HEADER_SIZE + slot_index * sizeof(SnapshotSlot);
    size_t tx_offset = CHECKPOINT_HEADER_SIZE + TOTAL_SNAPSHOT_SLOTS * sizeof(SnapshotSlot) + slot_index * sizeof(TxSlot);
    
    SnapshotSlot snap_slot;
    snap_slot.batch_num = batch_num;
    snap_slot.chunk_offset = chunk_index * STATE_CHUNK_COUNT;
    memcpy(snap_slot.state, state + chunk_index * STATE_CHUNK_COUNT, STATE_CHUNK_SIZE);
    nt_memcpy((char*)mapped_region + snap_offset, &snap_slot, sizeof(SnapshotSlot));
    
    prealloc_tx_slot->base_snapshot_slot = chunk_index;
    prealloc_tx_slot->batch_num = batch_num;
    prealloc_tx_slot->tx_count = tx_count[chunk_index];
    size_t tx_bytes = prealloc_tx_slot->tx_count * sizeof(Transaction);
    memcpy(prealloc_tx_slot->transactions, tx_accum[chunk_index], tx_bytes);
    nt_memcpy((char*)mapped_region + tx_offset, prealloc_tx_slot, sizeof(TxSlot));
    
    tx_count[chunk_index] = 0;
    
    // Synchronously flush each updated region.
    if(msync((char*)mapped_region + snap_offset, sizeof(SnapshotSlot), MS_SYNC) < 0) {
        perror("msync snapshot slot failed");
    }
    if(msync((char*)mapped_region + tx_offset, sizeof(TxSlot), MS_SYNC) < 0) {
        perror("msync transaction slot failed");
    }
    if(fsync(log_fd) < 0) {
        perror("fsync failed");
    }
}

// --- Transaction Application Function ---
// This inline function applies a transaction in order.
static inline void apply_tx(const Transaction *tx,
                             int64_t *restrict state,
                             Transaction **restrict tx_accum,
                             int *restrict tx_count)
{
    uint8_t sfunc = GET_FUNC(tx->sender);
    uint8_t rfunc = GET_FUNC(tx->receiver);
    uint64_t sidx = GET_DATA(tx->sender);
    uint64_t ridx = GET_DATA(tx->receiver);
    
    if(likely(sfunc == 0 && rfunc == 0)) {
        if(unlikely(sidx >= SMALL_ACCOUNT_COUNT || ridx >= SMALL_ACCOUNT_COUNT)) {
            fprintf(stderr, "Out-of-bounds transaction index: sender %llu, receiver %llu\n",
                    (unsigned long long)sidx, (unsigned long long)ridx);
            return;
        }
        if(state[sidx] > tx->amount)
            state[sidx] -= tx->amount;
        if(state[ridx] > tx->amount)
            state[ridx] += tx->amount;
        uint32_t chunk_s = sidx / STATE_CHUNK_COUNT;
        uint32_t chunk_r = ridx / STATE_CHUNK_COUNT;
        if(tx_count[chunk_s] < MAX_TX_COUNT) {
            tx_accum[chunk_s][tx_count[chunk_s]] = *tx;
            tx_count[chunk_s]++;
        }
        if(chunk_r != chunk_s && tx_count[chunk_r] < MAX_TX_COUNT) {
            tx_accum[chunk_r][tx_count[chunk_r]] = *tx;
            tx_count[chunk_r]++;
        }
    }
    else if(sfunc == 1 && rfunc == 1) {
        uint64_t start = sidx;
        uint64_t len   = GET_DATA(tx->receiver);
        if(start + len > SMALL_ACCOUNT_COUNT)
            return;
        for(uint64_t i = start; i < start + len; i++) {
            state[i] = tx->amount;
            uint32_t chunk = i / STATE_CHUNK_COUNT;
            if(tx_count[chunk] < MAX_TX_COUNT) {
                tx_accum[chunk][tx_count[chunk]] = *tx;
                tx_count[chunk]++;
            }
        }
    }
}

// --- Double-buffered Transaction Reader ---
typedef struct {
    Transaction *buffers[2]; // Two buffers
    int current;             // Index of the buffer ready for processing
    int ready[2];            // 1 means ready, 0 otherwise
    pthread_mutex_t mutex;
    pthread_cond_t cond;
    FILE *fp;
    int batches_read;
} tx_double_buffer_t;

void *reader_thread_func(void *arg) {
    tx_double_buffer_t *db = (tx_double_buffer_t *)arg;
    int buf = 0;
    while(db->batches_read < NUMBER_OF_BATCHES) {
        size_t items = fread(db->buffers[buf], sizeof(Transaction), BATCH_SIZE, db->fp);
        pthread_mutex_lock(&db->mutex);
        db->ready[buf] = 1;
        pthread_cond_signal(&db->cond);
        while(db->ready[buf] == 1)
            pthread_cond_wait(&db->cond, &db->mutex);
        pthread_mutex_unlock(&db->mutex);
        buf = 1 - buf;
        db->batches_read++;
    }
    return NULL;
}

// --- Main Routine ---
#define LOG_FILE "checkpoint_log_v2.dat"
#define TX_FILE  "transactions.bin"

int main(int argc, char **argv) {
    printf("Debug: Starting program\n");
    
    // Allocate full state.
    int64_t *state;
    if(posix_memalign((void **)&state, 64, SMALL_ACCOUNT_COUNT * sizeof(int64_t)) != 0) {
        perror("Error allocating state");
        exit(EXIT_FAILURE);
    }
    for(uint64_t i = 0; i < SMALL_ACCOUNT_COUNT; i++) {
        state[i] = 1000000;
    }
    
    // Allocate per-chunk transaction accumulators.
    Transaction **tx_accum = malloc(RING_SIZE * sizeof(Transaction *));
    if(!tx_accum) { perror("Error allocating tx_accum array"); free(state); exit(EXIT_FAILURE); }
    int tx_count[RING_SIZE] = {0};
    for(uint32_t i = 0; i < RING_SIZE; i++) {
        tx_accum[i] = malloc(MAX_TX_COUNT * sizeof(Transaction));
        if(!tx_accum[i]) {
            perror("Error allocating tx accumulator");
            for(uint32_t j = 0; j < i; j++) free(tx_accum[j]);
            free(tx_accum);
            free(state);
            exit(EXIT_FAILURE);
        }
    }
    
    // Preallocate TxSlot buffers for each chunk.
    TxSlot **prealloc_tx_slots = malloc(RING_SIZE * sizeof(TxSlot *));
    if(!prealloc_tx_slots) { perror("Error allocating prealloc_tx_slots"); exit(EXIT_FAILURE); }
    for(uint32_t i = 0; i < RING_SIZE; i++) {
        prealloc_tx_slots[i] = malloc(sizeof(TxSlot));
        if(!prealloc_tx_slots[i]) {
            perror("Error allocating prealloc TxSlot");
            for(uint32_t j = 0; j < i; j++) free(prealloc_tx_slots[j]);
            free(prealloc_tx_slots);
            exit(EXIT_FAILURE);
        }
    }
    
    // Perform recovery if possible.
    int recovered_batch = -1;
    int log_fd = open(LOG_FILE, O_RDWR);
    if(log_fd >= 0) {
        printf("Debug: Attempting recovery\n");
        if(recover_state(log_fd, state, &recovered_batch) == 0)
            printf("Recovered state up to batch %d.\n", recovered_batch);
        close(log_fd);
    }
    
    preallocate_log_file(LOG_FILE);
    log_fd = open(LOG_FILE, O_RDWR);
    if(log_fd < 0) {
        perror("Error opening log file for writing");
        for(uint32_t i = 0; i < RING_SIZE; i++) { free(tx_accum[i]); free(prealloc_tx_slots[i]); }
        free(prealloc_tx_slots);
        free(tx_accum);
        free(state);
        exit(EXIT_FAILURE);
    }
    
    void *mapped_region = mmap(NULL, TOTAL_CHECKPOINT_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED, log_fd, 0);
    if(mapped_region == MAP_FAILED) {
        perror("mmap failed");
        close(log_fd);
        for(uint32_t i = 0; i < RING_SIZE; i++) { free(tx_accum[i]); free(prealloc_tx_slots[i]); }
        free(prealloc_tx_slots);
        free(tx_accum);
        free(state);
        exit(EXIT_FAILURE);
    }
    
    CheckpointHeader *header = (CheckpointHeader *)mapped_region;
    if(header->magic != CHECKPOINT_MAGIC) {
        fprintf(stderr, "Invalid checkpoint file format\n");
        munmap(mapped_region, TOTAL_CHECKPOINT_SIZE);
        close(log_fd);
        for(uint32_t i = 0; i < RING_SIZE; i++) { free(tx_accum[i]); free(prealloc_tx_slots[i]); }
        free(prealloc_tx_slots);
        free(tx_accum);
        free(state);
        exit(EXIT_FAILURE);
    }
    
    // Remove asynchronous msync thread; we use synchronous flush in commit_chunk_v2.
    
    // Set up double-buffering for reading transactions.
    tx_double_buffer_t db;
    pthread_mutex_init(&db.mutex, NULL);
    pthread_cond_init(&db.cond, NULL);
    db.buffers[0] = malloc(BATCH_SIZE * sizeof(Transaction));
    db.buffers[1] = malloc(BATCH_SIZE * sizeof(Transaction));
    db.current = 0;
    db.ready[0] = 0;
    db.ready[1] = 0;
    db.batches_read = 0;
    FILE *fp_transactions = fopen(TX_FILE, "rb");
    if(!fp_transactions) {
        perror("Error opening transactions file for reading");
        exit(EXIT_FAILURE);
    }
    db.fp = fp_transactions;
    pthread_t reader_thread;
    if(pthread_create(&reader_thread, NULL, reader_thread_func, &db) != 0) {
        perror("Error creating reader thread");
        exit(EXIT_FAILURE);
    }
    
    double *batch_times = malloc(NUMBER_OF_BATCHES * sizeof(double));
    if(!batch_times) {
        perror("Error allocating batch_times");
        exit(EXIT_FAILURE);
    }
    
    double total_processing_time = 0.0;
    double start_total = get_time_ms();
    
    // --- Main Batch Loop ---
    for(unsigned int batch_num = 0; batch_num < NUMBER_OF_BATCHES; batch_num++) {
        double batch_start = get_time_ms();
        // Wait for the next batch to be ready.
        pthread_mutex_lock(&db.mutex);
        while(db.ready[db.current] == 0)
            pthread_cond_wait(&db.cond, &db.mutex);
        pthread_mutex_unlock(&db.mutex);
        
        // Process the batch.
        Transaction *transaction_batch = db.buffers[db.current];
        for(unsigned int i = 0; i < BATCH_SIZE; i++) {
            apply_tx(&transaction_batch[i], state, tx_accum, tx_count);
        }
        
        // Mark the buffer as processed.
        pthread_mutex_lock(&db.mutex);
        db.ready[db.current] = 0;
        pthread_cond_signal(&db.cond);
        pthread_mutex_unlock(&db.mutex);
        db.current = 1 - db.current;
        
        // Commit the chunk for this batch with synchronous flush.
        uint32_t chunk = batch_num % RING_SIZE;
        uint32_t cycle = (batch_num / RING_SIZE) % CYCLES;
        commit_chunk_v2(cycle, chunk, batch_num, state, tx_accum, tx_count,
                        mapped_region, prealloc_tx_slots[chunk], log_fd);
        if(((batch_num + 1) % RING_SIZE == 0) && (batch_num > 0))
            header->oldest_cycle = cycle;
        
        double batch_end = get_time_ms();
        double duration = batch_end - batch_start;
        batch_times[batch_num] = duration;
        total_processing_time += duration;
        if((batch_num % 10) == 0)
            printf("Batch %u processed in %.3f ms.\n", batch_num, duration);
    }
    
    pthread_join(reader_thread, NULL);
    munmap(mapped_region, TOTAL_CHECKPOINT_SIZE);
    fclose(fp_transactions);
    close(log_fd);
    
    double end_total = get_time_ms();
    double average = total_processing_time / NUMBER_OF_BATCHES;
    printf("Total processing time: %.3f ms\n", total_processing_time);
    printf("Average batch time: %.3f ms\n", average);
    printf("Total time: %.3f ms\n", end_total - start_total);
    
    // Cleanup.
    free(state);
    for(uint32_t i = 0; i < RING_SIZE; i++) {
        free(tx_accum[i]);
        free(prealloc_tx_slots[i]);
    }
    free(prealloc_tx_slots);
    free(tx_accum);
    free(batch_times);
    free(db.buffers[0]);
    free(db.buffers[1]);
    pthread_mutex_destroy(&db.mutex);
    pthread_cond_destroy(&db.cond);
    
    return 0;
}