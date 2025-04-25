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
#include <omp.h> // For OpenMP

// --- Definitions and Constants ---

#define BATCH_SIZE          (1 << 16)      // 65,536 transactions per batch
#define NUMBER_OF_BATCHES   125000
#define SMALL_ACCOUNT_COUNT 2000000UL

// We split the full state into RING_SIZE chunks.
#define RING_SIZE           32
// Round up so every chunk is the same size; pad total accounts to STATE_CHUNK_COUNT * RING_SIZE
#define STATE_CHUNK_COUNT   ((SMALL_ACCOUNT_COUNT + RING_SIZE - 1) / RING_SIZE)
#define PADDED_ACCOUNT_COUNT (STATE_CHUNK_COUNT * RING_SIZE)
#define STATE_CHUNK_SIZE    (STATE_CHUNK_COUNT * sizeof(int64_t))

// Maximum number of transactions per chunk (worst-case)
#define MAX_TX_COUNT        (BATCH_SIZE)

// Magic number for identification.
#define CHECKPOINT_MAGIC    0xC0CAC01A

// --- Checkpoint Header Structure ---
// Now includes an "oldest_cycle" field (0 or 1).
typedef struct {
    uint32_t magic;
    uint32_t version;
    uint32_t oldest_cycle; // Cycle (0 or 1) that is the older copy.
} CheckpointHeader;

#define CHECKPOINT_HEADER_SIZE (sizeof(CheckpointHeader))

// --- Ring Log Layout ---
// We maintain two complete cycles.
#define CYCLES 2
#define TOTAL_SNAPSHOT_SLOTS (RING_SIZE * CYCLES)
#define TOTAL_TX_SLOTS       (RING_SIZE * CYCLES)
#define TOTAL_CHECKPOINT_SIZE (CHECKPOINT_HEADER_SIZE \
    + TOTAL_SNAPSHOT_SLOTS * sizeof(struct SnapshotSlot) \
    + TOTAL_TX_SLOTS * sizeof(struct TxSlot))

// --- Operation Encoding ---
// Each 64-bit field encodes an operation: upper 4 bits for op code, lower 60 bits for data.
#define FUNC_MASK   0xF000000000000000UL
#define DATA_MASK   0x0FFFFFFFFFFFFFFFUL
#define GET_FUNC(x) ((uint8_t)((x) >> 60))
#define GET_DATA(x) ((x) & DATA_MASK)

// --- Data Structures ---

// Transaction structure (unchanged).
typedef struct {
    uint64_t sender;
    uint64_t receiver;
    uint32_t amount;
} Transaction;

// Snapshot slot stores a chunk's state.
typedef struct SnapshotSlot {
    uint32_t batch_num;           // Batch when snapshot was taken
    uint32_t chunk_offset;        // Starting index in full state
    int64_t state[STATE_CHUNK_COUNT]; // The state values for this chunk
} SnapshotSlot;

// Transaction slot stores the transactions applied since the snapshot.
typedef struct TxSlot {
    uint32_t base_snapshot_slot;  // Index into the snapshot ring (0 to RING_SIZE-1)
    uint32_t batch_num;           // Batch when recorded
    uint32_t tx_count;            // Number of transactions recorded
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
    if (fd < 0) {
        perror("Error opening log file for pre-allocation");
        exit(EXIT_FAILURE);
    }
    struct stat st;
    if (fstat(fd, &st) != 0) {
        perror("fstat error");
        close(fd);
        exit(EXIT_FAILURE);
    }
    off_t fsize = st.st_size;
    off_t expected = TOTAL_CHECKPOINT_SIZE;
    if (fsize < expected) {
        if (ftruncate(fd, expected) != 0) {
            perror("ftruncate error");
            close(fd);
            exit(EXIT_FAILURE);
        }
        if (posix_fallocate(fd, 0, expected) != 0) {
            perror("posix_fallocate error");
            close(fd);
            exit(EXIT_FAILURE);
        }
        CheckpointHeader header = { CHECKPOINT_MAGIC, 1, 0 };
        if (write(fd, &header, sizeof(header)) != sizeof(header)) {
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

// --- Asynchronous Flush (msync) Thread ---
typedef struct {
    void *mapped_region;
    size_t size;
    volatile int running;
} msync_thread_data;

void *msync_thread_func(void *arg) {
    msync_thread_data *data = (msync_thread_data *)arg;
    while (data->running) {
        msync(data->mapped_region, data->size, MS_ASYNC);
        struct timespec ts = {0, 10 * 1000 * 1000}; // sleep for 10ms
        nanosleep(&ts, NULL);
    }
    return NULL;
}

// --- Recovery Function ---
int recover_state(int fd, int64_t *restrict state, int *last_batch) {
    CheckpointHeader header;
    ssize_t bytes = pread(fd, &header, sizeof header, 0);
    if (bytes != (ssize_t)sizeof header || header.magic != CHECKPOINT_MAGIC) {
        fprintf(stderr, "Invalid or missing checkpoint header; skipping recovery.\n");
        return -1;
    }

    uint32_t oldest_cycle = header.oldest_cycle;
    uint32_t newest_cycle = (oldest_cycle + 1) % CYCLES;

    SnapshotSlot *snap_slot = malloc(sizeof *snap_slot);
    TxSlot *tx_slot = malloc(sizeof *tx_slot);
    if (!snap_slot || !tx_slot) {
        perror("malloc in recover_state");
        free(tx_slot);
        free(snap_slot);
        return -1;
    }

    *last_batch = -1;

    // 1. Restore base state
    for (uint32_t chunk = 0; chunk < RING_SIZE; ++chunk) {
        off_t snap_offset = CHECKPOINT_HEADER_SIZE
                          + ((off_t)oldest_cycle * RING_SIZE + chunk)
                          * sizeof *snap_slot;
        ssize_t r = pread(fd, snap_slot, sizeof *snap_slot, snap_offset);
        if (r != (ssize_t)sizeof *snap_slot) {
            fprintf(stderr, "Failed to read snapshot slot %u (cycle %u)\n",
                    chunk, oldest_cycle);
            continue;
        }
        memcpy(state + (size_t)chunk * STATE_CHUNK_COUNT,
               snap_slot->state, STATE_CHUNK_SIZE);
        if ((int)snap_slot->batch_num > *last_batch)
            *last_batch = (int)snap_slot->batch_num;
    }

    // 2. Replay transactions
    for (uint32_t chunk = 0; chunk < RING_SIZE; ++chunk) {
        off_t tx_offset = CHECKPOINT_HEADER_SIZE
                        + TOTAL_SNAPSHOT_SLOTS * sizeof *snap_slot
                        + ((off_t)newest_cycle * RING_SIZE + chunk)
                        * sizeof *tx_slot;
        ssize_t r = pread(fd, tx_slot, sizeof *tx_slot, tx_offset);
        if (r != (ssize_t)sizeof *tx_slot) {
            fprintf(stderr, "Failed to read tx slot %u (cycle %u)\n",
                    chunk, newest_cycle);
            continue;
        }
        if (tx_slot->tx_count > MAX_TX_COUNT) {
            fprintf(stderr, "Invalid tx_count %u in slot %u (cycle %u)\n",
                    tx_slot->tx_count, chunk, newest_cycle);
            continue;
        }
        for (uint32_t j = 0; j < tx_slot->tx_count; ++j) {
            Transaction *tx = &tx_slot->transactions[j];
            uint8_t sfunc = GET_FUNC(tx->sender);
            uint8_t rfunc = GET_FUNC(tx->receiver);
            uint64_t sidx = GET_DATA(tx->sender);
            uint64_t ridx = GET_DATA(tx->receiver);
            uint64_t chunk_lo = (uint64_t)chunk * STATE_CHUNK_COUNT;
            uint64_t chunk_hi = chunk_lo + STATE_CHUNK_COUNT;
            int touches = (sidx >= chunk_lo && sidx < chunk_hi)
                        || (ridx >= chunk_lo && ridx < chunk_hi);
            if (!touches) continue;
            if (sfunc == 0 && rfunc == 0) {
                if (state[sidx] > tx->amount) state[sidx] -= tx->amount;
                if (state[ridx] > tx->amount) state[ridx] += tx->amount;
            } else if (sfunc == 1 && rfunc == 1) {
                uint64_t start = sidx;
                uint64_t len   = ridx;
                for (uint64_t k = start; k < start + len; ++k) {
                    if (k >= chunk_lo && k < chunk_hi)
                        state[k] = tx->amount;
                }
            }
        }
        if ((int)tx_slot->batch_num > *last_batch)
            *last_batch = (int)tx_slot->batch_num;
    }

    free(tx_slot);
    free(snap_slot);
    return 0;
}

// --- Commit Function ---
static void commit_chunk_v2(uint32_t cycle, uint32_t chunk_index,
                            uint32_t batch_num,
                            int64_t *restrict state,
                            Transaction **restrict tx_accum,
                            int *restrict tx_count,
                            void *restrict mapped_region,
                            TxSlot *restrict prealloc_tx_slot)
{
    size_t slot_index = cycle * RING_SIZE + chunk_index;
    size_t snap_offset = CHECKPOINT_HEADER_SIZE + slot_index * sizeof(SnapshotSlot);
    size_t tx_offset   = CHECKPOINT_HEADER_SIZE + TOTAL_SNAPSHOT_SLOTS * sizeof(SnapshotSlot)
                         + slot_index * sizeof(TxSlot);

    SnapshotSlot snap_slot;
    snap_slot.batch_num   = batch_num;
    snap_slot.chunk_offset= chunk_index * STATE_CHUNK_COUNT;
    memcpy(snap_slot.state,
           state + chunk_index * STATE_CHUNK_COUNT,
           STATE_CHUNK_SIZE);
    nt_memcpy((char*)mapped_region + snap_offset,
              &snap_slot, sizeof(SnapshotSlot));

    prealloc_tx_slot->base_snapshot_slot = chunk_index;
    prealloc_tx_slot->batch_num          = batch_num;
    prealloc_tx_slot->tx_count           = tx_count[chunk_index];
    size_t tx_bytes = prealloc_tx_slot->tx_count * sizeof(Transaction);
    memcpy(prealloc_tx_slot->transactions,
           tx_accum[chunk_index], tx_bytes);
    nt_memcpy((char*)mapped_region + tx_offset,
              prealloc_tx_slot, sizeof(TxSlot));

    tx_count[chunk_index] = 0;
}

// --- Transaction Application ---
static inline void apply_tx(const Transaction *tx,
                             int64_t *restrict state,
                             Transaction **restrict tx_accum,
                             int *restrict tx_count)
{
    uint8_t sfunc = GET_FUNC(tx->sender);
    uint8_t rfunc = GET_FUNC(tx->receiver);
    uint64_t sidx = GET_DATA(tx->sender);
    uint64_t ridx = GET_DATA(tx->receiver);

    if (sfunc == 0 && rfunc == 0) {
        if (sidx >= SMALL_ACCOUNT_COUNT || ridx >= SMALL_ACCOUNT_COUNT) {
            fprintf(stderr, "Out-of-bounds transaction index: sender %llu, receiver %llu\n",
                    (unsigned long long)sidx,
                    (unsigned long long)ridx);
            return;
        }
        if (state[sidx] > tx->amount) state[sidx] -= tx->amount;
        if (state[ridx] > tx->amount) state[ridx] += tx->amount;
        uint32_t chunk_s = sidx / STATE_CHUNK_COUNT;
        uint32_t chunk_r = ridx / STATE_CHUNK_COUNT;
        if (tx_count[chunk_s] < MAX_TX_COUNT)
            tx_accum[chunk_s][tx_count[chunk_s]++] = *tx;
        if (chunk_r != chunk_s && tx_count[chunk_r] < MAX_TX_COUNT)
            tx_accum[chunk_r][tx_count[chunk_r]++] = *tx;
    } else if (sfunc == 1 && rfunc == 1) {
        uint64_t start = sidx;
        uint64_t len   = GET_DATA(tx->receiver);
        if (start + len > SMALL_ACCOUNT_COUNT)
            return;
        for (uint64_t i = start; i < start + len; i++) {
            state[i] = tx->amount;
            uint32_t chunk = i / STATE_CHUNK_COUNT;
            if (tx_count[chunk] < MAX_TX_COUNT)
                tx_accum[chunk][tx_count[chunk]++] = *tx;
        }
    }
}

// --- Main Routine ---
#define LOG_FILE "checkpoint_log_v2.dat"
#define TX_FILE  "transactions.bin"

int main(int argc, char **argv) {
    // Allocate full state (padded) with proper alignment.
    int64_t *state;
    if (posix_memalign((void **)&state, 64,
                       PADDED_ACCOUNT_COUNT * sizeof(int64_t)) != 0) {
        perror("Error allocating state");
        exit(EXIT_FAILURE);
    }
    // Initialize real accounts; zero out padding.
    uint64_t i = 0;
    for (; i < SMALL_ACCOUNT_COUNT; i++)
        state[i] = 1000000;
    for (; i < PADDED_ACCOUNT_COUNT; i++)
        state[i] = 0;

    // Allocate per-chunk transaction accumulators.
    Transaction **tx_accum = malloc(RING_SIZE * sizeof(Transaction*));
    if (!tx_accum) { perror("Error allocating tx_accum array"); free(state); exit(EXIT_FAILURE); }
    int tx_count[RING_SIZE] = {0};
    for (uint32_t j = 0; j < RING_SIZE; j++) {
        tx_accum[j] = malloc(MAX_TX_COUNT * sizeof(Transaction));
        if (!tx_accum[j]) { perror("Error allocating tx_accum");
            while (j--) free(tx_accum[j]); free(tx_accum); free(state); exit(EXIT_FAILURE);
        }
    }

    // Preallocate TxSlot buffers for each chunk.
    TxSlot **prealloc_tx_slots = malloc(RING_SIZE * sizeof(TxSlot*));
    if (!prealloc_tx_slots) { perror("Error allocating prealloc_tx_slots"); exit(EXIT_FAILURE); }
    for (uint32_t j = 0; j < RING_SIZE; j++) {
        prealloc_tx_slots[j] = malloc(sizeof(TxSlot));
        if (!prealloc_tx_slots[j]) {
            perror("Error allocating prealloc TxSlot");
            while (j--) free(prealloc_tx_slots[j]); free(prealloc_tx_slots); exit(EXIT_FAILURE);
        }
    }

    // Recovery
    double start_recovery = get_time_ms();
    int recovered_batch = -1;
    int log_fd = open(LOG_FILE, O_RDWR);
    if (log_fd >= 0) {
        printf("Debug: Attempting recovery\n");
        if (recover_state(log_fd, state, &recovered_batch) == 0)
            printf("Recovered state up to batch %d.\n", recovered_batch);
        close(log_fd);
    }
    double end_recovery = get_time_ms();
    printf("Total recovery time : %f ms\n", end_recovery - start_recovery);

    preallocate_log_file(LOG_FILE);
    log_fd = open(LOG_FILE, O_RDWR);
    if (log_fd < 0) {
        perror("Error opening log file for writing");
        for (uint32_t j = 0; j < RING_SIZE; j++) { free(tx_accum[j]); free(prealloc_tx_slots[j]); }
        free(prealloc_tx_slots);
        free(tx_accum);
        free(state);
        exit(EXIT.FAILURE);
    }

    void *mapped_region = mmap(NULL, TOTAL_CHECKPOINT_SIZE,
                               PROT_READ|PROT_WRITE, MAP_SHARED, log_fd, 0);
    if (mapped_region == MAP_FAILED) {
        perror("mmap failed");
        close(log_fd);
        for (uint32_t j = 0; j < RING_SIZE; j++) { free(tx_accum[j]); free(prealloc_tx_slots[j]); }
        free(prealloc_tx_slots);
        free(tx_accum);
        free(state);
        exit(EXIT_FAILURE);
    }

    CheckpointHeader *header = (CheckpointHeader*)mapped_region;
    if (header->magic != CHECKPOINT_MAGIC) {
        fprintf(stderr, "Invalid checkpoint file format\n");
        munmap(mapped_region, TOTAL_CHECKPOINT_SIZE);
        close(log_fd);
        for (uint32_t j = 0; j < RING_SIZE; j++) { free(tx_accum[j]); free(prealloc_tx_slots[j]); }
        free(prealloc_tx_slots);
        free(tx_accum);
        free(state);
        exit(EXIT_FAILURE);
    }

    // Start asynchronous msync thread.
    pthread_t msync_thread;
    msync_thread_data msync_data = { mapped_region, TOTAL_CHECKPOINT_SIZE, 1 };
    if (pthread_create(&msync_thread, NULL, msync_thread_func, &msync_data) != 0) {
        perror("Error creating msync thread");
        exit(EXIT_FAILURE);
    }

    // Open transactions file.
    Transaction *transaction_batch = malloc(BATCH_SIZE * sizeof(Transaction));
    if (!transaction_batch) { perror("Failed to allocate transaction batch buffer"); exit(EXIT_FAILURE); }
    FILE *fp_transactions = fopen(TX_FILE, "rb");
    if (!fp_transactions) {
        perror("Error opening transactions file for reading");
        free(state);
        free(transaction_batch);
        exit(EXIT_FAILURE);
    }

    double *batch_times = malloc(NUMBER_OF_BATCHES * sizeof(double));
    if (!batch_times) {
        perror("Error allocating batch_times");
        free(state);
        free(transaction_batch);
        fclose(fp_transactions);
        exit(EXIT_FAILURE);
    }

    double total_processing_time = 0.0;
    double start_total = get_time_ms();

    // --- Main Batch Loop ---
    for (unsigned int batch_num = 0; batch_num < NUMBER_OF_BATCHES; batch_num++) {
        double batch_start = get_time_ms();

        size_t items = fread(transaction_batch, sizeof(Transaction), BATCH_SIZE, fp_transactions);
        if (items < BATCH_SIZE) break;

        for (unsigned int k = 0; k < BATCH_SIZE; k++) {
            apply_tx(&transaction_batch[k], state, tx_accum, tx_count);
        }

        uint32_t chunk = batch_num % RING_SIZE;
        uint32_t cycle = (batch_num / RING_SIZE) % CYCLES;
        commit_chunk_v2(cycle, chunk, batch_num,
                        state, tx_accum, tx_count,
                        mapped_region, prealloc_tx_slots[chunk]);

        if (((batch_num+1) % RING_SIZE) == 0 && batch_num > 0)
            header->oldest_cycle = cycle;

        double batch_end = get_time_ms();
        double duration = batch_end - batch_start;
        batch_times[batch_num] = duration;
        total_processing_time += duration;
        if ((batch_num % 10) == 0)
            printf("Batch %u processed in %.3f ms.\n", batch_num, duration);
    }

    msync_data.running = 0;
    pthread_join(msync_thread, NULL);

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
    for (uint32_t j = 0; j < RING_SIZE; j++) {
        free(tx_accum[j]);
        free(prealloc_tx_slots[j]);
    }
    free(prealloc_tx_slots);
    free(tx_accum);
    free(batch_times);
    free(transaction_batch);

    return 0;
}
