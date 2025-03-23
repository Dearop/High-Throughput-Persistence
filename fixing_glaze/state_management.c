#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <errno.h>

// --- Definitions and constants ---

// Transaction and state parameters.
#define BATCH_SIZE          (1 << 16)          // 2^16 transactions per batch
#define NUMBER_OF_BATCHES   50                 
#define SMALL_ACCOUNT_COUNT 2000000UL         

// We split the full state into 8 chunks.
#define RING_SIZE 10
#define STATE_CHUNK_COUNT (SMALL_ACCOUNT_COUNT / RING_SIZE)
#define STATE_CHUNK_SIZE  (STATE_CHUNK_COUNT * sizeof(int64_t))

// For each transaction, worst-case a transaction produces 2 modifications.
// Over a full cycle (8 batches) the worst-case is 16 * BATCH_SIZE modifications per chunk.
#define MAX_WRITE_SET_COUNT (16 * BATCH_SIZE)
#define WRITE_SET_CHUNK_SIZE (MAX_WRITE_SET_COUNT * sizeof(WriteSetEntry))

// --- Crash Resistance: Checkpoint Header ---
#define CHECKPOINT_MAGIC 0xC0CAC01A

typedef struct {
    uint32_t magic;            // Must equal CHECKPOINT_MAGIC when completely written.
    uint32_t batch_num;        // Latest batch number in this cycle.
    uint32_t chunk_offset;     // Starting index in the full state array.
    uint32_t state_chunk_count;// Should equal STATE_CHUNK_COUNT.
    uint32_t write_set_count;  // Number of valid write-set entries.
    uint32_t checksum;         // Checksum computed over state chunk and write-set area.
} CheckpointHeader;

#define CHECKPOINT_HEADER_SIZE (sizeof(CheckpointHeader))
#define CHECKPOINT_SLOT_SIZE   (CHECKPOINT_HEADER_SIZE + STATE_CHUNK_SIZE + WRITE_SET_CHUNK_SIZE)

#define TX_FILE "transactions.bin"
#define LOG_FILE "checkpoint_log.dat"

// --- Data Structures ---

typedef struct {
    uint64_t sender;
    uint64_t receiver;
    uint32_t amount;
} Transaction;

typedef struct {
    uint64_t address;
    int64_t balance;
} WriteSetEntry;

// --- Utility Functions ---

// Simple 32-bit FNV-1a checksum.
uint32_t compute_checksum(const void *data, size_t size) {
    const unsigned char *p = data;
    uint32_t hash = 2166136261U;
    for (size_t i = 0; i < size; i++) {
        hash ^= p[i];
        hash *= 16777619U;
    }
    return hash;
}

int compare_doubles(const void *a, const void *b) {
    double diff = (*(const double *)a) - (*(const double *)b);
    return (diff < 0) ? -1 : (diff > 0) ? 1 : 0;
}

uint64_t fnv1a_hash(int64_t *data, size_t len) {
    uint64_t hash = 14695981039346656037UL;
    for (size_t i = 0; i < len; i++) {
        uint64_t val = (uint64_t)data[i];
        for (int j = 0; j < 8; j++) {
            uint8_t byte = (val >> (j * 8)) & 0xFF;
            hash ^= byte;
            hash *= 1099511628211UL;
        }
    }
    return hash;
}

double get_time_ms() {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec * 1000.0 + ts.tv_nsec / 1e6;
}

void preallocate_log_file_posix(const char *filename) {
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
    off_t expected = RING_SIZE * CHECKPOINT_SLOT_SIZE;
    if (fsize < expected) {
         if (ftruncate(fd, expected) != 0) {
              perror("ftruncate error");
              close(fd);
              exit(EXIT_FAILURE);
         }
         fsync(fd);
    }
    close(fd);
}

void write_state_to_file(const char *filename, int64_t *state, size_t count) {
    FILE *fp = fopen(filename, "w");
    if (!fp) {
        perror("Error opening state text file for writing");
        return;
    }
    for (size_t i = 0; i < count; i++) {
        fprintf(fp, "%zu: %lld\n", i, (long long)state[i]);
    }
    fclose(fp);
}

// --- Recovery ---
// Read each checkpoint slot and reassemble the full state from valid slots.
int reconstruct_state(int fd, int64_t *state, int *last_batch) {
    for (uint32_t slot = 0; slot < RING_SIZE; slot++) {
        off_t offset = slot * CHECKPOINT_SLOT_SIZE;
        CheckpointHeader header;
        ssize_t bytes = pread(fd, &header, sizeof(header), offset);
        if (bytes != sizeof(header)) {
            fprintf(stderr, "Failed to read header for slot %u\n", slot);
            continue;
        }
        if (header.magic != CHECKPOINT_MAGIC) {
            fprintf(stderr, "Slot %u has invalid magic (0x%x)\n", slot, header.magic);
            continue;
        }
        if (header.state_chunk_count != STATE_CHUNK_COUNT) {
            fprintf(stderr, "Slot %u has unexpected state_chunk_count\n", slot);
            continue;
        }
        int64_t *chunk = malloc(STATE_CHUNK_SIZE);
        if (!chunk) {
            perror("Allocation error during recovery");
            continue;
        }
        bytes = pread(fd, chunk, STATE_CHUNK_SIZE, offset + CHECKPOINT_HEADER_SIZE);
        if (bytes != STATE_CHUNK_SIZE) {
            perror("Error reading state chunk during recovery");
            free(chunk);
            continue;
        }
        void *ws_area = malloc(WRITE_SET_CHUNK_SIZE);
        if (!ws_area) {
            perror("Allocation error during recovery (write set)");
            free(chunk);
            continue;
        }
        bytes = pread(fd, ws_area, WRITE_SET_CHUNK_SIZE, offset + CHECKPOINT_HEADER_SIZE + STATE_CHUNK_SIZE);
        if (bytes != WRITE_SET_CHUNK_SIZE) {
            perror("Error reading write set during recovery");
            free(chunk);
            free(ws_area);
            continue;
        }
        uint32_t cs1 = compute_checksum(chunk, STATE_CHUNK_SIZE);
        uint32_t cs2 = compute_checksum(ws_area, WRITE_SET_CHUNK_SIZE);
        if ((cs1 ^ cs2) != header.checksum) {
            fprintf(stderr, "Slot %u checksum mismatch! (Computed: 0x%x, Expected: 0x%x)\n",
                    slot, cs1 ^ cs2, header.checksum);
            free(chunk);
            free(ws_area);
            continue;
        }
        memcpy(state + header.chunk_offset, chunk, STATE_CHUNK_SIZE);
        free(chunk);
        free(ws_area);
        *last_batch = header.batch_num;
    }
    return 0;
}

// --- Transaction Application ---
// For each transaction, update the state and record modifications into the appropriate accumulator.
void apply_transaction(const Transaction *tx, int64_t *state,
                       WriteSetEntry **ws_accum, int *ws_count) {
    if (tx->sender < SMALL_ACCOUNT_COUNT) {
        state[tx->sender] -= tx->amount;
        uint32_t chunk = tx->sender / STATE_CHUNK_COUNT;
        ws_accum[chunk][ws_count[chunk]].address = tx->sender;
        ws_accum[chunk][ws_count[chunk]].balance = state[tx->sender];
        ws_count[chunk]++;
    }
    if (tx->receiver < SMALL_ACCOUNT_COUNT) {
        state[tx->receiver] += tx->amount;
        uint32_t chunk = tx->receiver / STATE_CHUNK_COUNT;
        ws_accum[chunk][ws_count[chunk]].address = tx->receiver;
        ws_accum[chunk][ws_count[chunk]].balance = state[tx->receiver];
        ws_count[chunk]++;
    }
}

// --- Main Function ---
// Process batches. Every RING_SIZE batches (or at the end), write a full checkpoint cycle.
// For each checkpoint slot, write the state chunk and write-set area, then commit the slot by writing the header and calling fsync.
int main(int argc, char **argv) {
    int64_t *state = calloc(SMALL_ACCOUNT_COUNT, sizeof(int64_t));
    if (!state) {
        perror("Error allocating state");
        exit(EXIT_FAILURE);
    }
    
    // Allocate per-chunk write-set accumulators.
    WriteSetEntry *ws_accum[RING_SIZE];
    int ws_count[RING_SIZE] = {0};
    for (uint32_t i = 0; i < RING_SIZE; i++) {
        ws_accum[i] = malloc(WRITE_SET_CHUNK_SIZE);
        if (!ws_accum[i]) {
            perror("Error allocating write-set accumulator");
            exit(EXIT_FAILURE);
        }
    }
    
    // Recovery.
    int recovered_batch = -1;
    int log_fd = open(LOG_FILE, O_RDWR);
    if (log_fd >= 0) {
        if (reconstruct_state(log_fd, state, &recovered_batch) == 0) {
            printf("Reconstructed full state from log (up to batch %d).\n", recovered_batch);
            uint64_t hash = fnv1a_hash(state, SMALL_ACCOUNT_COUNT);
            FILE *hash_fp = fopen("state_hash.dat", "rb");
            if (hash_fp) {
                uint64_t stored_hash;
                fread(&stored_hash, sizeof(uint64_t), 1, hash_fp);
                fclose(hash_fp);
                if (hash == stored_hash)
                    printf("Reconstructed state hash matches stored hash: %llu\n", hash);
                else {
                    printf("Reconstructed state hash mismatch! Computed: %llu, Stored: %llu\n", hash, stored_hash);
                    write_state_to_file("reconstructed_state.txt", state, SMALL_ACCOUNT_COUNT);
                }
            }
        }
        close(log_fd);
        if (argc > 1 && strcmp(argv[1], "recover") == 0) {
            for (uint32_t i = 0; i < RING_SIZE; i++) free(ws_accum[i]);
            free(state);
            return 0;
        }
    }
    
    preallocate_log_file_posix(LOG_FILE);
    log_fd = open(LOG_FILE, O_RDWR);
    if (log_fd < 0) {
         perror("Error opening log file for writing");
         free(state);
         exit(EXIT_FAILURE);
    }
    
    // Allocate a reusable transaction batch buffer.
    Transaction *transaction_batch = malloc(BATCH_SIZE * sizeof(Transaction));
    if (!transaction_batch) {
        perror("Failed to allocate transaction batch buffer");
        exit(EXIT_FAILURE);
    }
    
    FILE *fp_transactions = fopen(TX_FILE, "rb");
    if (!fp_transactions) {
         perror("Error opening transactions file for reading");
         free(state);
         free(transaction_batch);
         close(log_fd);
         exit(EXIT_FAILURE);
    }
    
    double *batch_times = malloc(NUMBER_OF_BATCHES * sizeof(double));
    if (!batch_times) {
        perror("Error allocating batch_times");
        free(state);
        free(transaction_batch);
        fclose(fp_transactions);
        close(log_fd);
        exit(EXIT_FAILURE);
    }
    double total_processing_time = 0.0;
    uint64_t start_total = get_time_ms();
    
    for (unsigned int batch_num = 0; batch_num < NUMBER_OF_BATCHES; batch_num++) {
        double batch_start = get_time_ms();
        size_t items = fread(transaction_batch, sizeof(Transaction), BATCH_SIZE, fp_transactions);
        if (items != BATCH_SIZE) {
            if (feof(fp_transactions)) break;
            else {
                perror("Error reading transactions file");
                break;
            }
        }
        for (unsigned int i = 0; i < BATCH_SIZE; i++) {
            apply_transaction(&transaction_batch[i], state, ws_accum, ws_count);
        }
        double batch_end = get_time_ms();
        
        // Every RING_SIZE batches, write a full checkpoint cycle.
        if ((batch_num + 1) % RING_SIZE == 0) {
            for (uint32_t slot = 0; slot < RING_SIZE; slot++) {
                int64_t *state_snapshot = malloc(STATE_CHUNK_SIZE);
                if (!state_snapshot) {
                    perror("Error allocating state snapshot");
                    exit(EXIT_FAILURE);
                }
                memcpy(state_snapshot, state + slot * STATE_CHUNK_COUNT, STATE_CHUNK_SIZE);
                uint32_t cs1 = compute_checksum(state_snapshot, STATE_CHUNK_SIZE);
                uint32_t cs2 = compute_checksum(ws_accum[slot], WRITE_SET_CHUNK_SIZE);
                uint32_t combined_checksum = cs1 ^ cs2;
                
                CheckpointHeader header;
                header.magic = CHECKPOINT_MAGIC;
                header.batch_num = batch_num;  // Latest batch in this cycle.
                header.chunk_offset = slot * STATE_CHUNK_COUNT;
                header.state_chunk_count = STATE_CHUNK_COUNT;
                header.write_set_count = ws_count[slot];
                header.checksum = combined_checksum;
                
                off_t offset = slot * CHECKPOINT_SLOT_SIZE;
                if (pwrite(log_fd, state_snapshot, STATE_CHUNK_SIZE, offset + CHECKPOINT_HEADER_SIZE) != STATE_CHUNK_SIZE)
                    perror("Error writing state snapshot");
                if (pwrite(log_fd, ws_accum[slot], WRITE_SET_CHUNK_SIZE, offset + CHECKPOINT_HEADER_SIZE + STATE_CHUNK_SIZE) != WRITE_SET_CHUNK_SIZE)
                    perror("Error writing write set to log");
                if (pwrite(log_fd, &header, CHECKPOINT_HEADER_SIZE, offset) != CHECKPOINT_HEADER_SIZE)
                    perror("Error writing checkpoint header");
                // Call fsync after writing each slot.
                fsync(log_fd);
                free(state_snapshot);
                ws_count[slot] = 0;
            }
        }
        double duration = batch_end - batch_start;
        batch_times[batch_num] = duration;
        total_processing_time += duration;
        printf("Batch %u processed in %.3f ms.\n", batch_num, duration);
    }
    
    // Final checkpoint cycle if NUMBER_OF_BATCHES is not a multiple of RING_SIZE.
    if (NUMBER_OF_BATCHES % RING_SIZE != 0) {
        void *empty_ws = calloc(1, WRITE_SET_CHUNK_SIZE);
        if (!empty_ws) {
            perror("Error allocating empty write-set area");
            exit(EXIT_FAILURE);
        }
        for (uint32_t slot = 0; slot < RING_SIZE; slot++) {
            int64_t *state_snapshot = malloc(STATE_CHUNK_SIZE);
            if (!state_snapshot) {
                perror("Error allocating state snapshot");
                exit(EXIT_FAILURE);
            }
            memcpy(state_snapshot, state + slot * STATE_CHUNK_COUNT, STATE_CHUNK_SIZE);
            uint32_t cs1 = compute_checksum(state_snapshot, STATE_CHUNK_SIZE);
            uint32_t cs2 = compute_checksum(empty_ws, WRITE_SET_CHUNK_SIZE);
            uint32_t combined_checksum = cs1 ^ cs2;
            
            CheckpointHeader header;
            header.magic = CHECKPOINT_MAGIC;
            header.batch_num = NUMBER_OF_BATCHES - 1;
            header.chunk_offset = slot * STATE_CHUNK_COUNT;
            header.state_chunk_count = STATE_CHUNK_COUNT;
            header.write_set_count = 0;
            header.checksum = combined_checksum;
            
            off_t offset = slot * CHECKPOINT_SLOT_SIZE;
            if (pwrite(log_fd, state_snapshot, STATE_CHUNK_SIZE, offset + CHECKPOINT_HEADER_SIZE) != STATE_CHUNK_SIZE)
                perror("Error writing final state snapshot");
            if (pwrite(log_fd, empty_ws, WRITE_SET_CHUNK_SIZE, offset + CHECKPOINT_HEADER_SIZE + STATE_CHUNK_SIZE) != WRITE_SET_CHUNK_SIZE)
                perror("Error writing final write set to log");
            if (pwrite(log_fd, &header, CHECKPOINT_HEADER_SIZE, offset) != CHECKPOINT_HEADER_SIZE)
                perror("Error writing final checkpoint header");
            fsync(log_fd);
            free(state_snapshot);
        }
        free(empty_ws);
    }
    
    uint64_t end_total = get_time_ms();
    fclose(fp_transactions);
    close(log_fd);
    
    double average = total_processing_time / NUMBER_OF_BATCHES;
    double *sorted_times = malloc(NUMBER_OF_BATCHES * sizeof(double));
    if (!sorted_times) {
        perror("Error allocating sorted_times");
        free(state);
        free(batch_times);
        exit(EXIT_FAILURE);
    }
    memcpy(sorted_times, batch_times, NUMBER_OF_BATCHES * sizeof(double));
    qsort(sorted_times, NUMBER_OF_BATCHES, sizeof(double), compare_doubles);
    double median = sorted_times[NUMBER_OF_BATCHES / 2];
    double p90 = sorted_times[(int)(NUMBER_OF_BATCHES * 0.9) - 1];
    double p99 = sorted_times[(int)(NUMBER_OF_BATCHES * 0.99) - 1];
    
    printf("\nPerformance Metrics (ms):\n");
    printf("Total processing time: %.3f ms\n", total_processing_time);
    printf("Average batch time: %.3f ms\n", average);
    printf("Median batch time: %.3f ms\n", median);
    printf("90th percentile batch time: %.3f ms\n", p90);
    printf("99th percentile batch time: %.3f ms\n", p99);
    
    uint64_t state_hash = fnv1a_hash(state, SMALL_ACCOUNT_COUNT);
    printf("Final full state hash: %llu\n", state_hash);
    FILE *hash_fp = fopen("state_hash.dat", "wb");
    if (hash_fp) {
        fwrite(&state_hash, sizeof(uint64_t), 1, hash_fp);
        fclose(hash_fp);
    } else {
        perror("Error opening state_hash.dat for writing");
    }
    
    free(state);
    for (uint32_t i = 0; i < RING_SIZE; i++) {
        free(ws_accum[i]);
    }
    free(batch_times);
    free(sorted_times);
    free(transaction_batch);
    
    printf("Total time taken: %.3f ms\n", (double)(end_total - start_total));
    return 0;
}