#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <errno.h>

#define BATCH_SIZE          (1 << 16)            // 2^16 transactions per batch
#define NUMBER_OF_BATCHES   5000                   // Number of batches
#define SMALL_ACCOUNT_COUNT 2000000UL            // Total number of accounts
#define PAYLOAD_SIZE        (1 << 12)            // Must match generator: 4096 integers per transaction

// Ring log parameters.
#define RING_SIZE         8                      // Number of checkpoint slots in the log.
#define STATE_CHUNK_SIZE  (512 * 1024)           // 512KB state chunk per checkpoint.
#define STATE_CHUNK_COUNT (STATE_CHUNK_SIZE / sizeof(int64_t))  // Number of int64_t elements in the state chunk.
    
// Write-set size is the batch of transactions.
#define WRITE_SET_SIZE    (BATCH_SIZE * sizeof(Transaction))

// A checkpoint slot consists of a header, the state chunk, and the write-set.
#define CHECKPOINT_HEADER_SIZE (sizeof(CheckpointHeader))
#define CHECKPOINT_SLOT_SIZE (CHECKPOINT_HEADER_SIZE + STATE_CHUNK_SIZE + WRITE_SET_SIZE)
#define LOG_FILE          "checkpoint_log.dat"   // Single log file with ring structure.

// Updated transaction structure with huge payload.
typedef struct {
    uint64_t sender;
    uint64_t receiver;
    uint32_t amount;
    int payload[PAYLOAD_SIZE];
} Transaction;

// Checkpoint header structure.
typedef struct {
    uint32_t batch_num;         // Batch number of this checkpoint.
    uint32_t state_chunk_count; // Should equal STATE_CHUNK_COUNT.
    uint32_t write_set_count;   // Should equal BATCH_SIZE.
    uint32_t reserved;          // Reserved/padding.
} CheckpointHeader;

// FNV-1a 64-bit hash function.
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

// Returns current time in milliseconds.
double get_time_ms() {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec * 1000.0 + ts.tv_nsec / 1e6;
}

// Writes the given state chunk to a text file.
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

// Pre-allocate the log file to the expected size using POSIX calls.
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

// Reconstruction function.
int reconstruct_state(int fd, int64_t *state, int *last_batch) {
    uint32_t latest_batch = 0;
    int latest_slot = -1;
    CheckpointHeader header;
    
    // Scan all slots in the ring.
    for (uint32_t i = 0; i < RING_SIZE; i++) {
        off_t offset = i * CHECKPOINT_SLOT_SIZE;
        ssize_t bytes = pread(fd, &header, sizeof(header), offset);
        if (bytes != sizeof(header))
            continue;
        if (header.write_set_count == BATCH_SIZE && header.state_chunk_count == STATE_CHUNK_COUNT) {
            if (header.batch_num >= latest_batch) {
                latest_batch = header.batch_num;
                latest_slot = i;
            }
        }
    }
    if (latest_slot == -1) {
        printf("No valid checkpoint found in log.\n");
        return -1;
    }
    off_t offset = latest_slot * CHECKPOINT_SLOT_SIZE + sizeof(CheckpointHeader);
    ssize_t read_bytes = pread(fd, state, sizeof(int64_t) * STATE_CHUNK_COUNT, offset);
    if (read_bytes != sizeof(int64_t) * STATE_CHUNK_COUNT) {
        perror("Error reading state chunk during reconstruction");
        return -1;
    }
    *last_batch = latest_batch;
    return 0;
}

// Apply function: update state according to a transaction.
void apply(const Transaction *tx, int64_t *state) {
    // For simplicity, apply only the basic balance update.
    if (tx->sender < SMALL_ACCOUNT_COUNT)
         state[tx->sender] -= tx->amount;
    if (tx->receiver < SMALL_ACCOUNT_COUNT)
         state[tx->receiver] += tx->amount;
}

// Compare function for qsort.
int compare_doubles(const void *a, const void *b) {
    double diff = (*(double *)a) - (*(double *)b);
    return (diff < 0) ? -1 : (diff > 0) ? 1 : 0;
}

// Main function using a single thread.
int main(int argc, char **argv) {
    // Allocate the state array.
    int64_t *state = calloc(SMALL_ACCOUNT_COUNT + 1, sizeof(int64_t));
    if (!state) {
        perror("Error allocating state");
        exit(EXIT_FAILURE);
    }
    
    // If an old log file exists, attempt to reconstruct the state.
    int recovered_batch = -1;
    int log_fd = open(LOG_FILE, O_RDWR);
    if (log_fd >= 0) {
        if (reconstruct_state(log_fd, state, &recovered_batch) == 0) {
            printf("Reconstructed state from log up to batch %d.\n", recovered_batch);
            uint64_t hash = fnv1a_hash(state, STATE_CHUNK_COUNT);
            FILE *hash_fp = fopen("state_hash.dat", "rb");
            if (hash_fp) {
                uint64_t stored_hash;
                fread(&stored_hash, sizeof(uint64_t), 1, hash_fp);
                fclose(hash_fp);
                if (hash == stored_hash) {
                    printf("Reconstructed state hash matches stored hash: %llu\n", hash);
                } else {
                    printf("Reconstructed state hash mismatch! Computed: %llu, Stored: %llu\n", hash, stored_hash);
                    write_state_to_file("reconstructed_state.txt", state, STATE_CHUNK_COUNT);
                }
            }
        }
        close(log_fd);
    }
    
    // If running in "recover" mode, exit after reconstruction.
    if (argc > 1 && strcmp(argv[1], "recover") == 0) {
        free(state);
        return 0;
    }
    
    // Pre-allocate the log file (if not already large enough).
    preallocate_log_file_posix(LOG_FILE);
    
    // Open the log file for writing checkpoints.
    int fd_log = open(LOG_FILE, O_RDWR);
    if (fd_log < 0) {
         perror("Error opening log file for writing");
         free(state);
         exit(EXIT_FAILURE);
    }
    
    // Open the transactions file for reading.
    FILE *fp_transactions = fopen("transactions.bin", "rb");
    if (!fp_transactions) {
         perror("Error opening transactions.bin for reading");
         free(state);
         close(fd_log);
         exit(EXIT_FAILURE);
    }
    
    // Array for collecting batch processing times.
    double *batch_times = malloc(NUMBER_OF_BATCHES * sizeof(double));
    if (!batch_times) {
        perror("Error allocating batch_times");
        free(state);
        fclose(fp_transactions);
        close(fd_log);
        exit(EXIT_FAILURE);
    }
    double total_processing_time = 0.0;
    
    uint64_t start = get_time_ms();
    for (unsigned int batch_num = 0; batch_num < NUMBER_OF_BATCHES; batch_num++) {
        double start_time_ms = get_time_ms();
        
        // Read a transaction batch from the file.
        Transaction *transaction_batch = malloc(BATCH_SIZE * sizeof(Transaction));
        if (!transaction_batch) {
            perror("Failed to allocate transaction batch");
            exit(EXIT_FAILURE);
        }
        size_t items = fread(transaction_batch, sizeof(Transaction), BATCH_SIZE, fp_transactions);
        if (items != BATCH_SIZE) {
            if (feof(fp_transactions)) {
                free(transaction_batch);
                break;
            } else {
                perror("Error reading transactions.bin");
                free(transaction_batch);
                exit(EXIT_FAILURE);
            }
        }
        
        // Apply each transaction to the state.
        for (unsigned int i = 0; i < BATCH_SIZE; i++) {
            apply(&transaction_batch[i], state);
        }
        
        // Create a snapshot of the state chunk.
        int64_t *state_snapshot = malloc(STATE_CHUNK_SIZE);
        if (!state_snapshot) {
            perror("Error allocating state snapshot");
            exit(EXIT_FAILURE);
        }
        memcpy(state_snapshot, state, STATE_CHUNK_SIZE);
        
        // Compute ring slot offset.
        uint32_t slot_index = batch_num % RING_SIZE;
        off_t offset = slot_index * CHECKPOINT_SLOT_SIZE;
        // Prepare checkpoint header.
        CheckpointHeader header;
        header.batch_num = batch_num;
        header.state_chunk_count = STATE_CHUNK_COUNT;
        header.write_set_count = BATCH_SIZE;
        header.reserved = 0;
        
        ssize_t bytes_written;
        // Write header.
        bytes_written = pwrite(fd_log, &header, sizeof(header), offset);
        if (bytes_written != sizeof(header)) {
            perror("Error writing header");
        }
        // Write state snapshot.
        bytes_written = pwrite(fd_log, state_snapshot, sizeof(int64_t) * STATE_CHUNK_COUNT, offset + sizeof(header));
        if (bytes_written != sizeof(int64_t) * STATE_CHUNK_COUNT) {
            perror("Error writing state snapshot");
        }
        // Write the transaction batch (the write-set).
        bytes_written = pwrite(fd_log, transaction_batch, sizeof(Transaction) * BATCH_SIZE,
                               offset + sizeof(header) + STATE_CHUNK_SIZE);
        if (bytes_written != sizeof(Transaction) * BATCH_SIZE) {
            perror("Error writing transactions");
        }
        fsync(fd_log);
        
        free(state_snapshot);
        free(transaction_batch);
        
        double end_time_ms = get_time_ms();
        double batch_duration = end_time_ms - start_time_ms;
        batch_times[batch_num] = batch_duration;
        total_processing_time += batch_duration;
        
        printf("Batch %u processed in %.3f ms.\n", batch_num, batch_duration);
    }
    uint64_t end = get_time_ms();
    
    // Clean up file handles.
    fclose(fp_transactions);
    close(fd_log);
    
    // Compute performance metrics.
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
    
    // Compute and write final state chunk hash.
    uint64_t state_hash = fnv1a_hash(state, STATE_CHUNK_COUNT);
    printf("Final state chunk hash: %llu\n", state_hash);
    FILE *hash_fp = fopen("state_hash.dat", "wb");
    if (hash_fp) {
        fwrite(&state_hash, sizeof(uint64_t), 1, hash_fp);
        fclose(hash_fp);
    } else {
        perror("Error opening state_hash.dat for writing");
    }
    
    free(state);
    free(batch_times);
    free(sorted_times);
    
    printf("Total time taken: %llu ms\n", end - start);
    return 0;
}