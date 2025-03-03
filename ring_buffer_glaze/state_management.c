#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <time.h>
#include <unistd.h>  // For fsync

#define BATCH_SIZE        (1 << 16)            // 2^16 transactions per batch
#define NUMBER_OF_BATCHES 50                    // Total number of batches (should match the transaction generator)
#define TX_FILE           "transactions.bin"   // Transaction file generated earlier
#define SMALL_ACCOUNT_COUNT 2000000UL          // Total number of accounts

// Ring log parameters.
#define RING_SIZE         8                    // Number of checkpoint slots in the log.
#define STATE_CHUNK_SIZE  (512 * 1024)         // 512KB state chunk per checkpoint.
#define STATE_CHUNK_COUNT (STATE_CHUNK_SIZE / sizeof(int64_t))  // Number of int64_t elements in the state chunk.
    
// Write-set size is the batch of transactions.
#define WRITE_SET_SIZE    (BATCH_SIZE * sizeof(Transaction))

// A checkpoint slot consists of a header, the state chunk, and the write-set.
#define CHECKPOINT_HEADER_SIZE (sizeof(CheckpointHeader))
#define CHECKPOINT_SLOT_SIZE (CHECKPOINT_HEADER_SIZE + STATE_CHUNK_SIZE + WRITE_SET_SIZE)
#define LOG_FILE          "checkpoint_log.dat" // Single log file with ring structure.

// Transaction structure (as generated).
typedef struct {
    uint64_t sender;
    uint64_t receiver;
    uint32_t amount;
} Transaction;

// Checkpoint header stored at the beginning of each slot.
typedef struct {
    uint32_t batch_num;         // Batch number of this checkpoint.
    uint32_t state_chunk_count; // Should equal STATE_CHUNK_COUNT.
    uint32_t write_set_count;   // Should equal BATCH_SIZE.
    uint32_t reserved;          // Reserved/padding.
} CheckpointHeader;

// Structure to hold metadata for each checkpoint slot found in the log.
typedef struct {
    CheckpointHeader header;
    long offset; // File offset where this slot begins.
} CheckpointSlot;

// FNV-1a 64-bit hash function (to hash the state chunk or entire state).
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

// Returns current time in milliseconds (with sub-ms resolution).
double get_time_ms() {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec * 1000.0 + ts.tv_nsec / 1e6;
}

// Writes the given state chunk (of count int64_t values) to a text file.
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

// Ensure the log file exists and is pre-allocated to RING_SIZE * CHECKPOINT_SLOT_SIZE bytes.
FILE *open_log_file(const char *mode) {
    FILE *fp = fopen(LOG_FILE, mode);
    if (!fp && (strcmp(mode, "r+b") == 0 || strcmp(mode, "r+") == 0)) {
        // If file does not exist, create it.
        fp = fopen(LOG_FILE, "w+b");
    }
    if (!fp) {
        perror("Error opening log file");
        exit(EXIT_FAILURE);
    }
    // Pre-allocate file space if necessary.
    fseek(fp, 0, SEEK_END);
    long fsize = ftell(fp);
    long expected = RING_SIZE * CHECKPOINT_SLOT_SIZE;
    if (fsize < expected) {
        fseek(fp, 0, SEEK_SET);
        char *zero_buf = calloc(1, expected);
        if (!zero_buf) {
            perror("calloc");
            fclose(fp);
            exit(EXIT_FAILURE);
        }
        fwrite(zero_buf, 1, expected, fp);
        free(zero_buf);
        fflush(fp);
        fsync(fileno(fp));
    }
    return fp;
}

// Write a checkpoint (state chunk + write-set) into the log file at the appropriate ring slot.
int write_checkpoint_to_log(FILE *log_fp, uint32_t batch_num, int64_t *state, Transaction *transactions) {
    // Compute ring slot index and corresponding file offset.
    uint32_t slot_index = batch_num % RING_SIZE;
    long offset = slot_index * CHECKPOINT_SLOT_SIZE;
    if (fseek(log_fp, offset, SEEK_SET) != 0) {
        perror("fseek error in write_checkpoint_to_log");
        return -1;
    }
    // Prepare header.
    CheckpointHeader header;
    header.batch_num = batch_num;
    header.state_chunk_count = STATE_CHUNK_COUNT;
    header.write_set_count = BATCH_SIZE;
    header.reserved = 0;
    // Write header.
    if (fwrite(&header, sizeof(header), 1, log_fp) != 1) {
        perror("Error writing checkpoint header");
        return -1;
    }
    // Write state chunk.
    // In this example, we dump a fixed contiguous block of the state (accounts 0..STATE_CHUNK_COUNT-1).
    if (fwrite(state, sizeof(int64_t), STATE_CHUNK_COUNT, log_fp) != STATE_CHUNK_COUNT) {
        perror("Error writing state chunk");
        return -1;
    }
    // Write the write-set (the entire batch of transactions).
    if (fwrite(transactions, sizeof(Transaction), BATCH_SIZE, log_fp) != BATCH_SIZE) {
        perror("Error writing write-set");
        return -1;
    }
    fflush(log_fp);
    if (fsync(fileno(log_fp)) != 0) {
        perror("fsync failed");
        return -1;
    }
    return 0;
}

// Reconstruction: Read the checkpoint slot with the highest batch number from the log file
// and load its state chunk directly into the state array.
int reconstruct_state(FILE *log_fp, int64_t *state, int *last_batch) {
    uint32_t latest_batch = 0;
    int latest_slot = -1;
    CheckpointHeader header;
    
    // Scan all slots in the ring.
    for (uint32_t i = 0; i < RING_SIZE; i++) {
        long offset = i * CHECKPOINT_SLOT_SIZE;
        if (fseek(log_fp, offset, SEEK_SET) != 0)
            continue;
        if (fread(&header, sizeof(header), 1, log_fp) != 1)
            continue;
        // Check for a valid header.
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
    // Read the state chunk from the latest slot.
    long offset = latest_slot * CHECKPOINT_SLOT_SIZE + CHECKPOINT_HEADER_SIZE;
    if (fseek(log_fp, offset, SEEK_SET) != 0) {
        perror("fseek error reading state chunk during reconstruction");
        return -1;
    }
    if (fread(state, sizeof(int64_t), STATE_CHUNK_COUNT, log_fp) != STATE_CHUNK_COUNT) {
        perror("Error reading state chunk during reconstruction");
        return -1;
    }
    *last_batch = latest_batch;
    return 0;
}

// For performance metrics: compare two doubles.
int compare_doubles(const void *a, const void *b) {
    double diff = (*(double *)a) - (*(double *)b);
    return (diff < 0) ? -1 : (diff > 0) ? 1 : 0;
}

int main(int argc, char **argv) {
    // Allocate state array.
    int64_t *state = calloc(SMALL_ACCOUNT_COUNT + 1, sizeof(int64_t));
    if (!state) {
        perror("Error allocating state");
        exit(EXIT_FAILURE);
    }
    
    // If an old log file exists, reconstruct the state from it.
    FILE *log_fp = NULL;
    int reconstructed = 0;
    int recovered_batch = -1;
    if ((log_fp = fopen(LOG_FILE, "r+b")) != NULL) {
        if (reconstruct_state(log_fp, state, &recovered_batch) == 0) {
            reconstructed = 1;
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
                    // Output the reconstructed state to a text file for inspection.
                    write_state_to_file("reconstructed_state.txt", state, STATE_CHUNK_COUNT);
                }
            }
        }
        fclose(log_fp);
    }
    
    // If running in "recover" mode, exit after reconstruction.
    if (argc > 1 && strcmp(argv[1], "recover") == 0) {
        free(state);
        return 0;
    }
    
    // Reset batch_num to 0 for new processing.
    int batch_num = 0;
    
    // Normal processing mode.
    FILE *tx_fp = fopen(TX_FILE, "rb");
    if (!tx_fp) {
        perror("Error opening transactions file for reading");
        free(state);
        exit(EXIT_FAILURE);
    }
    
    // Rewind transactions file in case it was read during reconstruction.
    fseek(tx_fp, 0, SEEK_SET);
    
    log_fp = open_log_file("r+b");
    
    Transaction *batchTransactions = malloc(BATCH_SIZE * sizeof(Transaction));
    if (!batchTransactions) {
        perror("Error allocating memory for transaction batch");
        free(state);
        fclose(tx_fp);
        fclose(log_fp);
        exit(EXIT_FAILURE);
    }
    
    double *batch_times = malloc(NUMBER_OF_BATCHES * sizeof(double));
    if (!batch_times) {
        perror("Error allocating batch_times");
        free(state);
        free(batchTransactions);
        fclose(tx_fp);
        fclose(log_fp);
        exit(EXIT_FAILURE);
    }
    
    double total_time = 0.0;
    
    while (batch_num < NUMBER_OF_BATCHES) {
        size_t read = fread(batchTransactions, sizeof(Transaction), BATCH_SIZE, tx_fp);
        if (read != BATCH_SIZE) {
            if (feof(tx_fp))
                break;
            perror("Error reading a transaction batch");
            break;
        }
        
        double start = get_time_ms();
        for (size_t i = 0; i < BATCH_SIZE; i++) {
            uint64_t sender = batchTransactions[i].sender;
            uint64_t receiver = batchTransactions[i].receiver;
            uint32_t amount = batchTransactions[i].amount;
            if (sender <= SMALL_ACCOUNT_COUNT)
                state[sender] -= amount;
            if (receiver <= SMALL_ACCOUNT_COUNT)
                state[receiver] += amount;
        }
        double end = get_time_ms();
        double elapsed = end - start;
        batch_times[batch_num] = elapsed;
        total_time += elapsed;
        
        if (write_checkpoint_to_log(log_fp, batch_num, state, batchTransactions) != 0) {
            printf("Failed to write checkpoint for batch %d\n", batch_num);
        }
        printf("Batch %d processed in %.3f ms.\n", batch_num, elapsed);
        batch_num++;
    }
    
    fclose(tx_fp);
    
    double average = total_time / batch_num;
    double *sorted_times = malloc(batch_num * sizeof(double));
    if (!sorted_times) {
        perror("Error allocating sorted_times");
        free(state);
        free(batchTransactions);
        free(batch_times);
        fclose(log_fp);
        exit(EXIT_FAILURE);
    }
    memcpy(sorted_times, batch_times, batch_num * sizeof(double));
    qsort(sorted_times, batch_num, sizeof(double), compare_doubles);
    double median = sorted_times[batch_num / 2];
    double p90 = sorted_times[(int)(batch_num * 0.9)];
    double p99 = sorted_times[(int)(batch_num * 0.99)];
    
    printf("\nPerformance Metrics (ms):\n");
    printf("Total processing time: %.3f ms\n", total_time);
    printf("Average batch time: %.3f ms\n", average);
    printf("Median batch time: %.3f ms\n", median);
    printf("90th percentile batch time: %.3f ms\n", p90);
    printf("99th percentile batch time: %.3f ms\n", p99);
    
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
    free(batchTransactions);
    free(batch_times);
    free(sorted_times);
    fclose(log_fp);
    return 0;
}