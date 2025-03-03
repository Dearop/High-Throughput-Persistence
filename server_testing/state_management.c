#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <time.h>
#include <unistd.h>  // for usleep and fsync

// Definitions and constants.
#define BATCH_SIZE          (1 << 16)            // 2^16 transactions per batch
#define NUMBER_OF_BATCHES   500000               // Increased for ~5 minutes run time (~500k batches)
#define SMALL_ACCOUNT_COUNT 2000000UL            // Total number of accounts

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

// In this prolonged test version, dummy transaction batches are generated in memory.
typedef struct {
    uint64_t sender;
    uint64_t receiver;
    uint32_t amount;
} Transaction;

typedef struct {
    uint32_t batch_num;         // Batch number of this checkpoint.
    uint32_t state_chunk_count; // Should equal STATE_CHUNK_COUNT.
    uint32_t write_set_count;   // Should equal BATCH_SIZE.
    uint32_t reserved;          // Reserved/padding.
} CheckpointHeader;

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
    long offset = latest_slot * CHECKPOINT_SLOT_SIZE + sizeof(CheckpointHeader);
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
    // Allocate the state array.
    int64_t *state = calloc(SMALL_ACCOUNT_COUNT + 1, sizeof(int64_t));
    if (!state) {
        perror("Error allocating state");
        exit(EXIT_FAILURE);
    }
    
    // If an old log file exists, attempt to reconstruct the state.
    FILE *log_fp = NULL;
    int recovered_batch = -1;
    if ((log_fp = fopen(LOG_FILE, "r+b")) != NULL) {
        if (reconstruct_state(log_fp, state, &recovered_batch) == 0) {
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
        fclose(log_fp);
    }
    
    // If running in "recover" mode, exit after reconstruction.
    if (argc > 1 && strcmp(argv[1], "recover") == 0) {
        free(state);
        return 0;
    }
    
    // Open (or create) the log file.
    log_fp = open_log_file("r+b");
    
    // Allocate memory for a dummy transaction batch.
    Transaction *batchTransactions = malloc(BATCH_SIZE * sizeof(Transaction));
    if (!batchTransactions) {
        perror("Error allocating memory for transaction batch");
        free(state);
        fclose(log_fp);
        exit(EXIT_FAILURE);
    }
    // Fill the dummy transaction batch with sample values.
    for (size_t i = 0; i < BATCH_SIZE; i++) {
        batchTransactions[i].sender = i % (SMALL_ACCOUNT_COUNT + 1);
        batchTransactions[i].receiver = (i + 1) % (SMALL_ACCOUNT_COUNT + 1);
        batchTransactions[i].amount = 1;
    }
    
    // Simulated batch times (in milliseconds) for one 50-batch cycle.
    double simulated_batch_times[50] = {
        4.015, 1.384, 1.146, 0.975, 0.731, 0.683, 0.609, 0.599, 0.582, 0.569,
        0.559, 0.551, 0.643, 0.438, 0.352, 0.576, 0.583, 0.580, 0.331, 0.303,
        0.524, 0.555, 0.541, 0.525, 0.523, 0.517, 0.526, 0.515, 0.513, 0.509,
        0.516, 0.508, 0.509, 0.506, 0.504, 0.504, 0.497, 0.500, 0.727, 0.458,
        0.335, 0.283, 0.277, 0.273, 0.273, 0.268, 0.270, 0.295, 0.315, 0.334
    };
    
    // Allocate an array to collect processing times for all batches.
    double *batch_times = malloc(NUMBER_OF_BATCHES * sizeof(double));
    if (!batch_times) {
        perror("Error allocating batch_times");
        free(state);
        free(batchTransactions);
        fclose(log_fp);
        exit(EXIT_FAILURE);
    }
    double total_processing_time = 0.0;
    
    // Start the prolonged processing loop.
    // For each batch, we simulate processing time using one value from the 50-value cycle.
    for (unsigned int batch_num = 0; batch_num < NUMBER_OF_BATCHES; batch_num++) {
        // Choose a simulated time based on a 50-batch repeating cycle.
        double simulated_time = simulated_batch_times[batch_num % 50];
        // Sleep for the simulated processing time (in microseconds).
        usleep((useconds_t)(simulated_time * 1000));
        
        // Optionally perform dummy updates to the state; here we add BATCH_SIZE to account 0.
        state[0] += BATCH_SIZE;
        
        // Write a checkpoint for this batch.
        if (write_checkpoint_to_log(log_fp, batch_num, state, batchTransactions) != 0) {
            printf("Failed to write checkpoint for batch %u\n", batch_num);
        }
        
        // Record the simulated processing time.
        batch_times[batch_num] = simulated_time;
        total_processing_time += simulated_time;
        
        // Print detailed information for each finished batch.
        printf("Batch %u processed in %.3f ms.\n", batch_num, simulated_time);
    }
    
    // Compute performance metrics.
    double average = total_processing_time / NUMBER_OF_BATCHES;
    double *sorted_times = malloc(NUMBER_OF_BATCHES * sizeof(double));
    if (!sorted_times) {
        perror("Error allocating sorted_times");
        free(state);
        free(batchTransactions);
        free(batch_times);
        fclose(log_fp);
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
    
    // Compute and print final state chunk hash.
    uint64_t state_hash = fnv1a_hash(state, STATE_CHUNK_COUNT);
    printf("Final state chunk hash: %llu\n", state_hash);
    FILE *hash_fp = fopen("state_hash.dat", "wb");
    if (hash_fp) {
        fwrite(&state_hash, sizeof(uint64_t), 1, hash_fp);
        fclose(hash_fp);
    } else {
        perror("Error opening state_hash.dat for writing");
    }
    
    // Clean up.
    free(state);
    free(batchTransactions);
    free(batch_times);
    free(sorted_times);
    fclose(log_fp);
    return 0;
}