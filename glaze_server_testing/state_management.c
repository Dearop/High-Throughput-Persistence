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
#define BATCH_SIZE          (1 << 16)          
#define NUMBER_OF_BATCHES   5000                 
#define SMALL_ACCOUNT_COUNT 2000000UL         

#define STATE_CHUNK_SIZE  (SMALL_ACCOUNT_COUNT * (8 + 8 + 4) / RING_SIZE)  
#define STATE_CHUNK_COUNT (STATE_CHUNK_SIZE / sizeof(int64_t))

#define WRITE_SET_COUNT   (BATCH_SIZE * 2)
#define WRITE_SET_CHUNK_SIZE (WRITE_SET_COUNT * sizeof(WriteSetEntry))

#define CHECKPOINT_SLOT_SIZE (sizeof(CheckpointHeader) + STATE_CHUNK_SIZE + WRITE_SET_CHUNK_SIZE)


#define TX_FILE "transactions.bin"
#define LOG_FILE "checkpoint_log.dat"

#define RING_SIZE 8   

// --- Data Structures ---

// Transaction structure.
typedef struct {
    uint64_t sender;
    uint64_t receiver;
    uint32_t amount;
} Transaction;

// Write-set entry: records an account address and its balance after a transaction.
typedef struct {
    uint64_t address;
    int64_t balance;
} WriteSetEntry;

// Checkpoint header records the batch number, the size of the state chunk (in count of int64_t),
// and the number of write-set entries (which should equal 2 * BATCH_SIZE).
typedef struct {
    uint32_t batch_num;
    uint32_t state_chunk_count; // should equal STATE_CHUNK_COUNT
    uint32_t write_set_count;   // should equal WRITE_SET_COUNT (2 * BATCH_SIZE)
    uint32_t reserved;
} CheckpointHeader;

// --- Utility Functions ---

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

// Pre-allocate (or extend) the log file to the expected size.
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

// Write the state to a text file (for debugging recovery mismatches).
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

typedef struct {
    uint32_t batch_num;
    int64_t *state_chunk;         // pointer to the saved state chunk (STATE_CHUNK_SIZE bytes)
    WriteSetEntry *write_set;     // pointer to the saved write set (WRITE_SET_CHUNK_SIZE bytes)
} CheckpointRecord;

// Comparator to sort checkpoint records by batch number (ascending).
int compare_checkpoint_record(const void *a, const void *b) {
    const CheckpointRecord *ca = (const CheckpointRecord *)a;
    const CheckpointRecord *cb = (const CheckpointRecord *)b;
    return (int)(ca->batch_num - cb->batch_num);
}

// Reconstructs the state (for the first STATE_CHUNK_COUNT accounts) by scanning the ring log,
// sorting valid checkpoints by batch number, using the oldest state chunk as baseline,
// and then “replaying” all write set entries in order.
int reconstruct_state(int fd, int64_t *state, int *last_batch) {
    CheckpointRecord records[RING_SIZE];
    int valid_count = 0;

    for (uint32_t i = 0; i < RING_SIZE; i++) {
        off_t offset = i * CHECKPOINT_SLOT_SIZE;
        CheckpointHeader header;
        ssize_t bytes = pread(fd, &header, sizeof(header), offset);
        if (bytes != sizeof(header))
            continue;
        if (header.state_chunk_count != STATE_CHUNK_COUNT || header.write_set_count != WRITE_SET_COUNT)
            continue;  // Skip if the header does not match expectations.
        
        int64_t *sc = malloc(STATE_CHUNK_SIZE);
        if (!sc) {
            perror("Error allocating memory for state chunk in reconstruction");
            continue;
        }
        bytes = pread(fd, sc, STATE_CHUNK_SIZE, offset + sizeof(CheckpointHeader));
        if (bytes != STATE_CHUNK_SIZE) {
            perror("Error reading state chunk during reconstruction");
            free(sc);
            continue;
        }
        WriteSetEntry *ws = malloc(WRITE_SET_CHUNK_SIZE);
        if (!ws) {
            perror("Error allocating memory for write set in reconstruction");
            free(sc);
            continue;
        }
        bytes = pread(fd, ws, WRITE_SET_CHUNK_SIZE, offset + sizeof(CheckpointHeader) + STATE_CHUNK_SIZE);
        if (bytes != WRITE_SET_CHUNK_SIZE) {
            perror("Error reading write set during reconstruction");
            free(sc);
            free(ws);
            continue;
        }
        records[valid_count].batch_num = header.batch_num;
        records[valid_count].state_chunk = sc;
        records[valid_count].write_set = ws;
        valid_count++;
    }
    if (valid_count == 0) {
        printf("No valid checkpoint found in log.\n");
        return -1;
    }
    // Sort the checkpoint records from oldest to newest.
    qsort(records, valid_count, sizeof(CheckpointRecord), compare_checkpoint_record);

    // Use the state chunk from the oldest checkpoint as a baseline.
    memcpy(state, records[0].state_chunk, STATE_CHUNK_SIZE);
    *last_batch = records[0].batch_num;
    // For each subsequent checkpoint, update the state using its write-set entries.
    for (int i = 1; i < valid_count; i++) {
        for (uint32_t j = 0; j < WRITE_SET_COUNT; j++) {
            uint64_t addr = records[i].write_set[j].address;
            int64_t bal = records[i].write_set[j].balance;
            // Only update accounts that are in our checkpointed chunk.
            if (addr < STATE_CHUNK_COUNT)
                state[addr] = bal;
        }
        *last_batch = records[i].batch_num;
        free(records[i].state_chunk);
        free(records[i].write_set);
    }
    // Free the buffers for the first record as well.
    free(records[0].state_chunk);
    free(records[0].write_set);
    return 0;
}

// --- Transaction Application Function ---

// Applies a transaction to the state and records the updated balances for sender and receiver
// into the provided write-set array (starting at ws_entry). Returns the number of entries added.
int apply(const Transaction *tx, int64_t *state, WriteSetEntry *ws_entry) {
    int count = 0;
    // Process sender: deduct the amount.
    if (tx->sender < SMALL_ACCOUNT_COUNT) {
        state[tx->sender] -= tx->amount;
        ws_entry[count].address = tx->sender;
        ws_entry[count].balance = state[tx->sender];
        count++;
    }
    // Process receiver: add the amount.
    if (tx->receiver < SMALL_ACCOUNT_COUNT) {
        state[tx->receiver] += tx->amount;
        ws_entry[count].address = tx->receiver;
        ws_entry[count].balance = state[tx->receiver];
        count++;
    }
    return count;  // Expected to always be 2.
}

// --- Main Function ---

int main(int argc, char **argv) {
    // Allocate the full state.
    int64_t *state = calloc(SMALL_ACCOUNT_COUNT + 1, sizeof(int64_t));
    if (!state) {
        perror("Error allocating state");
        exit(EXIT_FAILURE);
    }
    
    // Attempt to recover state from an existing log file.
    int recovered_batch = -1;
    int log_fd = open(LOG_FILE, O_RDWR);
    if (log_fd >= 0) {
        if (reconstruct_state(log_fd, state, &recovered_batch) == 0) {
            printf("Reconstructed state from log up to batch %d.\n", recovered_batch);
            uint64_t hash = fnv1a_hash(state, STATE_CHUNK_COUNT); // hash only the checkpointed chunk
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
    FILE *fp_transactions = fopen(TX_FILE, "rb");
    if (!fp_transactions) {
         perror("Error opening transactions file for reading");
         free(state);
         close(fd_log);
         exit(EXIT_FAILURE);
    }
    
    // Allocate array to collect batch processing times.
    double *batch_times = malloc(NUMBER_OF_BATCHES * sizeof(double));
    if (!batch_times) {
        perror("Error allocating batch_times");
        free(state);
        fclose(fp_transactions);
        close(fd_log);
        exit(EXIT_FAILURE);
    }
    double total_processing_time = 0.0;
    uint64_t start_total = get_time_ms();
    
    // Process each batch of transactions.
    for (unsigned int batch_num = 0; batch_num < NUMBER_OF_BATCHES; batch_num++) {
        double batch_start = get_time_ms();
        
        // Read a batch of transactions.
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
                perror("Error reading transactions file");
                free(transaction_batch);
                exit(EXIT_FAILURE);
            }
        }
        
        // Allocate the write set for this batch (2 entries per transaction).
        WriteSetEntry *write_set = malloc(WRITE_SET_CHUNK_SIZE);
        if (!write_set) {
            perror("Error allocating write set for batch");
            free(transaction_batch);
            exit(EXIT_FAILURE);
        }
        
        // Apply each transaction, recording the two modifications (sender and receiver)
        // sequentially into the write set.
        for (unsigned int i = 0, ws_index = 0; i < BATCH_SIZE; i++) {
            int modified = apply(&transaction_batch[i], state, &write_set[ws_index]);
            ws_index += modified; // expected to be 2 each time
        }
        
        // Save a snapshot of the state chunk (e.g. the first STATE_CHUNK_COUNT accounts).
        int64_t *state_snapshot = malloc(STATE_CHUNK_SIZE);
        if (!state_snapshot) {
            perror("Error allocating state snapshot");
            free(write_set);
            free(transaction_batch);
            exit(EXIT_FAILURE);
        }
        memcpy(state_snapshot, state, STATE_CHUNK_SIZE);
        
        // Prepare the checkpoint header.
        CheckpointHeader header;
        header.batch_num = batch_num;
        header.state_chunk_count = STATE_CHUNK_COUNT;
        header.write_set_count = WRITE_SET_COUNT;  // 2 entries per transaction
        header.reserved = 0;
        
        // Compute the ring slot offset.
        uint32_t slot_index = batch_num % RING_SIZE;
        off_t offset = slot_index * CHECKPOINT_SLOT_SIZE;
        
        ssize_t bytes_written;
        // Write the header.
        bytes_written = pwrite(fd_log, &header, sizeof(header), offset);
        if (bytes_written != sizeof(header)) {
            perror("Error writing checkpoint header");
        }
        // Write the state chunk snapshot.
        bytes_written = pwrite(fd_log, state_snapshot, STATE_CHUNK_SIZE, offset + sizeof(CheckpointHeader));
        if (bytes_written != STATE_CHUNK_SIZE) {
            perror("Error writing state snapshot");
        }
        // Write the write set.
        bytes_written = pwrite(fd_log, write_set, WRITE_SET_CHUNK_SIZE,
                               offset + sizeof(CheckpointHeader) + STATE_CHUNK_SIZE);
        if (bytes_written != WRITE_SET_CHUNK_SIZE) {
            perror("Error writing write set to log");
        }
        fsync(fd_log);
        
        free(state_snapshot);
        free(write_set);
        free(transaction_batch);
        
        double batch_end = get_time_ms();
        double duration = batch_end - batch_start;
        batch_times[batch_num] = duration;
        total_processing_time += duration;
        
        printf("Batch %u processed in %.3f ms.\n", batch_num, duration);
    }
    
    uint64_t end_total = get_time_ms();
    fclose(fp_transactions);
    close(fd_log);
    
    // Compute performance metrics.
    double average = total_processing_time / NUMBER_OF_BATCHES;
    // For simplicity, we sort the batch times array to get median/p90/p99.
    double *sorted_times = malloc(NUMBER_OF_BATCHES * sizeof(double));
    if (!sorted_times) {
        perror("Error allocating sorted_times");
        free(state);
        free(batch_times);
        exit(EXIT_FAILURE);
    }
    memcpy(sorted_times, batch_times, NUMBER_OF_BATCHES * sizeof(double));
    qsort(sorted_times, NUMBER_OF_BATCHES, sizeof(double), 
          (int(*)(const void*, const void*)) (int (*)(const double*, const double*)) 
          (^(const void *a, const void *b) {
              double diff = (*(double*)a) - (*(double*)b);
              return (diff < 0) ? -1 : (diff > 0) ? 1 : 0;
          }));
    double median = sorted_times[NUMBER_OF_BATCHES / 2];
    double p90 = sorted_times[(int)(NUMBER_OF_BATCHES * 0.9) - 1];
    double p99 = sorted_times[(int)(NUMBER_OF_BATCHES * 0.99) - 1];
    
    printf("\nPerformance Metrics (ms):\n");
    printf("Total processing time: %.3f ms\n", total_processing_time);
    printf("Average batch time: %.3f ms\n", average);
    printf("Median batch time: %.3f ms\n", median);
    printf("90th percentile batch time: %.3f ms\n", p90);
    printf("99th percentile batch time: %.3f ms\n", p99);
    
    // Compute and write the final state hash (for the checkpointed chunk).
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
    
    printf("Total time taken: %.3lld ms\n", (end_total - start_total));
    return 0;
}