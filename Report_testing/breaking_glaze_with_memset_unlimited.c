#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <time.h>
#include <errno.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <stdbool.h>
#include <math.h>

// Configuration parameters
#define BATCH_SIZE        (1 << 16)  // 65,536 transactions per batch
#define TOTAL_BATCHES     50000       // Process 5000 batches total
#define INITIAL_BALANCE   1000000L   // Starting balance per account

// Ring log parameters - rotating chunk checkpointing (inspired by glaze)
#define RING_SIZE         8          // Number of checkpoint slots in the log
#define STATE_CHUNK_SIZE  (512 * 1024)           // 512KB state chunk per checkpoint (efficient)
#define STATE_CHUNK_COUNT (STATE_CHUNK_SIZE / sizeof(int64_t))  // 65,536 accounts per chunk

// Write-set size is variable, but we need to allocate maximum possible space
// Worst case: each transaction modifies multiple accounts (transfers: 2, memset: range length)
// Conservative estimate: 10x batch size for large memset operations
#define MAX_WRITE_SET_SIZE (BATCH_SIZE * 10 * sizeof(WriteSetEntry))

// Checkpoint slot size is now based on maximum possible write-set size
#define CHECKPOINT_HEADER_SIZE (sizeof(CheckpointHeader))
#define CHECKPOINT_SLOT_SIZE (CHECKPOINT_HEADER_SIZE + STATE_CHUNK_SIZE + MAX_WRITE_SET_SIZE)
#define LOG_FILE          "checkpoint_log.dat"

// --- Operation Encoding (from Report_testing) ---
#define FUNC_MASK   0xF000000000000000UL
#define DATA_MASK   0x0FFFFFFFFFFFFFFFUL
#define GET_FUNC(x) ((uint8_t)((x) >> 60))
#define GET_DATA(x) ((x) & DATA_MASK)

// --- Compiler-specific macros ---
#if defined(__GNUC__) || defined(__clang__)
#  define LIKELY(x)   __builtin_expect(!!(x), 1)
#  define UNLIKELY(x) __builtin_expect(!!(x), 0)
#  define PREFETCH(addr,rw,locality) __builtin_prefetch((addr),(rw),(locality))
#else
#  define LIKELY(x)   (x)
#  define UNLIKELY(x) (x)
#  define PREFETCH(addr,rw,locality)
#endif

// Global variables (now parameterized for processing, chunked for checkpointing)
static uint64_t g_max_accounts = 0;  // Process ALL accounts
static uint64_t g_total_chunks = 0;  // Total number of chunks needed
static size_t g_state_size_bytes = 0;
static size_t g_checkpoint_header_size = 0;
static size_t g_checkpoint_slot_size = 0;

// Transaction structure updated for Report_testing format
typedef struct {
    uint64_t sender;    // Encoded: top 4 bits = function, bottom 60 bits = data
    uint64_t receiver;  // Encoded: top 4 bits = function, bottom 60 bits = data
    uint64_t amount;    // 64-bit amount
} Transaction;

// Write-set entry: captures the actual state changes
typedef struct {
    uint64_t account_index;  // Which account was modified
    int64_t new_value;       // The new value after the transaction
} WriteSetEntry;

// Checkpoint header structure (inspired by glaze)
typedef struct {
    uint32_t batch_num;         // Batch number of this checkpoint
    uint32_t chunk_offset;      // Starting account index for this chunk
    uint32_t chunk_count;       // Number of accounts in this chunk (usually STATE_CHUNK_COUNT)
    uint32_t write_set_count;   // Number of actual write-set entries (not transactions)
} CheckpointHeader;

void print_usage(const char *program_name) {
    printf("Usage: %s <number_of_accounts>\n", program_name);
    printf("\n");
    printf("Arguments:\n");
    printf("  number_of_accounts  Number of accounts to manage (must be > 0)\n");
    printf("                      Example: 5000000 for 5 million accounts\n");
    printf("\n");
    printf("Examples:\n");
    printf("  %s 5000000     # Manage 5 million accounts\n", program_name);
    printf("  %s 1000000     # Manage 1 million accounts\n", program_name);
    printf("  %s 100000000   # Manage 100 million accounts\n", program_name);
    printf("\n");
    printf("Configuration:\n");
    printf("  Batch size: %d transactions\n", BATCH_SIZE);
    printf("  Total batches: %d\n", TOTAL_BATCHES);
    printf("  Ring size: %d checkpoint slots\n", RING_SIZE);
    printf("  Initial balance: %ld per account\n", INITIAL_BALANCE);
    printf("\n");
    printf("Input file: transactions.bin (must exist)\n");
    printf("Output files: %s, reconstructed_state.txt\n", LOG_FILE);
}

// FNV-1a 64-bit hash function
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

// Returns current time in milliseconds
double get_time_ms() {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec * 1000.0 + ts.tv_nsec / 1e6;
}

// Writes the given state to a text file
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

// Pre-allocate the log file to the expected size
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

// Reconstruction function (enhanced for write-sets)
int reconstruct_state(int fd, int64_t *state, int *last_batch) {
    uint32_t latest_batch = 0;
    int latest_slot = -1;
    CheckpointHeader header;
    
    // Scan all ring slots to find the latest checkpoint for each chunk
    for (uint32_t i = 0; i < RING_SIZE; i++) {
        off_t offset = i * CHECKPOINT_SLOT_SIZE;
        ssize_t bytes = pread(fd, &header, sizeof(header), offset);
        if (bytes != sizeof(header))
            continue;
        if (header.chunk_offset < g_max_accounts) {
            // Read this chunk's state (actual size based on chunk_count)
            size_t actual_chunk_size = header.chunk_count * sizeof(int64_t);
            int64_t state_chunk[STATE_CHUNK_COUNT];
            ssize_t read_bytes = pread(fd, state_chunk, actual_chunk_size, offset + sizeof(CheckpointHeader));
            if (read_bytes == (ssize_t)actual_chunk_size) {
                // Copy chunk data to the appropriate position in full state
                uint64_t copy_count = header.chunk_count;
                if (header.chunk_offset + copy_count > g_max_accounts) {
                    copy_count = g_max_accounts - header.chunk_offset;
                }
                memcpy(state + header.chunk_offset, state_chunk, copy_count * sizeof(int64_t));
                
                // Read and apply write-sets sequentially
                if (header.write_set_count > 0) {
                    size_t write_set_size = header.write_set_count * sizeof(WriteSetEntry);
                    WriteSetEntry *write_set = malloc(write_set_size);
                    if (write_set) {
                        ssize_t ws_bytes = pread(fd, write_set, write_set_size, offset + sizeof(CheckpointHeader) + actual_chunk_size);
                        if (ws_bytes == (ssize_t)write_set_size) {
                            // Apply write-sets sequentially to get final state
                            for (uint32_t j = 0; j < header.write_set_count; j++) {
                                if (write_set[j].account_index < g_max_accounts) {
                                    state[write_set[j].account_index] = write_set[j].new_value;
                                }
                            }
                        }
                        free(write_set);
                    }
                }
                
                if (header.batch_num >= latest_batch) {
                    latest_batch = header.batch_num;
                    latest_slot = i;
                }
            }
        }
    }
    
    if (latest_slot == -1) {
        printf("No valid checkpoint found in log.\n");
        return -1;
    }
    
    *last_batch = latest_batch;
    return 0;
}

// Enhanced transaction application with write-set collection
static inline bool apply_transaction_to_state_array_with_writeset(const Transaction *__restrict tx, int64_t *__restrict state, WriteSetEntry *__restrict write_set, uint32_t *__restrict write_set_count)
{
    /* Decode the packed sender/receiver words */
    const uint8_t  sfunc = GET_FUNC(tx->sender);
    const uint8_t  rfunc = GET_FUNC(tx->receiver);
    const uint64_t sidx  = GET_DATA(tx->sender);
    const uint64_t ridx  = GET_DATA(tx->receiver);

    /* -------- early rejection of out-of-bounds indices -------- */
    if (UNLIKELY(sidx >= g_max_accounts || (rfunc == 0 && ridx >= g_max_accounts)))
        return false;

    /* ==========================================================
     *  Hot path: simple balance transfer (func == 0 → account id)
     * ========================================================== */
    if (LIKELY(sfunc == 0 && rfunc == 0)) {
        /* Bounds check for both accounts */
        if (UNLIKELY(sidx >= g_max_accounts || ridx >= g_max_accounts))
            return false;

        int64_t *__restrict from = &state[sidx];
        int64_t *__restrict to   = &state[ridx];

        PREFETCH(from, 0, 1); /* read-only prefetch */
        PREFETCH(to,   1, 1); /* write-intent prefetch */

        const int64_t amt = (int64_t)tx->amount;
        if (LIKELY(*from >= amt)) {
            *from -= amt;
            *to += amt;
            
            // Record write-set entries for both accounts
            write_set[*write_set_count].account_index = sidx;
            write_set[*write_set_count].new_value = *from;
            (*write_set_count)++;
            
            write_set[*write_set_count].account_index = ridx;
            write_set[*write_set_count].new_value = *to;
            (*write_set_count)++;
            
            return true;
        }
        return false; /* insufficient funds */
    }

    /* ==========================================================
     *  Cold path: range-set (func == 1 → [start,len])  
     *  Writes `amount` into accounts [start, start+len)
     * ========================================================== */
    if (LIKELY(sfunc == 1 && rfunc == 1)) {
        uint64_t start = sidx;
        uint64_t len   = ridx;          /* receiver "data" holds length */

        if (UNLIKELY(!len || start >= g_max_accounts))
            return false;

        uint64_t end = start + len;
        if (end > g_max_accounts) end = g_max_accounts;

        const int64_t val = (int64_t)tx->amount;
        
        for (uint64_t i = start; i < end; ++i) {
            state[i] = val;
            
            // Record write-set entry for each modified account
            write_set[*write_set_count].account_index = i;
            write_set[*write_set_count].new_value = val;
            (*write_set_count)++;
        }
        return true;
    }

    /* Any other function codes -> unsupported */
    return false;
}

void apply_with_writeset(const Transaction *tx, int64_t *state, WriteSetEntry *write_set, uint32_t *write_set_count, uint64_t *successful_tx, uint64_t *failed_tx) {
    if (apply_transaction_to_state_array_with_writeset(tx, state, write_set, write_set_count)) {
        (*successful_tx)++;
    } else {
        (*failed_tx)++;
    }
}

int compare_doubles(const void *a, const void *b) {
    double da = *(const double *)a;
    double db = *(const double *)b;
    return (da > db) - (da < db);
}

int main(int argc, char **argv) {
    // Parse command-line arguments
    if (argc != 2) {
        fprintf(stderr, "Error: Invalid number of arguments.\n\n");
        print_usage(argv[0]);
        return EXIT_FAILURE;
    }

    char *endptr;
    unsigned long long account_count = strtoull(argv[1], &endptr, 10);
    
    // Validate the account count
    if (*endptr != '\0') {
        fprintf(stderr, "Error: Invalid account count '%s'. Must be a valid number.\n\n", argv[1]);
        print_usage(argv[0]);
        return EXIT_FAILURE;
    }
    
    if (account_count == 0) {
        fprintf(stderr, "Error: Account count must be greater than 0.\n\n");
        print_usage(argv[0]);
        return EXIT_FAILURE;
    }

    // Check for reasonable limits (avoid excessive memory usage)
    if (account_count > 1000000000ULL) {  // 1 billion accounts
        fprintf(stderr, "Error: Account count %llu is too large (max: 1,000,000,000).\n\n", account_count);
        print_usage(argv[0]);
        return EXIT_FAILURE;
    }

    g_max_accounts = account_count;
    g_total_chunks = (g_max_accounts + STATE_CHUNK_COUNT - 1) / STATE_CHUNK_COUNT;  // Round up
    g_state_size_bytes = g_max_accounts * sizeof(int64_t);
    g_checkpoint_header_size = sizeof(CheckpointHeader);
    g_checkpoint_slot_size = g_checkpoint_header_size + STATE_CHUNK_SIZE + MAX_WRITE_SET_SIZE;

    printf("=== Breaking Glaze with Memset Support (Unlimited Accounts) ===\n");
    printf("Configuration:\n");
    printf("  MAX_ACCOUNTS: %llu (processing ALL accounts)\n", (unsigned long long)g_max_accounts);
    printf("  TOTAL_CHUNKS: %llu (rotating through chunks of %lu accounts each)\n", (unsigned long long)g_total_chunks, STATE_CHUNK_COUNT);
    printf("  BATCH_SIZE: %d\n", BATCH_SIZE);
    printf("  TOTAL_BATCHES: %d\n", TOTAL_BATCHES);
    printf("  RING_SIZE: %d\n", RING_SIZE);
    printf("  Memory usage: ~%.1f MB for account state\n", 
           (double)g_state_size_bytes / (1024.0 * 1024.0));
    printf("  Checkpoint slot size: ~%.1f KB (efficient chunked)\n", 
           (double)CHECKPOINT_SLOT_SIZE / 1024.0);
    printf("  Total log file size: ~%.1f MB\n", 
           (double)(RING_SIZE * CHECKPOINT_SLOT_SIZE) / (1024.0 * 1024.0));
    printf("  Checkpointing strategy: Rotating chunks (full coverage over time)\n");
    printf("  Supports: Balance transfers (func=0) and Range-set operations (func=1)\n\n");

    // Allocate state array
    int64_t *state = calloc(g_max_accounts, sizeof(int64_t));
    if (!state) {
        perror("Error allocating state array");
        exit(EXIT_FAILURE);
    }

    // Initialize state with starting balances
    for (size_t i = 0; i < g_max_accounts; i++) {
        state[i] = INITIAL_BALANCE;
    }

    // Pre-allocate log file
    preallocate_log_file_posix(LOG_FILE);

    // Try to reconstruct state from existing log
    int fd_log = open(LOG_FILE, O_RDWR);
    if (fd_log < 0) {
        perror("Error opening log file");
        free(state);
        exit(EXIT_FAILURE);
    }

    int last_batch = -1;
    double reconstruction_start = get_time_ms();
    if (reconstruct_state(fd_log, state, &last_batch) == 0) {
        double reconstruction_end = get_time_ms();
        printf("State reconstructed from checkpoint (batch %d) in %.3f ms.\n", 
               last_batch, reconstruction_end - reconstruction_start);
        
        uint64_t hash = fnv1a_hash(state, STATE_CHUNK_COUNT);
        printf("Reconstructed state hash: 0x%016llx\n", (unsigned long long)hash);
        
        write_state_to_file("reconstructed_state.txt", state, g_max_accounts);
        printf("Reconstructed state written to reconstructed_state.txt\n");
    } else {
        double reconstruction_end = get_time_ms();
        printf("No valid checkpoint found. Using fresh state in %.3f ms.\n", 
               reconstruction_end - reconstruction_start);
    }

    // Open transaction file
    FILE *tx_file = fopen("transactions.bin", "rb");
    if (!tx_file) {
        perror("Error opening transaction file");
        close(fd_log);
        free(state);
        exit(EXIT_FAILURE);
    }

    // Allocate transaction batch buffer
    Transaction *batch = malloc(BATCH_SIZE * sizeof(Transaction));
    if (!batch) {
        perror("Error allocating transaction batch");
        fclose(tx_file);
        close(fd_log);
        free(state);
        exit(EXIT_FAILURE);
    }

    // Timing arrays
    double *batch_times = malloc(TOTAL_BATCHES * sizeof(double));
    if (!batch_times) {
        perror("Error allocating timing arrays");
        free(batch);
        fclose(tx_file);
        close(fd_log);
        free(state);
        exit(EXIT_FAILURE);
    }

    // Allocate write-set buffer (worst case: each transaction can modify multiple accounts)
    // For transfers: 2 accounts per transaction, for memset: up to range length
    // Conservative estimate: 10x batch size to handle large memset operations
    WriteSetEntry *write_set = malloc(MAX_WRITE_SET_SIZE);
    if (!write_set) {
        perror("Error allocating write-set buffer");
        free(batch_times);
        free(batch);
        fclose(tx_file);
        close(fd_log);
        free(state);
        exit(EXIT_FAILURE);
    }

    uint64_t total_successful_tx = 0;
    uint64_t total_failed_tx = 0;
    double total_elapsed_ms = 0.0;

    printf("\nStarting transaction processing...\n");

    // Process batches
    for (uint32_t batch_num = 0; batch_num < TOTAL_BATCHES; batch_num++) {
        double batch_start = get_time_ms();

        // Read transaction batch
        size_t read_count = fread(batch, sizeof(Transaction), BATCH_SIZE, tx_file);
        if (read_count < BATCH_SIZE) {
            if (feof(tx_file)) {
                rewind(tx_file);
                read_count = fread(batch, sizeof(Transaction), BATCH_SIZE, tx_file);
            } else {
                perror("Error reading transaction batch");
                break;
            }
        }

        // Apply transactions and collect write-sets
        uint64_t batch_successful = 0;
        uint64_t batch_failed = 0;
        uint32_t write_set_count = 0;  // Reset for each batch
        
        for (size_t i = 0; i < read_count; i++) {
            apply_with_writeset(&batch[i], state, write_set, &write_set_count, &batch_successful, &batch_failed);
        }
        
        total_successful_tx += batch_successful;
        total_failed_tx += batch_failed;

        // Create checkpoint for rotating chunk (inspired by glaze)
        uint32_t slot_index = batch_num % RING_SIZE;
        uint32_t chunk_index = slot_index % g_total_chunks;  // Rotate through available chunks
        uint32_t chunk_offset = chunk_index * STATE_CHUNK_COUNT;
        off_t offset = slot_index * CHECKPOINT_SLOT_SIZE;

        // Prepare checkpoint header
        CheckpointHeader header;
        header.batch_num = batch_num;
        header.chunk_offset = chunk_offset;
        header.chunk_count = (chunk_offset + STATE_CHUNK_COUNT <= g_max_accounts) ? 
                            STATE_CHUNK_COUNT : (g_max_accounts - chunk_offset);
        header.write_set_count = write_set_count;  // Actual number of write-set entries

        // Copy the appropriate chunk of state for efficient checkpointing
        int64_t state_chunk[STATE_CHUNK_COUNT];
        memset(state_chunk, 0, sizeof(state_chunk));  // Initialize to zero
        if (chunk_offset < g_max_accounts) {
            memcpy(state_chunk, state + chunk_offset, header.chunk_count * sizeof(int64_t));
        }

        // Write checkpoint header
        ssize_t bytes_written = pwrite(fd_log, &header, sizeof(header), offset);
        if (bytes_written != sizeof(header)) {
            perror("Error writing checkpoint header");
            break;
        }

        // Write state chunk (actual size based on chunk_count)
        size_t actual_chunk_size = header.chunk_count * sizeof(int64_t);
        bytes_written = pwrite(fd_log, state_chunk, actual_chunk_size, offset + sizeof(header));
        if (bytes_written != (ssize_t)actual_chunk_size) {
            perror("Error writing state chunk");
            break;
        }

        // Write write-set entries (actual size based on write_set_count)
        size_t actual_write_set_size = header.write_set_count * sizeof(WriteSetEntry);
        bytes_written = pwrite(fd_log, write_set, actual_write_set_size, offset + sizeof(header) + actual_chunk_size);
        if (bytes_written != (ssize_t)actual_write_set_size) {
            perror("Error writing write-set");
            break;
        }

        // Ensure data is written to disk
        if (fsync(fd_log) != 0) {
            perror("Error syncing log file");
            break;
        }

        double batch_end = get_time_ms();
        double elapsed = batch_end - batch_start;
        batch_times[batch_num] = elapsed;
        total_elapsed_ms += elapsed;

        if ((batch_num + 1) % 10 == 0 || batch_num == 0) {
            size_t total_checkpoint_bytes = sizeof(header) + actual_chunk_size + actual_write_set_size;
            printf("Batch %u of %u processed in %.3f ms (success: %llu, failed: %llu, checkpointed chunk %u: accounts %u-%u, %u write-sets, %zu bytes)\n",
                   batch_num + 1, TOTAL_BATCHES, elapsed,
                   (unsigned long long)batch_successful,
                   (unsigned long long)batch_failed,
                   chunk_index, chunk_offset, chunk_offset + header.chunk_count - 1,
                   write_set_count, total_checkpoint_bytes);
        }
    }

    // Calculate statistics
    double avg_elapsed_ms = total_elapsed_ms / (double)TOTAL_BATCHES;
    double total_tx = total_successful_tx + total_failed_tx;
    double success_rate = (total_tx > 0) ? (total_successful_tx * 100.0 / total_tx) : 0.0;
    
    printf("\n=== Processing Summary ===\n");
    printf("Processed %d batches total.\n", TOTAL_BATCHES);
    printf("Total transactions: %llu (successful: %llu, failed: %llu)\n", 
           (unsigned long long)total_tx, 
           (unsigned long long)total_successful_tx, 
           (unsigned long long)total_failed_tx);
    printf("Success rate: %.2f%%\n", success_rate);
    printf("Total time: %.3f ms\n", total_elapsed_ms);
    printf("Average batch time: %.3f ms\n", avg_elapsed_ms);
    printf("Throughput: %.0f tx/sec\n", total_tx * 1000.0 / total_elapsed_ms);

    // Calculate percentiles
    qsort(batch_times, TOTAL_BATCHES, sizeof(double), compare_doubles);
    
    double median_ms, p90_ms, p99_ms;
    if (TOTAL_BATCHES % 2 == 0) {
        int mid = TOTAL_BATCHES / 2;
        median_ms = (batch_times[mid - 1] + batch_times[mid]) / 2.0;
    } else {
        median_ms = batch_times[TOTAL_BATCHES / 2];
    }
    
    int idx_90 = (int)ceil(0.90 * TOTAL_BATCHES) - 1;
    if (idx_90 < 0) idx_90 = 0;
    if (idx_90 >= (int)TOTAL_BATCHES) idx_90 = TOTAL_BATCHES - 1;
    p90_ms = batch_times[idx_90];
    
    int idx_99 = (int)ceil(0.99 * TOTAL_BATCHES) - 1;
    if (idx_99 < 0) idx_99 = 0;
    if (idx_99 >= (int)TOTAL_BATCHES) idx_99 = TOTAL_BATCHES - 1;
    p99_ms = batch_times[idx_99];

    printf("\nLatency statistics:\n");
    printf("  Median batch time: %.3f ms\n", median_ms);
    printf("  90th percentile batch time: %.3f ms\n", p90_ms);
    printf("  99th percentile batch time: %.3f ms\n", p99_ms);

    // Final state hash
    uint64_t state_hash = fnv1a_hash(state, STATE_CHUNK_COUNT);
    printf("\nFinal state hash: 0x%016llx\n", (unsigned long long)state_hash);

    // Cleanup
    free(batch_times);
    free(batch);
    fclose(tx_file);
    close(fd_log);
    free(state);
    free(write_set);

    printf("\nExecution complete.\n");
    return 0;
} 