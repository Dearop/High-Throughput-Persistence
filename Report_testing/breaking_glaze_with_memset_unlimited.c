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
#define TOTAL_BATCHES     5000       // Process 5000 batches total
#define INITIAL_BALANCE   1000000L   // Starting balance per account

// Ring log parameters - now dynamic based on account count
#define RING_SIZE         8          // Number of checkpoint slots in the log

// Write-set size is the batch of transactions
#define WRITE_SET_SIZE    (BATCH_SIZE * sizeof(Transaction))

// Checkpoint slot size will be calculated dynamically
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

// Global variables (now parameterized)
static uint64_t g_max_accounts = 0;
static size_t g_state_size_bytes = 0;
static size_t g_checkpoint_header_size = 0;
static size_t g_checkpoint_slot_size = 0;

// Transaction structure updated for Report_testing format
typedef struct {
    uint64_t sender;    // Encoded: top 4 bits = function, bottom 60 bits = data
    uint64_t receiver;  // Encoded: top 4 bits = function, bottom 60 bits = data
    uint64_t amount;    // 64-bit amount
} Transaction;

// Checkpoint header structure
typedef struct {
    uint32_t batch_num;         // Batch number of this checkpoint
    uint32_t account_count;     // Number of accounts in this checkpoint
    uint32_t write_set_count;   // Should equal BATCH_SIZE
    uint32_t reserved;          // Reserved/padding
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
    off_t expected = RING_SIZE * g_checkpoint_slot_size;
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

// Reconstruction function
int reconstruct_state(int fd, int64_t *state, int *last_batch) {
    uint32_t latest_batch = 0;
    int latest_slot = -1;
    CheckpointHeader header;
    
    for (uint32_t i = 0; i < RING_SIZE; i++) {
        off_t offset = i * g_checkpoint_slot_size;
        ssize_t bytes = pread(fd, &header, sizeof(header), offset);
        if (bytes != sizeof(header))
            continue;
        if (header.write_set_count == BATCH_SIZE && header.account_count == g_max_accounts) {
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
    off_t offset = latest_slot * g_checkpoint_slot_size + sizeof(CheckpointHeader);
    ssize_t read_bytes = pread(fd, state, g_state_size_bytes, offset);
    if (read_bytes != (ssize_t)g_state_size_bytes) {
        perror("Error reading state during reconstruction");
        return -1;
    }
    *last_batch = latest_batch;
    return 0;
}

// Enhanced transaction application with memset support (unlimited accounts)
static inline bool apply_transaction_to_state_array(const Transaction *__restrict tx, int64_t *__restrict state)
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
        }
        return true;
    }

    /* Any other function codes -> unsupported */
    return false;
}

void apply(const Transaction *tx, int64_t *state, uint64_t *successful_tx, uint64_t *failed_tx) {
    if (apply_transaction_to_state_array(tx, state)) {
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
    g_state_size_bytes = g_max_accounts * sizeof(int64_t);
    g_checkpoint_header_size = sizeof(CheckpointHeader);
    g_checkpoint_slot_size = g_checkpoint_header_size + g_state_size_bytes + WRITE_SET_SIZE;

    printf("=== Breaking Glaze with Memset Support (Unlimited Accounts) ===\n");
    printf("Configuration:\n");
    printf("  MAX_ACCOUNTS: %lu (processing ALL accounts)\n", g_max_accounts);
    printf("  BATCH_SIZE: %d\n", BATCH_SIZE);
    printf("  TOTAL_BATCHES: %d\n", TOTAL_BATCHES);
    printf("  RING_SIZE: %d\n", RING_SIZE);
    printf("  Memory usage: ~%.1f MB for account state\n", 
           (double)g_state_size_bytes / (1024.0 * 1024.0));
    printf("  Checkpoint slot size: ~%.1f MB\n", 
           (double)g_checkpoint_slot_size / (1024.0 * 1024.0));
    printf("  Total log file size: ~%.1f MB\n", 
           (double)(RING_SIZE * g_checkpoint_slot_size) / (1024.0 * 1024.0));
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
        
        uint64_t hash = fnv1a_hash(state, g_max_accounts);
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

    // Allocate state snapshot buffer for checkpoints
    int64_t *state_snapshot = malloc(g_state_size_bytes);
    if (!state_snapshot) {
        perror("Error allocating state snapshot");
        free(batch);
        fclose(tx_file);
        close(fd_log);
        free(state);
        exit(EXIT_FAILURE);
    }

    // Timing arrays
    double *batch_times = malloc(TOTAL_BATCHES * sizeof(double));
    if (!batch_times) {
        perror("Error allocating timing arrays");
        free(state_snapshot);
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

        // Apply transactions
        uint64_t batch_successful = 0;
        uint64_t batch_failed = 0;
        
        for (size_t i = 0; i < read_count; i++) {
            apply(&batch[i], state, &batch_successful, &batch_failed);
        }
        
        total_successful_tx += batch_successful;
        total_failed_tx += batch_failed;

        // Create checkpoint
        uint32_t slot_index = batch_num % RING_SIZE;
        off_t offset = slot_index * g_checkpoint_slot_size;

        // Prepare checkpoint header
        CheckpointHeader header;
        header.batch_num = batch_num;
        header.account_count = g_max_accounts;
        header.write_set_count = BATCH_SIZE;
        header.reserved = 0;

        // Copy current state for checkpoint
        memcpy(state_snapshot, state, g_state_size_bytes);

        // Write checkpoint header
        ssize_t bytes_written = pwrite(fd_log, &header, sizeof(header), offset);
        if (bytes_written != sizeof(header)) {
            perror("Error writing checkpoint header");
            break;
        }

        // Write state snapshot
        bytes_written = pwrite(fd_log, state_snapshot, g_state_size_bytes, offset + sizeof(header));
        if (bytes_written != (ssize_t)g_state_size_bytes) {
            perror("Error writing state snapshot");
            break;
        }

        // Write transaction batch
        bytes_written = pwrite(fd_log, batch, WRITE_SET_SIZE, offset + sizeof(header) + g_state_size_bytes);
        if (bytes_written != WRITE_SET_SIZE) {
            perror("Error writing transaction batch");
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
            printf("Batch %u of %u processed in %.3f ms (success: %llu, failed: %llu)\n",
                   batch_num + 1, TOTAL_BATCHES, elapsed,
                   (unsigned long long)batch_successful,
                   (unsigned long long)batch_failed);
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
    uint64_t state_hash = fnv1a_hash(state, g_max_accounts);
    printf("\nFinal state hash: 0x%016llx\n", (unsigned long long)state_hash);

    // Cleanup
    free(batch_times);
    free(state_snapshot);
    free(batch);
    fclose(tx_file);
    close(fd_log);
    free(state);

    printf("\nExecution complete.\n");
    return 0;
} 