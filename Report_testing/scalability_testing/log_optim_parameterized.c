#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <time.h>
#include <errno.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <pthread.h>   // For pthread_create, etc.
#include <math.h>      // For ceil()
#include <stdbool.h>

#define BATCH_SIZE        (1 << 16)  // 2^16 transactions per batch
#define INITIAL_BALANCE   10000000UL
#define TOTAL_BATCHES     5000         // Process 5000 batches total
#define MAX_LOG_BATCHES   100          // Snapshot every 100 batches

// Filenames
#define TX_FILE           "transactions.bin"
#define LOG_FILE          "state_log.bin"
#define SNAPSHOT_FILE     "state_snapshot.bin"

// --- Operation Encoding (from remember_glaze) ---
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

typedef struct {
    uint64_t sender;
    uint64_t receiver;
    uint64_t amount;
} Transaction;

typedef struct {
    uint64_t address;
    int64_t balance;  // Changed to int64_t to support negative balances
} Account;

// -----------------------------------------------------------------------------
// Global variables (now parameterized)
// -----------------------------------------------------------------------------
static Account *g_state = NULL;
static uint64_t g_processed_batches = 0;
static uint64_t g_max_accounts = 0;  // Will be set from command line

// Background snapshot task structure
typedef struct {
    Account *snapshot_state;  // Copy of accounts to write
    size_t   account_count;
} SnapshotTask;

// For managing the background snapshot thread
static pthread_t  g_snapshot_thread;
static int        g_snapshot_in_progress = 0;

// -----------------------------------------------------------------------------
// Utility functions
// -----------------------------------------------------------------------------
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
    printf("  Snapshot frequency: Every %d batches\n", MAX_LOG_BATCHES);
    printf("  Initial balance: %lu per account\n", INITIAL_BALANCE);
    printf("\n");
    printf("Input file: %s (must exist)\n", TX_FILE);
    printf("Output files: %s, %s\n", LOG_FILE, SNAPSHOT_FILE);
}

// -----------------------------------------------------------------------------
// Utility: compute time difference in milliseconds
// -----------------------------------------------------------------------------
static inline double timespec_diff_ms(const struct timespec *start, const struct timespec *end)
{
    double secs  = (double)(end->tv_sec - start->tv_sec);
    double nsecs = (double)(end->tv_nsec - start->tv_nsec) / 1e9;
    return (secs + nsecs) * 1000.0;
}

// -----------------------------------------------------------------------------
// simple comparator for qsort (used for median/p90/p99 calculations)
// -----------------------------------------------------------------------------
static int cmp_doubles(const void *a, const void *b)
{
    double da = *(const double *)a;
    double db = *(const double *)b;
    return (da > db) - (da < db);  // sign of (da - db)
}

// -----------------------------------------------------------------------------
// Enhanced transaction application with memset support
// -----------------------------------------------------------------------------
static inline bool apply_transaction_to_state_array(const Transaction *__restrict tx)
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

        Account *__restrict from = &g_state[sidx];
        Account *__restrict to   = &g_state[ridx];

        PREFETCH(from, 0, 1); /* read-only prefetch */
        PREFETCH(to,   1, 1); /* write-intent prefetch */

        const int64_t amt = (int64_t)tx->amount;
        if (LIKELY(from->balance >= amt)) {
            from->balance -= amt;
            to->balance += amt;
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
            g_state[i].balance = val;
        }
        return true;
    }

    /* Any other function codes -> unsupported */
    return false;
}

// -----------------------------------------------------------------------------
// Load existing snapshot or create fresh state if none
// -----------------------------------------------------------------------------
static void load_or_init_state(void)
{
    FILE *snapshot = fopen(SNAPSHOT_FILE, "rb");
    if (!snapshot) {
        // No snapshot => create fresh state
        g_state = (Account *)malloc(g_max_accounts * sizeof(Account));
        if (!g_state) {
            perror("malloc for state failed");
            exit(EXIT_FAILURE);
        }
        for (uint64_t i = 0; i < g_max_accounts; i++) {
            g_state[i].address = i;
            g_state[i].balance = INITIAL_BALANCE;
        }
        printf("No snapshot found; initialized fresh state with %lu accounts.\n", g_max_accounts);
        return;
    }

    // Load snapshot
    g_state = (Account *)malloc(g_max_accounts * sizeof(Account));
    if (!g_state) {
        perror("malloc for state failed");
        fclose(snapshot);
        exit(EXIT_FAILURE);
    }
    size_t read_items = fread(g_state, sizeof(Account), g_max_accounts, snapshot);
    fclose(snapshot);
    if (read_items != g_max_accounts) {
        fprintf(stderr, "Snapshot file incomplete or corrupted (expected %lu accounts, got %zu).\n", 
                g_max_accounts, read_items);
        free(g_state);
        exit(EXIT_FAILURE);
    }
    printf("Loaded existing snapshot from %s (%lu accounts)\n", SNAPSHOT_FILE, g_max_accounts);
}

// -----------------------------------------------------------------------------
// Replay any existing log into g_state with enhanced transaction support
// -----------------------------------------------------------------------------
static uint64_t replay_log(void)
{
    FILE *log_fp = fopen(LOG_FILE, "rb");
    if (!log_fp) {
        printf("No log file found; nothing to replay.\n");
        return 0;
    }
    
    struct timespec replay_start, replay_end;
    clock_gettime(CLOCK_MONOTONIC, &replay_start);
    
    Transaction tx;
    uint64_t tx_count = 0;
    uint64_t successful_tx = 0;
    
    // Read transactions in batches for better performance
    Transaction batch[1024];
    size_t batch_read;
    
    while ((batch_read = fread(batch, sizeof(Transaction), 1024, log_fp)) > 0) {
        for (size_t i = 0; i < batch_read; i++) {
            if (apply_transaction_to_state_array(&batch[i])) {
                successful_tx++;
            }
            tx_count++;
        }
    }
    
    fclose(log_fp);
    
    clock_gettime(CLOCK_MONOTONIC, &replay_end);
    double replay_elapsed_ms = timespec_diff_ms(&replay_start, &replay_end);

    if (tx_count > 0) {
        printf("Replayed %llu transactions from log (%llu successful) in %.3f ms.\n", 
               (unsigned long long)tx_count, (unsigned long long)successful_tx, replay_elapsed_ms);
        printf("Replay throughput: %.0f tx/sec\n", tx_count * 1000.0 / replay_elapsed_ms);
    } else {
        printf("Log file was empty.\n");
    }

    return tx_count;
}

// -----------------------------------------------------------------------------
// Background thread function to write a snapshot
// -----------------------------------------------------------------------------
static void *snapshot_worker(void *arg)
{
    struct timespec snapshot_start, snapshot_end;
    clock_gettime(CLOCK_MONOTONIC, &snapshot_start);
    
    SnapshotTask *task = (SnapshotTask *)arg;
    
    // Write the snapshot
    FILE *snapshot = fopen(SNAPSHOT_FILE, "wb");
    if (!snapshot) {
        perror("Error creating snapshot file");
        free(task->snapshot_state);
        free(task);
        return NULL;
    }

    size_t written = fwrite(task->snapshot_state, sizeof(Account), task->account_count, snapshot);
    fflush(snapshot);
    if (fsync(fileno(snapshot)) != 0) {
        perror("fsync failed on snapshot file");
    }
    fclose(snapshot);
    
    clock_gettime(CLOCK_MONOTONIC, &snapshot_end);
    double snapshot_elapsed_ms = timespec_diff_ms(&snapshot_start, &snapshot_end);
    
    if (written != task->account_count) {
        fprintf(stderr, "Error writing snapshot: wrote=%zu, expected=%zu\n",
                written, task->account_count);
    } else {
        printf("[Async] Snapshot created with %zu accounts in %.3f ms.\n", 
               task->account_count, snapshot_elapsed_ms);
    }

    // Remove/truncate old log
    if (remove(LOG_FILE) != 0 && errno != ENOENT) {
        perror("[Async] Error removing old log");
    } else {
        printf("[Async] Log has been reset (old log removed).\n");
    }

    // Free the copy of the state
    free(task->snapshot_state);
    free(task);

    // Mark snapshot as complete
    g_snapshot_in_progress = 0;
    return NULL;
}

// -----------------------------------------------------------------------------
// Wait for any prior snapshot to finish before starting a new one
// -----------------------------------------------------------------------------
static void wait_for_snapshot_if_needed(void)
{
    if (g_snapshot_in_progress) {
        pthread_join(g_snapshot_thread, NULL);
        g_snapshot_in_progress = 0;
    }
}

// -----------------------------------------------------------------------------
// Create a snapshot asynchronously
// -----------------------------------------------------------------------------
static void create_snapshot_async(void)
{
    // Wait for any previous snapshot to complete
    wait_for_snapshot_if_needed();

    // Create a copy of the current state for the background thread
    SnapshotTask *task = (SnapshotTask *)malloc(sizeof(SnapshotTask));
    if (!task) {
        perror("malloc for snapshot task failed");
        return;
    }

    task->snapshot_state = (Account *)malloc(g_max_accounts * sizeof(Account));
    if (!task->snapshot_state) {
        perror("malloc for snapshot state failed");
        free(task);
        return;
    }

    // Copy current state
    memcpy(task->snapshot_state, g_state, g_max_accounts * sizeof(Account));
    task->account_count = g_max_accounts;

    // Mark snapshot as in progress and start the thread
    g_snapshot_in_progress = 1;
    if (pthread_create(&g_snapshot_thread, NULL, snapshot_worker, task) != 0) {
        perror("pthread_create for snapshot failed");
        free(task->snapshot_state);
        free(task);
        g_snapshot_in_progress = 0;
    }
}

// -----------------------------------------------------------------------------
// Main function
// -----------------------------------------------------------------------------
int main(int argc, char *argv[])
{
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

    struct timespec recovery_start, recovery_end;
    struct timespec replay_start, replay_end;

    printf("=== Parameterized Log-Optimized State Management with Memset Support ===\n");
    printf("Configuration:\n");
    printf("  MAX_ACCOUNTS: %lu\n", g_max_accounts);
    printf("  BATCH_SIZE: %d\n", BATCH_SIZE);
    printf("  TOTAL_BATCHES: %d\n", TOTAL_BATCHES);
    printf("  MAX_LOG_BATCHES: %d\n", MAX_LOG_BATCHES);
    printf("  Memory usage: ~%.1f MB for account state\n", 
           (double)(g_max_accounts * sizeof(Account)) / (1024.0 * 1024.0));
    printf("  Supports: Balance transfers (func=0) and Range-set operations (func=1)\n\n");

    clock_gettime(CLOCK_MONOTONIC, &recovery_start);

    // Step 1: Load or initialize state
    load_or_init_state();

    // Step 2: Replay log if it exists
    clock_gettime(CLOCK_MONOTONIC, &replay_start);
    uint64_t replayed_tx = replay_log();
    clock_gettime(CLOCK_MONOTONIC, &replay_end);
    double replay_elapsed_ms = timespec_diff_ms(&replay_start, &replay_end);

    clock_gettime(CLOCK_MONOTONIC, &recovery_end);
    double recovery_elapsed_ms = timespec_diff_ms(&recovery_start, &recovery_end);
    
    printf("Recovery phase completed in %.3f ms (replay: %.3f ms)\n", 
           recovery_elapsed_ms, replay_elapsed_ms);

    // Open transaction file
    FILE *tx_file = fopen(TX_FILE, "rb");
    if (!tx_file) {
        perror("Error opening transaction file");
        free(g_state);
        exit(EXIT_FAILURE);
    }

    // Open log file for appending
    FILE *log_fp = fopen(LOG_FILE, "ab");
    if (!log_fp) {
        perror("Error opening log file");
        fclose(tx_file);
        free(g_state);
        exit(EXIT_FAILURE);
    }

    // Allocate batch buffer
    Transaction *batch = (Transaction *)malloc(BATCH_SIZE * sizeof(Transaction));
    if (!batch) {
        perror("Batch malloc failed");
        fclose(log_fp);
        fclose(tx_file);
        free(g_state);
        exit(EXIT_FAILURE);
    }

    // We'll store each batch's processing time for stats
    double *batch_times = (double *)malloc(TOTAL_BATCHES * sizeof(double));
    double *commit_times = (double *)malloc(TOTAL_BATCHES * sizeof(double));
    if (!batch_times || !commit_times) {
        perror("malloc for time arrays failed");
        free(batch);
        fclose(log_fp);
        fclose(tx_file);
        free(g_state);
        exit(EXIT_FAILURE);
    }

    double total_elapsed_ms = 0.0;
    double total_commit_ms = 0.0;
    uint64_t total_successful_tx = 0;
    uint64_t total_failed_tx = 0;

    printf("\nStarting transaction processing...\n");

    // Process transactions in batches
    for (uint64_t iteration = 0; iteration < TOTAL_BATCHES; iteration++) {
        struct timespec start, end;
        struct timespec commit_start, commit_end;
        clock_gettime(CLOCK_MONOTONIC, &start);

        // Read BATCH_SIZE transactions
        size_t read_count = fread(batch, sizeof(Transaction), BATCH_SIZE, tx_file);
        if (read_count < BATCH_SIZE) {
            if (feof(tx_file)) {
                // Loop back to start if at EOF
                rewind(tx_file);
                read_count = fread(batch, sizeof(Transaction), BATCH_SIZE, tx_file);
            } else {
                perror("Error reading transaction batch");
                free(batch_times);
                free(commit_times);
                free(batch);
                fclose(log_fp);
                fclose(tx_file);
                free(g_state);
                exit(EXIT_FAILURE);
            }
        }

        // Apply transactions in-memory with enhanced support
        uint64_t batch_successful = 0;
        uint64_t batch_failed = 0;
        
        for (size_t i = 0; i < read_count; i++) {
            if (apply_transaction_to_state_array(&batch[i])) {
                batch_successful++;
            } else {
                batch_failed++;
            }
        }
        
        total_successful_tx += batch_successful;
        total_failed_tx += batch_failed;

        // Start commit time measurement
        clock_gettime(CLOCK_MONOTONIC, &commit_start);

        // Append those transactions to the log
        if (fwrite(batch, sizeof(Transaction), read_count, log_fp) != read_count) {
            perror("Error writing to log");
            free(batch_times);
            free(commit_times);
            free(batch);
            fclose(log_fp);
            fclose(tx_file);
            free(g_state);
            exit(EXIT_FAILURE);
        }

        // ---------------------------------------------------------------------
        // ALWAYS FSync here to ensure this batch is safely on disk.
        // ---------------------------------------------------------------------
        fflush(log_fp);
        if (fsync(fileno(log_fp)) != 0) {
            perror("fsync on log");
            free(batch_times);
            free(commit_times);
            free(batch);
            fclose(log_fp);
            fclose(tx_file);
            free(g_state);
            exit(EXIT_FAILURE);
        }

        // End commit time measurement
        clock_gettime(CLOCK_MONOTONIC, &commit_end);
        double commit_elapsed_ms = timespec_diff_ms(&commit_start, &commit_end);
        commit_times[iteration] = commit_elapsed_ms;
        total_commit_ms += commit_elapsed_ms;

        clock_gettime(CLOCK_MONOTONIC, &end);
        double elapsed_ms = timespec_diff_ms(&start, &end);
        batch_times[iteration] = elapsed_ms;
        total_elapsed_ms += elapsed_ms;

        g_processed_batches++;
        
        if ((iteration + 1) % 10 == 0 || iteration == 0) {
            printf("Batch %llu of %llu processed in %.3f ms (commit: %.3f ms, success: %llu, failed: %llu)\n",
                   (unsigned long long)(iteration + 1),
                   (unsigned long long)TOTAL_BATCHES,
                   elapsed_ms,
                   commit_elapsed_ms,
                   (unsigned long long)batch_successful,
                   (unsigned long long)batch_failed);
        }

        // Check if it's time for a snapshot
        if (g_processed_batches % MAX_LOG_BATCHES == 0) {
            struct timespec snapshot_trigger_start, snapshot_trigger_end;
            clock_gettime(CLOCK_MONOTONIC, &snapshot_trigger_start);
            
            // We already fsync'ed above, so the log is safe
            // Close the current log file so snapshot thread can remove it
            fclose(log_fp);

            // Launch the asynchronous snapshot
            create_snapshot_async();
            
            clock_gettime(CLOCK_MONOTONIC, &snapshot_trigger_end);
            double snapshot_trigger_ms = timespec_diff_ms(&snapshot_trigger_start, &snapshot_trigger_end);
            printf("Snapshot triggered in %.3f ms (state copy + thread spawn)\n", snapshot_trigger_ms);

            // Open a fresh log file so main thread can keep processing
            log_fp = fopen(LOG_FILE, "ab");
            if (!log_fp) {
                perror("Error re-opening log file");
                free(batch_times);
                free(commit_times);
                free(batch);
                fclose(tx_file);
                free(g_state);
                exit(EXIT_FAILURE);
            }
        }
    }

    // Done with all batches
    double avg_elapsed_ms = total_elapsed_ms / (double)TOTAL_BATCHES;
    double avg_commit_ms = total_commit_ms / (double)TOTAL_BATCHES;
    double total_tx = total_successful_tx + total_failed_tx;
    double success_rate = (total_tx > 0) ? (total_successful_tx * 100.0 / total_tx) : 0.0;
    
    printf("\n=== Processing Summary ===\n");
    printf("Processed %d batches total.\n", TOTAL_BATCHES);
    printf("Total transactions: %llu (successful: %llu, failed: %llu)\n", 
           (unsigned long long)total_tx, 
           (unsigned long long)total_successful_tx, 
           (unsigned long long)total_failed_tx);
    printf("Success rate: %.2f%%\n", success_rate);
    printf("Total time:    %.3f ms\n", total_elapsed_ms);
    printf("Avg per batch: %.3f ms\n", avg_elapsed_ms);
    printf("Avg commit:    %.3f ms\n", avg_commit_ms);
    printf("Throughput:    %.0f tx/sec\n", total_tx * 1000.0 / total_elapsed_ms);

    // -------------------------------------------------------------------------
    // Calculate median, p90, p99 for both total time and commit time
    // -------------------------------------------------------------------------
    qsort(batch_times, TOTAL_BATCHES, sizeof(double), cmp_doubles);
    qsort(commit_times, TOTAL_BATCHES, sizeof(double), cmp_doubles);

    // median calculations for both
    double median_ms, median_commit_ms;
    if (TOTAL_BATCHES % 2 == 0) {
        // even
        int mid = TOTAL_BATCHES / 2;
        median_ms = (batch_times[mid - 1] + batch_times[mid]) / 2.0;
        median_commit_ms = (commit_times[mid - 1] + commit_times[mid]) / 2.0;
    } else {
        // odd
        median_ms = batch_times[TOTAL_BATCHES / 2];
        median_commit_ms = commit_times[TOTAL_BATCHES / 2];
    }

    // 90th percentile
    int idx_90 = (int)ceil(0.90 * TOTAL_BATCHES) - 1;  // 0-based index
    if (idx_90 < 0) idx_90 = 0;
    if (idx_90 >= (int)TOTAL_BATCHES) idx_90 = TOTAL_BATCHES - 1;
    double p90_ms = batch_times[idx_90];
    double p90_commit_ms = commit_times[idx_90];

    // 99th percentile
    int idx_99 = (int)ceil(0.99 * TOTAL_BATCHES) - 1;
    if (idx_99 < 0) idx_99 = 0;
    if (idx_99 >= (int)TOTAL_BATCHES) idx_99 = TOTAL_BATCHES - 1;
    double p99_ms = batch_times[idx_99];
    double p99_commit_ms = commit_times[idx_99];

    printf("\nLatency statistics for %d batches:\n", TOTAL_BATCHES);
    printf("  Median:    %.3f ms\n", median_ms);
    printf("  p90:       %.3f ms\n", p90_ms);
    printf("  p99:       %.3f ms\n", p99_ms);

    printf("\nCommit time statistics for %d batches:\n", TOTAL_BATCHES);
    printf("  Median:    %.3f ms\n", median_commit_ms);
    printf("  p90:       %.3f ms\n", p90_commit_ms);
    printf("  p99:       %.3f ms\n", p99_commit_ms);

    // Cleanup
    free(batch_times);
    free(commit_times);
    free(batch);
    fclose(tx_file);
    fclose(log_fp);

    // Before exit, wait if a snapshot is still in progress
    wait_for_snapshot_if_needed();

    free(g_state);
    printf("\nExecution complete.\n");
    return 0;
} 