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

#define BATCH_SIZE        (1 << 16)  // 2^16 transactions per batch
#define INITIAL_BALANCE   1000000UL
#define MAX_ACCOUNTS      500000000UL
#define TOTAL_BATCHES     5000         // Process 50 batches total
#define MAX_LOG_BATCHES   100          // Snapshot every 5 batches

// Filenames
#define TX_FILE           "transactions.bin"
#define LOG_FILE          "state_log.bin"
#define SNAPSHOT_FILE     "state_snapshot.bin"

typedef struct {
    uint64_t sender;
    uint64_t receiver;
    uint32_t amount;
} Transaction;

typedef struct {
    uint64_t address;
    uint64_t balance;
} Account;

// -----------------------------------------------------------------------------
// Global in-memory state
// -----------------------------------------------------------------------------
static Account *g_state = NULL;
static uint64_t g_processed_batches = 0;

// Background snapshot task structure
typedef struct {
    Account *snapshot_state;  // Copy of accounts to write
    size_t   account_count;
} SnapshotTask;

// For managing the background snapshot thread
static pthread_t  g_snapshot_thread;
static int        g_snapshot_in_progress = 0;

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
// Load existing snapshot or create fresh state if none
// -----------------------------------------------------------------------------
static void load_or_init_state(void)
{
    FILE *snapshot = fopen(SNAPSHOT_FILE, "rb");
    if (!snapshot) {
        // No snapshot => create fresh state
        g_state = (Account *)malloc(MAX_ACCOUNTS * sizeof(Account));
        if (!g_state) {
            perror("malloc for state failed");
            exit(EXIT_FAILURE);
        }
        for (uint64_t i = 0; i < MAX_ACCOUNTS; i++) {
            g_state[i].address = i;
            g_state[i].balance = INITIAL_BALANCE;
        }
        printf("No snapshot found; initialized fresh state.\n");
        return;
    }

    // Load snapshot
    g_state = (Account *)malloc(MAX_ACCOUNTS * sizeof(Account));
    if (!g_state) {
        perror("malloc for state failed");
        fclose(snapshot);
        exit(EXIT_FAILURE);
    }
    size_t read_items = fread(g_state, sizeof(Account), MAX_ACCOUNTS, snapshot);
    fclose(snapshot);
    if (read_items != MAX_ACCOUNTS) {
        fprintf(stderr, "Snapshot file incomplete or corrupted.\n");
        free(g_state);
        exit(EXIT_FAILURE);
    }
    printf("Loaded existing snapshot from %s\n", SNAPSHOT_FILE);
}

// -----------------------------------------------------------------------------
// Replay any existing log into g_state
// -----------------------------------------------------------------------------
static uint64_t replay_log(void)
{
    FILE *log_fp = fopen(LOG_FILE, "rb");
    if (!log_fp) {
        printf("No log file found; nothing to replay.\n");
        return 0;
    }
    Transaction tx;
    uint64_t tx_count = 0;
    while (fread(&tx, sizeof(Transaction), 1, log_fp) == 1) {
        if (g_state[tx.sender].balance >= tx.amount) {
            g_state[tx.sender].balance -= tx.amount;
            g_state[tx.receiver].balance += tx.amount;
        }
        tx_count++;
    }
    fclose(log_fp);

    if (tx_count > 0)
        printf("Replayed %llu transactions from log.\n", (unsigned long long)tx_count);
    else
        printf("Log file was empty.\n");

    return tx_count;
}

// -----------------------------------------------------------------------------
// Background thread function to write a snapshot
// -----------------------------------------------------------------------------
static void *snapshot_worker(void *arg)
{
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
    
    if (written != task->account_count) {
        fprintf(stderr, "Error writing snapshot: wrote=%zu, expected=%zu\n",
                written, task->account_count);
    } else {
        printf("[Async] Snapshot created with full state.\n");
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
// Start an asynchronous snapshot: copy current state, spawn a worker thread
// -----------------------------------------------------------------------------
static void create_snapshot_async(void)
{
    wait_for_snapshot_if_needed();

    // Make a copy of the entire state
    Account *copy_state = (Account *)malloc(MAX_ACCOUNTS * sizeof(Account));
    if (!copy_state) {
        perror("malloc for snapshot copy failed");
        return;
    }
    memcpy(copy_state, g_state, MAX_ACCOUNTS * sizeof(Account));

    // Package the snapshot task
    SnapshotTask *task = (SnapshotTask *)malloc(sizeof(SnapshotTask));
    if (!task) {
        perror("malloc for SnapshotTask failed");
        free(copy_state);
        return;
    }
    task->snapshot_state = copy_state;
    task->account_count  = MAX_ACCOUNTS;

    g_snapshot_in_progress = 1;
    // Spawn the background thread
    if (pthread_create(&g_snapshot_thread, NULL, snapshot_worker, task) != 0) {
        perror("Failed to create snapshot thread");
        g_snapshot_in_progress = 0;
        free(copy_state);
        free(task);
    }
}

// -----------------------------------------------------------------------------
// Main
// -----------------------------------------------------------------------------
int main(void)
{
    struct timespec recovery_start, recovery_end;
    struct timespec snapshot_start, snapshot_end;
    struct timespec replay_start, replay_end;
    double snapshot_time_ms = 0.0;
    double replay_time_ms = 0.0;
    uint64_t replayed_tx_count = 0;
    
    // Start recovery timing
    clock_gettime(CLOCK_MONOTONIC, &recovery_start);
    
    // Time snapshot loading
    clock_gettime(CLOCK_MONOTONIC, &snapshot_start);
    load_or_init_state();
    clock_gettime(CLOCK_MONOTONIC, &snapshot_end);
    snapshot_time_ms = timespec_diff_ms(&snapshot_start, &snapshot_end);
    
    // Time log replay
    clock_gettime(CLOCK_MONOTONIC, &replay_start);
    replayed_tx_count = replay_log();
    clock_gettime(CLOCK_MONOTONIC, &replay_end);
    replay_time_ms = timespec_diff_ms(&replay_start, &replay_end);
    
    // End recovery timing
    clock_gettime(CLOCK_MONOTONIC, &recovery_end);
    double recovery_time_ms = timespec_diff_ms(&recovery_start, &recovery_end);
    
    // Print recovery statistics
    printf("\n--- Recovery Performance ---\n");
    printf("Total recovery time: %.3f ms\n", recovery_time_ms);
    printf("  - Snapshot loading: %.3f ms\n", snapshot_time_ms);
    printf("  - Log replay: %.3f ms (%llu transactions, %.1f tx/ms)\n", 
           replay_time_ms, (unsigned long long)replayed_tx_count,
           replayed_tx_count > 0 ? (double)replayed_tx_count / replay_time_ms : 0.0);
    printf("----------------------------\n\n");

    // Open main transaction file
    FILE *tx_file = fopen(TX_FILE, "rb");
    if (!tx_file) {
        perror("Error opening transactions file");
        free(g_state);
        exit(EXIT_FAILURE);
    }

    // Open log in append mode
    FILE *log_fp = fopen(LOG_FILE, "ab");
    if (!log_fp) {
        perror("Error opening log file");
        fclose(tx_file);
        free(g_state);
        exit(EXIT_FAILURE);
    }

    // Allocate one-time batch buffer
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
    double *commit_times = (double *)malloc(TOTAL_BATCHES * sizeof(double)); // Add array for commit times
    if (!batch_times || !commit_times) {
        perror("malloc for time arrays failed");
        free(batch);
        fclose(log_fp);
        fclose(tx_file);
        free(g_state);
        exit(EXIT_FAILURE);
    }

    double total_elapsed_ms = 0.0;
    double total_commit_ms = 0.0;  // Track total commit time

    // Process transactions in batches
    for (uint64_t iteration = 0; iteration < TOTAL_BATCHES; iteration++) {
        struct timespec start, end;
        struct timespec commit_start, commit_end;  // For tracking commit time
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

        // Apply transactions in-memory
        for (size_t i = 0; i < read_count; i++) {
            Transaction *tx = &batch[i];
            if (g_state[tx->sender].balance >= tx->amount) {
                g_state[tx->sender].balance -= tx->amount;
                g_state[tx->receiver].balance += tx->amount;
            }
        }

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
        batch_times[iteration] = elapsed_ms;  // store this batch's latency
        total_elapsed_ms += elapsed_ms;

        g_processed_batches++;
        printf("Batch %llu of %llu processed in %.3f ms (commit: %.3f ms)\n",
               (unsigned long long)(iteration + 1),
               (unsigned long long)TOTAL_BATCHES,
               elapsed_ms,
               commit_elapsed_ms);

        // Check if it's time for a snapshot
        if (g_processed_batches % MAX_LOG_BATCHES == 0) {
            // We already fsync'ed above, so the log is safe
            // Close the current log file so snapshot thread can remove it
            fclose(log_fp);

            // Launch the asynchronous snapshot
            create_snapshot_async();

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
    printf("\nProcessed %d batches total.\n", TOTAL_BATCHES);
    printf("Total time:    %.3f ms\n", total_elapsed_ms);
    printf("Avg per batch: %.3f ms\n", avg_elapsed_ms);
    printf("Avg commit:    %.3f ms\n", avg_commit_ms);

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
    return 0;
}