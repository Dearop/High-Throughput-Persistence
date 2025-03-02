#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <time.h>
#include <errno.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <pthread.h>   // For pthread_create, etc.

#define BATCH_SIZE        (1 << 16)  // 2^16 transactions per batch
#define INITIAL_BALANCE   1000000UL
#define MAX_ACCOUNTS      2000000UL
#define TOTAL_BATCHES     50         // Process 50 batches total
#define MAX_LOG_BATCHES   5          // Snapshot every 5 batches

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
// Global in-memory state (actively updated by the main thread)
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
// Utility: compute elapsed time in milliseconds
static inline double timespec_diff_ms(const struct timespec *start, const struct timespec *end) {
    double secs  = (double)(end->tv_sec - start->tv_sec);
    double nsecs = (double)(end->tv_nsec - start->tv_nsec) / 1e9;
    return (secs + nsecs) * 1000.0;
}

// -----------------------------------------------------------------------------
// Load existing snapshot or create a fresh state if none
static void load_or_init_state(void) {
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
static void replay_log(void) {
    FILE *log_fp = fopen(LOG_FILE, "rb");
    if (!log_fp) {
        printf("No log file found; nothing to replay.\n");
        return;
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
}

// -----------------------------------------------------------------------------
// Background thread function to write a snapshot
static void *snapshot_worker(void *arg) {
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
        // fall through
    } else {
        printf("[Async] Snapshot created with full state.\n");
    }

    // After snapshot is written, remove/truncate the old log
    if (remove(LOG_FILE) != 0 && errno != ENOENT) {
        perror("[Async] Error removing old log");
        // not a fatal error for the background thread
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
static void wait_for_snapshot_if_needed(void) {
    if (g_snapshot_in_progress) {
        // Join on the existing snapshot thread before launching another
        pthread_join(g_snapshot_thread, NULL);
        g_snapshot_in_progress = 0;
    }
}

// -----------------------------------------------------------------------------
// Start an asynchronous snapshot: copy current state, spawn a worker thread
static void create_snapshot_async(void) {
    wait_for_snapshot_if_needed();

    // Make a copy of the entire state to avoid concurrency issues
    Account *copy_state = (Account *)malloc(MAX_ACCOUNTS * sizeof(Account));
    if (!copy_state) {
        perror("malloc for snapshot copy failed");
        return; // or exit
    }
    memcpy(copy_state, g_state, MAX_ACCOUNTS * sizeof(Account));

    // Package the snapshot task
    SnapshotTask *task = (SnapshotTask *)malloc(sizeof(SnapshotTask));
    if (!task) {
        perror("malloc for SnapshotTask failed");
        free(copy_state);
        return; // or exit
    }
    task->snapshot_state = copy_state;
    task->account_count  = MAX_ACCOUNTS;

    g_snapshot_in_progress = 1;
    // Spawn background thread
    if (pthread_create(&g_snapshot_thread, NULL, snapshot_worker, task) != 0) {
        perror("Failed to create snapshot thread");
        g_snapshot_in_progress = 0;
        free(copy_state);
        free(task);
    }
}

// -----------------------------------------------------------------------------

int main(void) {
    load_or_init_state();
    replay_log();

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

    double total_elapsed_ms = 0.0;

    // Process transactions in batches
    for (uint64_t iteration = 0; iteration < TOTAL_BATCHES; iteration++) {
        struct timespec start, end;
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

        // Append those transactions to the log
        if (fwrite(batch, sizeof(Transaction), read_count, log_fp) != read_count) {
            perror("Error writing to log");
            free(batch);
            fclose(log_fp);
            fclose(tx_file);
            free(g_state);
            exit(EXIT_FAILURE);
        }
        // For performance, skip fsync here. The snapshot thread will remove
        // the log after it finishes writing a snapshot.

        clock_gettime(CLOCK_MONOTONIC, &end);
        double elapsed_ms = timespec_diff_ms(&start, &end);
        total_elapsed_ms += elapsed_ms;

        g_processed_batches++;
        printf("Batch %llu of %llu processed in %.3f ms\n",
               (unsigned long long)(iteration + 1),
               (unsigned long long)TOTAL_BATCHES,
               elapsed_ms);

        // Check if it's time for a snapshot
        if (g_processed_batches % MAX_LOG_BATCHES == 0) {
            // Flush the log so the background thread sees it fully
            fflush(log_fp);
            fsync(fileno(log_fp));  // Ensure it's on disk
            // Close the current log file
            fclose(log_fp);

            // Launch the asynchronous snapshot
            create_snapshot_async();

            // Open a fresh log file so main thread can keep processing
            log_fp = fopen(LOG_FILE, "ab");
            if (!log_fp) {
                perror("Error re-opening log file");
                free(batch);
                fclose(tx_file);
                free(g_state);
                exit(EXIT_FAILURE);
            }
        }
    }

    // Done with all batches
    double avg_elapsed_ms = total_elapsed_ms / (double)TOTAL_BATCHES;
    printf("\nProcessed %d batches total.\n", TOTAL_BATCHES);
    printf("Total time:    %.3f ms\n", total_elapsed_ms);
    printf("Avg per batch: %.3f ms\n", avg_elapsed_ms);

    // Clean up
    free(batch);
    fclose(tx_file);
    fclose(log_fp);

    // Before exit, wait if a snapshot is still in progress
    wait_for_snapshot_if_needed();

    free(g_state);
    return 0;
}