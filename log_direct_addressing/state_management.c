#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <time.h>
#include <errno.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>

#define BATCH_SIZE        (1 << 16)  
#define INITIAL_BALANCE   1000000UL
#define MAX_ACCOUNTS      2000000UL
#define TOTAL_BATCHES     50         
#define MAX_LOG_BATCHES   5          // Create a new snapshot after 5 batches (example)

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

// Global in-memory state
static Account *state = NULL;
static uint64_t processed_batches = 0;

//
// Function to get a time difference in milliseconds
//
static double timespec_diff_ms(const struct timespec *start, const struct timespec *end) {
    double secs  = (double)(end->tv_sec - start->tv_sec);
    double nsecs = (double)(end->tv_nsec - start->tv_nsec) / 1e9;
    return (secs + nsecs) * 1000.0;
}

//
// Function to initialize or load an existing state snapshot
//
static void load_or_init_state(void) {
    FILE *snapshot = fopen(SNAPSHOT_FILE, "rb");
    if (!snapshot) {
        // Snapshot doesn't exist -> Create a fresh state
        state = (Account *)malloc(MAX_ACCOUNTS * sizeof(Account));
        if (!state) {
            perror("Error allocating state array");
            exit(EXIT_FAILURE);
        }
        for (uint64_t i = 0; i < MAX_ACCOUNTS; i++) {
            state[i].address = i;
            state[i].balance = INITIAL_BALANCE;
        }
        printf("No existing snapshot found. Initialized fresh state.\n");
        return;
    }

    // Load existing snapshot
    state = (Account *)malloc(MAX_ACCOUNTS * sizeof(Account));
    if (!state) {
        perror("Error allocating state array");
        fclose(snapshot);
        exit(EXIT_FAILURE);
    }
    fread(state, sizeof(Account), MAX_ACCOUNTS, snapshot);
    fclose(snapshot);
    
    printf("Loaded existing snapshot from %s\n", SNAPSHOT_FILE);
}

//
// Rebuild state from log
//
static void replay_log(void) {
    FILE *log_fp = fopen(LOG_FILE, "rb");
    if (!log_fp) {
        // No log yet, nothing to replay
        printf("No log file found to replay. Continuing...\n");
        return;
    }

    Transaction tx;
    uint64_t tx_count = 0;
    while (fread(&tx, sizeof(Transaction), 1, log_fp) == 1) {
        // Apply transaction
        if (state[tx.sender].balance >= tx.amount) {
            state[tx.sender].balance -= tx.amount;
            state[tx.receiver].balance += tx.amount;
        }
        tx_count++;
    }
    fclose(log_fp);

    if (tx_count > 0) {
        printf("Replayed %llu transactions from the log.\n", tx_count);
    } else {
        printf("Log file was empty, no transactions replayed.\n");
    }
}

//
// Persist state to a snapshot file, then clear the log
//
static void create_snapshot_and_reset_log(void) {
    // 1. Write full state to snapshot
    FILE *snapshot = fopen(SNAPSHOT_FILE, "wb");
    if (!snapshot) {
        perror("Error opening snapshot file for writing");
        exit(EXIT_FAILURE);
    }

    size_t written = fwrite(state, sizeof(Account), MAX_ACCOUNTS, snapshot);
    fflush(snapshot);
    int fd = fileno(snapshot);
    if (fsync(fd) != 0) {
        perror("fsync() failed on snapshot file");
    }
    fclose(snapshot);

    if (written != MAX_ACCOUNTS) {
        fprintf(stderr, "Error writing entire snapshot. Written=%zu\n", written);
        exit(EXIT_FAILURE);
    }

    printf("Snapshot created with full state.\n");

    // 2. Remove or truncate the log file
    if (remove(LOG_FILE) != 0 && errno != ENOENT) {
        // It's okay if the log file doesn't exist
        perror("Error removing log file");
        exit(EXIT_FAILURE);
    }
    printf("Log has been reset (old log file removed).\n");
}

int main() {
    load_or_init_state();
    // Replay any pending transactions in the log
    replay_log();

    // Open transaction file (generated previously)
    FILE *tx_file = fopen(TX_FILE, "rb");
    if (!tx_file) {
        perror("Error opening transactions file");
        free(state);
        exit(EXIT_FAILURE);
    }

    // Open log file in append mode
    FILE *log_fp = fopen(LOG_FILE, "ab");
    if (!log_fp) {
        perror("Error opening log file for append");
        free(state);
        fclose(tx_file);
        exit(EXIT_FAILURE);
    }

    Transaction *batch = (Transaction *)malloc(BATCH_SIZE * sizeof(Transaction));
    if (!batch) {
        perror("Error allocating memory for transaction batch");
        fclose(log_fp);
        fclose(tx_file);
        free(state);
        exit(EXIT_FAILURE);
    }

    // Timing variables
    double total_elapsed_ms = 0.0;

    // Process up to TOTAL_BATCHES (example)
    for (uint64_t iteration = 0; iteration < TOTAL_BATCHES; iteration++) {
        // Start timing
        struct timespec start, end;
        clock_gettime(CLOCK_MONOTONIC, &start);

        // Read a batch of transactions
        size_t read_count = fread(batch, sizeof(Transaction), BATCH_SIZE, tx_file);
        if (read_count < BATCH_SIZE) {
            if (feof(tx_file)) {
                // Wrap around to beginning
                rewind(tx_file);
                read_count = fread(batch, sizeof(Transaction), BATCH_SIZE, tx_file);
            } else {
                perror("Error reading transactions");
                free(batch);
                fclose(tx_file);
                fclose(log_fp);
                free(state);
                exit(EXIT_FAILURE);
            }
        }

        // Apply in-memory
        for (size_t i = 0; i < read_count; i++) {
            Transaction *tx = &batch[i];
            if (state[tx->sender].balance >= tx->amount) {
                state[tx->sender].balance -= tx->amount;
                state[tx->receiver].balance += tx->amount;
            }
        }

        // Append to the log
        size_t written = fwrite(batch, sizeof(Transaction), read_count, log_fp);
        if (written != read_count) {
            perror("Error writing to log file");
            free(batch);
            fclose(tx_file);
            fclose(log_fp);
            free(state);
            exit(EXIT_FAILURE);
        }
        fflush(log_fp);
        fsync(fileno(log_fp));

        // End timing
        clock_gettime(CLOCK_MONOTONIC, &end);
        double elapsed_ms = timespec_diff_ms(&start, &end);
        total_elapsed_ms += elapsed_ms;

        processed_batches++;
        printf("Batch %llu / %lu processed in %.3f ms\n",
               iteration + 1, (unsigned long)TOTAL_BATCHES, elapsed_ms);

        // Check if we reached the threshold for log-based snapshot
        if (processed_batches % MAX_LOG_BATCHES == 0) {
            create_snapshot_and_reset_log();
            // Reopen log file after removing
            log_fp = fopen(LOG_FILE, "ab");
            if (!log_fp) {
                perror("Error re-opening log file after snapshot");
                free(batch);
                fclose(tx_file);
                free(state);
                exit(EXIT_FAILURE);
            }
        }
    }

    double avg_elapsed_ms = total_elapsed_ms / (double)TOTAL_BATCHES;
    printf("\nProcessed %d batches. Total time: %.3f ms, average per batch: %.3f ms\n",
           TOTAL_BATCHES, total_elapsed_ms, avg_elapsed_ms);

    free(batch);
    fclose(tx_file);
    fclose(log_fp);
    free(state);

    return 0;
}