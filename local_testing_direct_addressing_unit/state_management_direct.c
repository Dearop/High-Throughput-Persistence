#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <unistd.h>
#include <time.h>

#define BATCH_SIZE (1 << 16)       // 2^16 transactions per batch
#define TX_FILE "transactions.bin"
#define STATE_FILE_PREFIX "state_"
#define MAX_VERSIONS 10            // Keep last 10 versions of state
#define INITIAL_BALANCE 1000000L   // Each account starts with this balance
#define MAX_ACCOUNTS 2000000UL     // Number of accounts (addresses 0 to MAX_ACCOUNTS-1)

typedef struct {
    uint64_t sender;
    uint64_t receiver;
    uint32_t amount;
} Transaction;

typedef struct {
    uint64_t address;   // This will be equal to the index.
    uint64_t balance;
} Account;

// Global state array.
Account state[MAX_ACCOUNTS];

int main() {
    
    FILE *tx_file = fopen(TX_FILE, "rb");
    if (!tx_file) {
        perror("Error opening transactions file");
        exit(EXIT_FAILURE);
    }
    
    Transaction *batch = malloc(BATCH_SIZE * sizeof(Transaction));
    if (!batch) {
        perror("Error allocating memory for transaction batch");
        fclose(tx_file);
        exit(EXIT_FAILURE);
    }
    
    unsigned long iteration = 0;
    size_t read_count;
    while (1) {
        printf("\nPress ENTER to process the next batch of transactions...\n");
        getchar();  // Wait for user input.
        
        struct timespec start, end;
        clock_gettime(CLOCK_MONOTONIC, &start);
        
        // Read a batch of transactions.
        read_count = fread(batch, sizeof(Transaction), BATCH_SIZE, tx_file);
        if (read_count < BATCH_SIZE) {
            if (feof(tx_file)) {
                // Rewind if at end-of-file.
                rewind(tx_file);
                read_count = fread(batch, sizeof(Transaction), BATCH_SIZE, tx_file);
            } else {
                perror("Error reading transactions");
                break;
            }
        }
        
        // Process each transaction directly.
        for (size_t i = 0; i < read_count; i++) {
            Transaction *tx = &batch[i];
            if (state[tx->sender].balance >= tx->amount) {
                state[tx->sender].balance -= tx->amount;
                state[tx->receiver].balance += tx->amount;
            }
        }
        
        int version = iteration % MAX_VERSIONS;
        char old_filename[256];
        snprintf(old_filename, sizeof(old_filename), "%s%d.bin", STATE_FILE_PREFIX, version);
        if (access(old_filename, F_OK) == 0) {
            if (remove(old_filename) == 0) {
                printf("Removed old state file %s\n", old_filename);
            } else {
                perror("Error removing old state file");
            }
        }
        
        // Save the full state to disk.
        char filename[256];
        snprintf(filename, sizeof(filename), "%s%d.bin", STATE_FILE_PREFIX, version);
        FILE *f = fopen(filename, "wb");
        if (!f) {
            perror("Error opening state file for writing");
            exit(EXIT_FAILURE);
        }
        if (fwrite(state, sizeof(Account), MAX_ACCOUNTS, f) != MAX_ACCOUNTS) {
            perror("Error writing state file");
            fclose(f);
            exit(EXIT_FAILURE);
        }
        fclose(f);
        printf("State saved to %s\n", filename);
        
        clock_gettime(CLOCK_MONOTONIC, &end);
        double elapsed_ms = (end.tv_sec - start.tv_sec) * 1000.0 +
                            (end.tv_nsec - start.tv_nsec) / 1000000.0;
        printf("Processed %zu transactions in %.3f ms\n", read_count, elapsed_ms);
        
        iteration++;
    }
    
    free(batch);
    fclose(tx_file);
    return 0;
}