/* state_updater_dynamic.c */
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <unistd.h>
#include <time.h>

#define BATCH_SIZE (1 << 16)       // 2^16 transactions per batch
#define TX_FILE "transactions.bin"
#define STATE_FILE_PREFIX "state_"
#define MAX_VERSIONS 10            // Keep last 10 versions of state
#define INITIAL_BALANCE 1000000L   // New accounts start with this balance
#define MAX_ACCOUNTS 2000000UL      // Maximum number of accounts we can track
#define HASH_TABLE_SIZE (1 << 21)  // Must be a power of 2

// Structure representing a transaction.
typedef struct {
    uint64_t sender;
    uint64_t receiver;
    uint32_t amount;
} Transaction;

// Structure representing an account.
typedef struct {
    uint64_t address;
    uint64_t balance;
} Account;

// Global state array and counter.
Account state[MAX_ACCOUNTS];
size_t num_accounts = 0;

// --- Hash table for fast account lookups ---
typedef struct {
    uint64_t key;
    size_t index;
    int used;
} HashEntry;

HashEntry hash_table[HASH_TABLE_SIZE];

// 64-bit hash function.
static inline uint64_t hash_uint64(uint64_t key) {
    key ^= key >> 33;
    key *= 0xff51afd7ed558ccdULL;
    key ^= key >> 33;
    key *= 0xc4ceb9fe1a85ec53ULL;
    key ^= key >> 33;
    return key;
}

// Insert (key, index) into the hash table.
void hash_table_insert(uint64_t key, size_t index) {
    uint64_t h = hash_uint64(key) & (HASH_TABLE_SIZE - 1);
    while (hash_table[h].used) {
        if (hash_table[h].key == key) { // Key already exists.
            hash_table[h].index = index;
            return;
        }
        h = (h + 1) & (HASH_TABLE_SIZE - 1);
    }
    hash_table[h].used = 1;
    hash_table[h].key = key;
    hash_table[h].index = index;
}

// Look up an account index by its address.
size_t find_account_index_ht(uint64_t key) {
    uint64_t h = hash_uint64(key) & (HASH_TABLE_SIZE - 1);
    while (hash_table[h].used) {
        if (hash_table[h].key == key) {
            return hash_table[h].index;
        }
        h = (h + 1) & (HASH_TABLE_SIZE - 1);
    }
    return (size_t)-1;  // Not found.
}

// Get the index of an account for a given address. If not found, create a new account.
size_t get_account_index(uint64_t address) {
    size_t idx = find_account_index_ht(address);
    if (idx == (size_t)-1) {
        if (num_accounts >= MAX_ACCOUNTS) {
            fprintf(stderr, "Maximum number of accounts reached!\n");
            exit(EXIT_FAILURE);
        }
        // Create a new account with INITIAL_BALANCE.
        idx = num_accounts;
        state[idx].address = address;
        state[idx].balance = INITIAL_BALANCE;
        hash_table_insert(address, idx);
        num_accounts++;
    }
    return idx;
}

// Process each transaction in the batch.
void apply_transactions(Transaction *batch, size_t batch_size) {
    for (size_t i = 0; i < batch_size; i++) {
        Transaction *tx = &batch[i];
        size_t sender_idx = get_account_index(tx->sender);
        size_t receiver_idx = get_account_index(tx->receiver);
        
        // Only process if sender has sufficient funds.
        if (state[sender_idx].balance >= tx->amount) {
            state[sender_idx].balance -= tx->amount;
            state[receiver_idx].balance += tx->amount;
        }
    }
}

// Save the current state to disk as state_(version).bin.
void save_state_to_disk(int version) {
    char filename[256];
    snprintf(filename, sizeof(filename), "%s%d.bin", STATE_FILE_PREFIX, version);
    FILE *f = fopen(filename, "wb");
    if (!f) {
        perror("Error opening state file for writing");
        exit(EXIT_FAILURE);
    }
    if (fwrite(state, sizeof(Account), num_accounts, f) != num_accounts) {
        perror("Error writing state file");
        fclose(f);
        exit(EXIT_FAILURE);
    }
    fclose(f);
    printf("State saved to %s (total accounts: %zu)\n", filename, num_accounts);
}

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
                // Rewind if end-of-file is reached.
                rewind(tx_file);
                read_count = fread(batch, sizeof(Transaction), BATCH_SIZE, tx_file);
            } else {
                perror("Error reading transactions");
                break;
            }
        }

        // Apply the transactions.
        apply_transactions(batch, read_count);

        // Compute the version (modulo MAX_VERSIONS).
        int version = iteration % MAX_VERSIONS;
        // Remove the old state file for this version if it exists.
        char old_filename[256];
        snprintf(old_filename, sizeof(old_filename), "%s%d.bin", STATE_FILE_PREFIX, version);
        if (access(old_filename, F_OK) == 0) {
            if (remove(old_filename) == 0) {
                printf("Removed old state file %s\n", old_filename);
            } else {
                perror("Error removing old state file");
            }
        }
        save_state_to_disk(version);

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