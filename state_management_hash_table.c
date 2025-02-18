#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <unistd.h>
#include <time.h>

#define BATCH_SIZE (1 << 16)       // 2^16 transactions per batch
#define TRANSACTION_FILE "transactions.bin"
#define STATE_FILE_PREFIX "state_"
#define MAX_VERSIONS 10            // Keep last 10 versions of state
#define INITIAL_BALANCE 1000000L   // (Not used here, since state is pre-populated)
#define MAX_ACCOUNTS 2000000L      // Maximum number of accounts we can track
#define HASH_TABLE_SIZE (1 << 21)  // Must be a power of 2; adjust based on expected # of accounts

// Structure representing a transaction
typedef struct {
    uint64_t sender;
    uint64_t receiver;
    uint32_t amount;
} Transaction;

// Structure representing an account in the state
typedef struct {
    uint64_t address;
    uint64_t balance;
} Account;

// Global in-memory state (pre-populated externally)
Account state[MAX_ACCOUNTS];
size_t num_accounts = 100000;  // Example: 100,000 pre-populated accounts

typedef struct {
    uint64_t key;
    size_t index;
    int used;
} HashEntry;

HashEntry hash_table[HASH_TABLE_SIZE];

// 64-bit hash function by Austin Appleby found on the internet
static inline uint64_t hash_uint64(uint64_t key) {
    key ^= key >> 33;
    key *= 0xff51afd7ed558ccdULL;
    key ^= key >> 33;
    key *= 0xc4ceb9fe1a85ec53ULL;
    key ^= key >> 33;
    return key;
}

// Insert a (key, index) pair into the hash table
void hash_table_insert(uint64_t key, size_t index) {
    uint64_t h = hash_uint64(key) & (HASH_TABLE_SIZE - 1);
    while (hash_table[h].used) {
        if (hash_table[h].key == key) { // Duplicate key, update index if needed
            hash_table[h].index = index;
            return;
        }
        h = (h + 1) & (HASH_TABLE_SIZE - 1);
    }
    hash_table[h].used = 1;
    hash_table[h].key = key;
    hash_table[h].index = index;
}

// Look up an account index by its address using the hash table
size_t find_account_index_ht(uint64_t key) {
    uint64_t h = hash_uint64(key) & (HASH_TABLE_SIZE - 1);
    while (hash_table[h].used) {
        if (hash_table[h].key == key) {
            return hash_table[h].index;
        }
        h = (h + 1) & (HASH_TABLE_SIZE - 1);
    }
    return (size_t)-1;  // Not found
}

// Process each transaction in the given batch using the hash table for lookups.
void apply_transactions(Transaction *batch, size_t batch_size) {
    for (size_t i = 0; i < batch_size; i++) {
        Transaction *tx = &batch[i];
        size_t sender_idx = find_account_index_ht(tx->sender);
        size_t receiver_idx = find_account_index_ht(tx->receiver);

        if (sender_idx == (size_t)-1 || receiver_idx == (size_t)-1) {
            fprintf(stderr, "Transaction skipped: account not found (sender: %llu, receiver: %llu)\n",
                    tx->sender, tx->receiver);
            continue;
        }

        // Only process if sender has sufficient funds
        if (state[sender_idx].balance >= tx->amount) {
            state[sender_idx].balance -= tx->amount;
            state[receiver_idx].balance += tx->amount;
        }
    }
}

// Save the current state to disk (without timing in this function)
void save_state_to_disk(int version) {
    char filename[256];
    snprintf(filename, sizeof(filename), "%s%d.bin", STATE_FILE_PREFIX, version);
    FILE *f = fopen(filename, "wb");
    if (!f) {
        perror("Error opening state file for writing");
        exit(EXIT_FAILURE);
    }
    if (fwrite(state, sizeof(Account), num_accounts, f) != num_accounts) {
        perror("Error writing state accounts");
        fclose(f);
        exit(EXIT_FAILURE);
    }
    fclose(f);
    printf("Processed and saved state to %s", filename);
}

// Rotate state versions (delete the oldest file when needed)
void rotate_state_versions(int current_version) {
    int oldest_version = current_version - MAX_VERSIONS;
    if (oldest_version >= 0) {
        char filename[256];
        snprintf(filename, sizeof(filename), "%s%d.bin", STATE_FILE_PREFIX, oldest_version);
        if (access(filename, F_OK) == 0) {
            remove(filename);
            printf("Old state file %s removed.\n", filename);
        }
    }
}

int main() {
    // Open the transactions file for reading.
    FILE *tx_file = fopen(TRANSACTION_FILE, "rb");
    if (!tx_file) {
        perror("Error opening transactions file");
        exit(EXIT_FAILURE);
    }

    int version_counter = 0;
    Transaction batch[BATCH_SIZE];
    size_t read_count;

    // Load the pre-populated state from disk.
    FILE *stateFile = fopen("state_0.bin", "rb");
    if (!stateFile) {
        perror("Error opening state file");
        exit(EXIT_FAILURE);
    }
    if (fread(state, sizeof(Account), num_accounts, stateFile) != num_accounts) {
        perror("Error reading state file");
        fclose(stateFile);
        exit(EXIT_FAILURE);
    }
    fclose(stateFile);
    printf("Loaded %zu accounts from state_0.bin\n", num_accounts);

    // Build the hash table from the loaded state for fast lookups.
    for (size_t i = 0; i < num_accounts; i++) {
        hash_table_insert(state[i].address, i);
    }

    // Main processing loop
    while (1) {
        printf("\nPress ENTER to process the next batch of transactions...\n");
        getchar();  // Wait for user input

        struct timespec start, end;
        clock_gettime(CLOCK_MONOTONIC, &start);  // Start timing

        // Read a batch of transactions.
        read_count = fread(batch, sizeof(Transaction), BATCH_SIZE, tx_file);
        if (read_count < BATCH_SIZE) {
            if (feof(tx_file)) {
                // Rewind to beginning if end of file is reached.
                rewind(tx_file);
                read_count = fread(batch, sizeof(Transaction), BATCH_SIZE, tx_file);
            } else {
                perror("Error reading transactions");
                break;
            }
        }

        // Apply transactions using the fast hash table lookups.
        apply_transactions(batch, read_count);

        // Save the updated state to disk.
        version_counter = (version_counter + 1) % 10;
        save_state_to_disk(version_counter);
        rotate_state_versions(version_counter);

        clock_gettime(CLOCK_MONOTONIC, &end);  // End timing
        double elapsed_time = (end.tv_sec - start.tv_sec) * 1000.0 +
                              (end.tv_nsec - start.tv_nsec) / 1000000.0;
        printf(" for %zu accounts in %.3f ms\n", num_accounts, elapsed_time);
    }

    fclose(tx_file);
    return 0;
}