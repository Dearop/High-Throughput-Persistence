#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <unistd.h>

#define BATCH_SIZE (1 << 16)       // 2^16 transactions per batch
#define TRANSACTION_FILE "transactions.bin"
#define STATE_FILE_PREFIX "state_"
#define MAX_VERSIONS 10            // Keep last 10 versions of state
#define INITIAL_BALANCE 1000000L   // (Not used here, since state is pre-populated)
#define MAX_ACCOUNTS 2000000L      // Maximum number of accounts we can track

// Structure representing a transaction (each operation)
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

// Global in-memory state (an array of accounts)
// It is assumed that the state is already populated externally.
// For example, a state generation program might create the first num_accounts entries.
Account state[MAX_ACCOUNTS];
size_t num_accounts = 100000;  // Pre-populated number of accounts

/** 
 * Searches for an account by its address.
 * Returns the index if found, or (size_t)-1 if not found.
 */
size_t find_account_index(uint64_t address) {
    for (size_t i = 0; i < num_accounts; i++) {
        if (state[i].address == address) {
            return i;
        }
    }
    return (size_t)-1;
}

/**
 * Processes each transaction in the given batch.
 * For each transaction, if both the sender and receiver exist in the state and 
 * if the sender has enough funds, the amount is transferred from the sender to the receiver.
 * If an account is not found, the transaction is skipped.
 */
void apply_transactions(Transaction *batch, size_t batch_size) {
    for (size_t i = 0; i < batch_size; i++) {
        Transaction *tx = &batch[i];
        
        // Look up sender and receiver accounts.
        size_t sender_idx = find_account_index(tx->sender);
        size_t receiver_idx = find_account_index(tx->receiver);
        
        // If either account does not exist, skip the transaction.
        if (sender_idx == (size_t)-1 || receiver_idx == (size_t)-1) {
            fprintf(stderr, "Transaction skipped: account not found (sender: %llu, receiver: %llu)\n",
                    tx->sender, tx->receiver);
            continue;
        }
        
        // Only apply the transfer if the sender has sufficient balance.
        if (state[sender_idx].balance >= tx->amount) {
            state[sender_idx].balance -= tx->amount;
            state[receiver_idx].balance += tx->amount;
        }
    }
}

/**
 * Saves the current state to disk in a file named like "state_X.bin"
 * where X is the provided version number.
 */
void save_state_to_disk(int version) {
    char filename[256];
    snprintf(filename, sizeof(filename), "%s%d.bin", STATE_FILE_PREFIX, version);
    FILE *f = fopen(filename, "wb");
    if (!f) {
        perror("Error opening state file for writing");
        exit(EXIT_FAILURE);
    }
    
    // Write the entire state array (only the pre-populated num_accounts entries)
    if (fwrite(state, sizeof(Account), num_accounts, f) != num_accounts) {
        perror("Error writing state accounts");
        fclose(f);
        exit(EXIT_FAILURE);
    }
    
    fclose(f);
    printf("State saved to %s (accounts: %zu)\n", filename, num_accounts);
}

/**
 * Rotates state versions: if the current version exceeds MAX_VERSIONS,
 * deletes the file for the oldest version.
 */
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
    
    // Main processing loop: wait for user input and process one batch each time.
    while (1) {
        printf("\nPress ENTER to process the next batch of transactions...\n");
        getchar();  // Wait for the user to press ENTER.
        
        // Read a batch of transactions.
        read_count = fread(batch, sizeof(Transaction), BATCH_SIZE, tx_file);
        if (read_count < BATCH_SIZE) {
            if (feof(tx_file)) {
                // If we've reached the end of the file, rewind and read again.
                rewind(tx_file);
                read_count = fread(batch, sizeof(Transaction), BATCH_SIZE, tx_file);
            } else {
                perror("Error reading transactions");
                break;
            }
        }
        
        // Apply the transactions to the state.
        apply_transactions(batch, read_count);
        
        // Save the updated state to disk with the current version number.
        save_state_to_disk(version_counter = (version_counter + 1) % 10);
        // Rotate the state files: delete the oldest version if needed.
        rotate_state_versions(version_counter);
    }
    
    fclose(tx_file);
    return 0;
}