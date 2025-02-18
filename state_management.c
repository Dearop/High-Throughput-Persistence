#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <unistd.h>

#define BATCH_SIZE (1 << 16)       // 2^16 transactions per batch
#define TRANSACTION_FILE "transactions.bin"
#define STATE_FILE_PREFIX "state_"
#define MAX_VERSIONS 10            // Keep last 10 versions of state
#define INITIAL_BALANCE 1000000L // Default initial balance for new accounts
#define MAX_ACCOUNTS 2000000L       // Maximum number of accounts we can track

// Transaction structure (each op)
typedef struct {
    uint64_t sender;
    uint64_t receiver;
    uint32_t amount;
} Transaction;

// Account structure for state S
typedef struct {
    uint64_t address;
    uint64_t balance;
} Account;

// Global in-memory state (array of accounts) and count of accounts
Account state[MAX_ACCOUNTS];
size_t num_accounts = 0;

/** Helper: Find an account by address.
   Returns the index if found, or (size_t)-1 if not found. 
   NOTE : Can be done with a hash table for better performance
**/
size_t find_account_index(uint64_t address) {
    for (size_t i = 0; i < num_accounts; i++) {
        if (state[i].address == address) {
            return i;
        }
    }
    return (size_t)-1;
}

/* Helper: Find an account by address or create it if not present.
   New accounts start with INITIAL_BALANCE. */
size_t find_or_create_account(uint64_t address) {
    size_t idx = find_account_index(address);
    if (idx != (size_t)-1)
        return idx;
    if (num_accounts >= MAX_ACCOUNTS) {
        fprintf(stderr, "Exceeded maximum accounts.\n");
        exit(EXIT_FAILURE);
    }
    state[num_accounts].address = address;
    state[num_accounts].balance = INITIAL_BALANCE;
    return num_accounts++;
}

/* The apply function:
   For each transaction in the batch, if the sender has enough balance,
   subtract the amount from the sender and add it to the receiver.
   (Optionally, you could track the set of modified addresses.) */
void apply_transactions(Transaction *batch, size_t batch_size) {
    for (size_t i = 0; i < batch_size; i++) {
        Transaction *tx = &batch[i];
        // Get or create sender and receiver accounts
        size_t sender_idx = find_or_create_account(tx->sender);
        size_t receiver_idx = find_or_create_account(tx->receiver);

        // Only perform the transfer if sender has enough funds
        if (state[sender_idx].balance >= tx->amount) {
            state[sender_idx].balance -= tx->amount;
            state[receiver_idx].balance += tx->amount;
            // Optionally, record these addresses as "written to"
        }
    }
}

/* Save the current state S to a disk file.
   The file is named using a version number, e.g. "state_0.bin". */
void save_state_to_disk(int version) {
    char filename[256];
    snprintf(filename, sizeof(filename), "%s%d.bin", STATE_FILE_PREFIX, version);
    
    FILE *f = fopen(filename, "wb");
    if (!f) {
        perror("Error opening state file for writing");
        exit(EXIT_FAILURE);
    }
    // First write the number of accounts, then the account array
    if (fwrite(&num_accounts, sizeof(num_accounts), 1, f) != 1) {
        perror("Error writing state (num_accounts)");
        exit(EXIT_FAILURE);
    }
    if (fwrite(state, sizeof(Account), num_accounts, f) != num_accounts) {
        perror("Error writing state accounts");
        exit(EXIT_FAILURE);
    }
    fclose(f);
    printf("State saved to %s (accounts: %zu)\n", filename, num_accounts);
}

/* Rotate state versions:
   When more than MAX_VERSIONS exist, delete the oldest version.
   Here we simply delete the file for version (current - MAX_VERSIONS) */
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
    // Open the transactions file for reading
    FILE *tx_file = fopen(TRANSACTION_FILE, "rb");
    if (!tx_file) {
        perror("Error opening transactions file");
        exit(EXIT_FAILURE);
    }

    int version_counter = 0;
    Transaction batch[BATCH_SIZE];
    size_t read_count;

    // Main processing loop: wait for user input and then process one batch
    while (1) {
        printf("\nPress ENTER to process the next batch of transactions...\n");
        getchar();  // Wait for user to press ENTER

        // Read a batch from the transactions file
        read_count = fread(batch, sizeof(Transaction), BATCH_SIZE, tx_file);
        if (read_count < BATCH_SIZE) {
            if (feof(tx_file)) {
                // Rewind to beginning if at EOF (for continuous processing)
                rewind(tx_file);
                read_count = fread(batch, sizeof(Transaction), BATCH_SIZE, tx_file);
            } else {
                perror("Error reading transactions");
                break;
            }
        }

        // Apply the transactions in the batch to our state S
        apply_transactions(batch, read_count);

        // Save the updated state to disk under the current version
        save_state_to_disk(version_counter);
        // Remove older version if more than MAX_VERSIONS exist
        rotate_state_versions(version_counter);

        version_counter++;
    }

    fclose(tx_file);
    return 0;
}