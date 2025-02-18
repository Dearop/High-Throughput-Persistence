#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <time.h>

#define BATCH_SIZE (1 << 16)      // 2^16 transactions per batch
#define NUMBER_OF_BATCHES 10      // Total number of batches to generate
#define TX_FILE "transactions.bin"
#define STATE_FILE "state.bin"

// Transaction structure (each operation)
typedef struct {
    uint64_t sender;
    uint64_t receiver;
    uint32_t amount;
} Transaction;

// Account structure (matches the one in generate_state.c)
typedef struct {
    uint64_t address;
    uint64_t balance;
} Account;

int main(void) {
    // Open the state file and read the accounts
    FILE *stateFile = fopen(STATE_FILE, "rb");
    if (!stateFile) {
        perror("Error opening state file");
        exit(EXIT_FAILURE);
    }
    
    size_t num_accounts;
    if (fread(&num_accounts, sizeof(num_accounts), 1, stateFile) != 1) {
        perror("Error reading number of accounts");
        fclose(stateFile);
        exit(EXIT_FAILURE);
    }
    
    Account *accounts = malloc(num_accounts * sizeof(Account));
    if (!accounts) {
        perror("Error allocating memory for accounts");
        fclose(stateFile);
        exit(EXIT_FAILURE);
    }
    
    if (fread(accounts, sizeof(Account), num_accounts, stateFile) != num_accounts) {
        perror("Error reading accounts from state file");
        free(accounts);
        fclose(stateFile);
        exit(EXIT_FAILURE);
    }
    fclose(stateFile);
    
    // Open the transaction file for writing (this will clear previous data)
    FILE *txFile = fopen(TX_FILE, "wb");
    if (!txFile) {
        perror("Error opening transactions file for writing");
        free(accounts);
        exit(EXIT_FAILURE);
    }
    
    srand(time(NULL));
    Transaction *batch = malloc(BATCH_SIZE * sizeof(Transaction));
    if (!batch) {
        perror("Error allocating memory for transaction batch");
        free(accounts);
        fclose(txFile);
        exit(EXIT_FAILURE);
    }
    
    // Generate transactions in batches
    for (size_t batch_num = 0; batch_num < NUMBER_OF_BATCHES; batch_num++) {
        for (size_t i = 0; i < BATCH_SIZE; i++) {
            size_t idx_sender = rand() % num_accounts;
            size_t idx_receiver = rand() % num_accounts;
            // Ensure sender and receiver are not the same
            while (idx_receiver == idx_sender) {
                idx_receiver = rand() % num_accounts;
            }
            batch[i].sender   = accounts[idx_sender].address;
            batch[i].receiver = accounts[idx_receiver].address;
            batch[i].amount   = rand() % (1 << 16);  // Amount in range [0, 65535]
        }
        
        size_t written = fwrite(batch, sizeof(Transaction), BATCH_SIZE, txFile);
        if (written != BATCH_SIZE) {
            perror("Error writing transaction batch");
            free(batch);
            free(accounts);
            fclose(txFile);
            exit(EXIT_FAILURE);
        }
        printf("Batch %zu written.\n", batch_num + 1);
    }
    
    free(batch);
    free(accounts);
    fclose(txFile);
    printf("Generated %d transactions (in %d batches) and saved to '%s'.\n", NUMBER_OF_BATCHES * BATCH_SIZE, NUMBER_OF_BATCHES, TX_FILE);
    return 0;
}