#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <time.h>

#define BATCH_SIZE (1 << 16)      // 2^16 transactions per batch
#define NUMBER_OF_BATCHES 30      // Total number of batches to generate
#define TX_FILE "transactions.bin"
#define MAX_ACCOUNTS 2000000UL    // Maximum number of accounts (addresses 0 .. MAX_ACCOUNTS-1)

typedef struct {
    uint64_t sender;
    uint64_t receiver;
    uint32_t amount;
} Transaction;

int main(void) {
    FILE *txFile = fopen(TX_FILE, "wb");
    if (!txFile) {
        perror("Error opening transactions file for writing");
        exit(EXIT_FAILURE);
    }
    
    srand((unsigned)time(NULL));
    Transaction *batch = malloc(BATCH_SIZE * sizeof(Transaction));
    if (!batch) {
        perror("Error allocating memory for transaction batch");
        fclose(txFile);
        exit(EXIT_FAILURE);
    }
    
    // Generate transaction batches.
    for (size_t batch_num = 0; batch_num < NUMBER_OF_BATCHES; batch_num++) {
        for (size_t i = 0; i < BATCH_SIZE; i++) {
            uint64_t sender = rand() % MAX_ACCOUNTS;
            uint64_t receiver = rand() % MAX_ACCOUNTS;
            // Ensure sender and receiver are not the same.
            while (receiver == sender) {
                receiver = rand() % MAX_ACCOUNTS;
            }
            batch[i].sender   = sender;
            batch[i].receiver = receiver;
            batch[i].amount   = rand() % (1 << 16);  // Amount in range [0, 65535]
        }
        
        size_t written = fwrite(batch, sizeof(Transaction), BATCH_SIZE, txFile);
        if (written != BATCH_SIZE) {
            perror("Error writing transaction batch");
            free(batch);
            fclose(txFile);
            exit(EXIT_FAILURE);
        }
        printf("Batch %zu written.\n", batch_num + 1);
    }
    
    free(batch);
    fclose(txFile);
    printf("Generated %d transactions (in %d batches) and saved to '%s'.\n",
           NUMBER_OF_BATCHES * BATCH_SIZE, NUMBER_OF_BATCHES, TX_FILE);
    
    return 0;
}