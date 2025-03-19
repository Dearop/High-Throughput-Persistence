#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <time.h>
#include <omp.h>    

#define BATCH_SIZE (1 << 16)         // 2^16 transactions per batch
#define NUMBER_OF_BATCHES 5000         // Total number of batches to generate
#define TX_FILE "transactions.bin"
#define SMALL_ACCOUNT_COUNT 2000000UL   
// Use the high bit (bit 63) to mark an expensive transaction.
#define EXPENSIVE_FLAG (1ULL << 32)
// Set probability (in percent) for a transaction to be marked expensive.
#define EXPENSIVE_PROB 5

// Transaction structure remains small.
typedef struct {
    uint64_t sender;
    uint64_t receiver;
    uint32_t amount;
} Transaction;

int main(void) {
    uint64_t *addresses = malloc(SMALL_ACCOUNT_COUNT * sizeof(uint64_t));
    if (!addresses) {
        perror("Error allocating addresses");
        exit(EXIT_FAILURE);
    }
    for (unsigned i = 0; i < SMALL_ACCOUNT_COUNT; i++) {
        addresses[i] = i + 1; // Unique 64-bit addresses.
    }
    
    FILE *txFile = fopen(TX_FILE, "wb");
    if (!txFile) {
        perror("Error opening transaction file for writing");
        free(addresses);
        exit(EXIT_FAILURE);
    }
    
    Transaction *batch = malloc(BATCH_SIZE * sizeof(Transaction));
    if (!batch) {
        perror("Error allocating transaction batch");
        free(addresses);
        fclose(txFile);
        exit(EXIT_FAILURE);
    }
    
    unsigned int base_seed = (unsigned int)time(NULL);
    
    // Generate batches using OpenMP with a unique seed per transaction.
    for (size_t batch_num = 0; batch_num < NUMBER_OF_BATCHES; batch_num++) {
        #pragma omp parallel for schedule(static)
        for (size_t i = 0; i < BATCH_SIZE; i++) {
            // Create a per-transaction seed.
            unsigned int seed = base_seed + (unsigned int)(batch_num * BATCH_SIZE + i);
            size_t idx_sender = rand_r(&seed) % SMALL_ACCOUNT_COUNT;
            size_t idx_receiver = rand_r(&seed) % SMALL_ACCOUNT_COUNT;
            while (idx_receiver == idx_sender)
                idx_receiver = rand_r(&seed) % SMALL_ACCOUNT_COUNT;
            // With EXPENSIVE_PROB% chance, mark the transaction as expensive.
            if (rand_r(&seed) % 100 < EXPENSIVE_PROB)
                batch[i].sender = addresses[idx_sender] | EXPENSIVE_FLAG;
            else
                batch[i].sender = addresses[idx_sender];
            batch[i].receiver = addresses[idx_receiver];
            batch[i].amount = rand_r(&seed) % (1 << 16);
        }
        
        size_t written = fwrite(batch, sizeof(Transaction), BATCH_SIZE, txFile);
        if (written != BATCH_SIZE) {
            perror("Error writing transaction batch");
            free(batch);
            free(addresses);
            fclose(txFile);
            exit(EXIT_FAILURE);
        }
        printf("Batch %zu written.\n", batch_num + 1);
    }
    
    free(batch);
    free(addresses);
    fclose(txFile);
    printf("Generated %lu transactions (in %d batches) and saved to '%s'.\n",
           NUMBER_OF_BATCHES * BATCH_SIZE, NUMBER_OF_BATCHES, TX_FILE);
    
    return 0;
}