#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <time.h>
#include <omp.h>  // Include OpenMP header

#define BATCH_SIZE (1 << 16)      // 2^16 transactions per batch
#define NUMBER_OF_BATCHES 125000UL     // Total number of batches to generate
#define TX_FILE "transactions.bin"
#define SMALL_ACCOUNT_COUNT 500000000UL   

// Transaction structure.
typedef struct {
    uint64_t sender;
    uint64_t receiver;
    uint32_t amount;
} Transaction;

// Thread-local random number generator function
uint32_t fast_rand(uint32_t* seed) {
    *seed = (*seed * 1103515245 + 12345) & 0x7fffffff;
    return *seed;
}

int main(void) {
    // Dynamically allocate a fixed set of addresses.
    uint64_t *addresses = malloc(SMALL_ACCOUNT_COUNT * sizeof(uint64_t));
    if (!addresses) {
        perror("Error allocating memory for addresses");
        exit(EXIT_FAILURE);
    }
    
    for (unsigned i = 0; i < SMALL_ACCOUNT_COUNT; i++) {
        addresses[i] = i + 1;  // or any other scheme to generate unique 64-bit values
    }
    
    // Open the transaction file for writing.
    FILE *txFile = fopen(TX_FILE, "wb");
    if (!txFile) {
        perror("Error opening transactions file for writing");
        free(addresses);
        exit(EXIT_FAILURE);
    }

    // Get system time for random seed base
    uint32_t base_seed = (uint32_t)time(NULL);
    
    // Parallel generation of transaction batches
    #pragma omp parallel
    {
        // Create thread-local batch memory and seed
        Transaction *batch = malloc(BATCH_SIZE * sizeof(Transaction));
        uint32_t thread_seed = base_seed ^ omp_get_thread_num();
        
        if (!batch) {
            perror("Error allocating memory for transaction batch");
            exit(EXIT_FAILURE); // Not ideal in parallel context but simple
        }

        #pragma omp for schedule(dynamic)
        for (size_t batch_num = 0; batch_num < NUMBER_OF_BATCHES; batch_num++) {
            // Generate transactions for this batch
            for (size_t i = 0; i < BATCH_SIZE; i++) {
                size_t idx_sender = fast_rand(&thread_seed) % SMALL_ACCOUNT_COUNT;
                size_t idx_receiver = fast_rand(&thread_seed) % SMALL_ACCOUNT_COUNT;
                // Ensure sender and receiver are not the same.
                while (idx_receiver == idx_sender) {
                    idx_receiver = fast_rand(&thread_seed) % SMALL_ACCOUNT_COUNT;
                }
                batch[i].sender   = addresses[idx_sender];
                batch[i].receiver = addresses[idx_receiver];
                batch[i].amount   = fast_rand(&thread_seed) % (1 << 16);  // Amount in range [0, 65535]
            }
            
            // Critical section for file writing
            #pragma omp critical
            {
                size_t written = fwrite(batch, sizeof(Transaction), BATCH_SIZE, txFile);
                if (written != BATCH_SIZE) {
                    perror("Error writing transaction batch");
                    // Handle error (simplified)
                }
                //printf("Batch %zu written.\n", batch_num + 1);
            }
        }
        
        // Free thread-local memory
        free(batch);
    }
    
    free(addresses);
    fclose(txFile);
    printf("Generated %lu transactions (in %lu batches) and saved to '%s'.\n",
           NUMBER_OF_BATCHES * BATCH_SIZE, NUMBER_OF_BATCHES, TX_FILE);
    
    return 0;
}