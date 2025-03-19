#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <time.h>
#include <omp.h>

#define BATCH_SIZE (1 << 16)      // 2^16 transactions per batch
#define NUMBER_OF_BATCHES 5000      // Total number of batches to generate
#define TX_FILE "transactions.bin"
#define SMALL_ACCOUNT_COUNT 2000000UL
#define MASS_ZERO_PROB 0.01        // 1% chance to generate a mass-zero transaction
#define MASS_ZERO_LENGTH 10000     // Region length for mass zero

// Define transaction types.
typedef enum {
    TX_TRANSFER = 0,
    TX_MASS_ZERO = 1
} TxType;

// New transaction structure.
typedef struct {
    uint32_t op_type; // TX_TRANSFER or TX_MASS_ZERO
    // For TX_TRANSFER:
    uint64_t sender;
    uint64_t receiver;
    uint32_t amount;
    // For TX_MASS_ZERO:
    uint64_t start_index;
    uint64_t length;
} Transaction;

int main(void) {
    // Dynamically allocate a fixed set of addresses.
    uint64_t *addresses = malloc(SMALL_ACCOUNT_COUNT * sizeof(uint64_t));
    if (!addresses) {
        perror("Error allocating memory for addresses");
        exit(EXIT_FAILURE);
    }
    
    for (unsigned i = 0; i < SMALL_ACCOUNT_COUNT; i++) {
        addresses[i] = i + 1;  // Generate unique 64-bit values.
    }
    
    // Open the transaction file for writing.
    FILE *txFile = fopen(TX_FILE, "wb");
    if (!txFile) {
        perror("Error opening transactions file for writing");
        free(addresses);
        exit(EXIT_FAILURE);
    }
    
    srand((unsigned)time(NULL));
    Transaction *batch = malloc(BATCH_SIZE * sizeof(Transaction));
    if (!batch) {
        perror("Error allocating memory for transaction batch");
        free(addresses);
        fclose(txFile);
        exit(EXIT_FAILURE);
    }
    
    // Generate transaction batches with OpenMP.
    for (size_t batch_num = 0; batch_num < NUMBER_OF_BATCHES; batch_num++) {
        #pragma omp parallel
        {
            unsigned int seed = time(NULL) ^ omp_get_thread_num(); // Unique seed per thread
            
            #pragma omp for
            for (size_t i = 0; i < BATCH_SIZE; i++) {
                double p = (double)rand_r(&seed) / RAND_MAX;
                if (p < MASS_ZERO_PROB) {
                    // Generate a mass-zero transaction.
                    batch[i].op_type = TX_MASS_ZERO;
                    batch[i].start_index = rand_r(&seed) % SMALL_ACCOUNT_COUNT;
                    // Ensure the region does not extend past the state.
                    uint64_t max_len = SMALL_ACCOUNT_COUNT - batch[i].start_index;
                    batch[i].length = (MASS_ZERO_LENGTH < max_len) ? MASS_ZERO_LENGTH : max_len;
                    // For mass-zero, the transfer fields are not used.
                    batch[i].sender = 0;
                    batch[i].receiver = 0;
                    batch[i].amount = 0;
                } else {
                    // Generate a transfer transaction.
                    batch[i].op_type = TX_TRANSFER;
                    size_t idx_sender = rand_r(&seed) % SMALL_ACCOUNT_COUNT;
                    size_t idx_receiver = rand_r(&seed) % SMALL_ACCOUNT_COUNT;
                    while (idx_receiver == idx_sender) {
                        idx_receiver = rand_r(&seed) % SMALL_ACCOUNT_COUNT;
                    }
                    batch[i].sender   = addresses[idx_sender];
                    batch[i].receiver = addresses[idx_receiver];
                    batch[i].amount   = rand_r(&seed) % (1 << 16);
                    // For transfers, the mass-zero fields are set to 0.
                    batch[i].start_index = 0;
                    batch[i].length = 0;
                }
            }
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