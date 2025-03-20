#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <time.h>
#include <omp.h>

#define BATCH_SIZE (1 << 16)         // 2^16 transactions per batch
#define NUMBER_OF_BATCHES 50         // Total number of batches to generate
#define TX_FILE "transactions.bin"
#define SMALL_ACCOUNT_COUNT 2000000UL   
// Use a flag to mark an expensive transaction.
#define EXPENSIVE_FLAG (1ULL << 31)
// Set probability (in percent) for a transaction to be marked expensive.
#define EXPENSIVE_PROB 5
#define MAX_FUNC_NAME 32
#define MAX_PARAMS 8

// Define the P2P transaction structure.
typedef struct {
    uint64_t sender;
    uint64_t receiver;
    uint32_t amount;
} P2P_Transaction;

// Define the function transaction structure.
typedef struct {
    char function_name[MAX_FUNC_NAME];
    uint64_t params[MAX_PARAMS];
    uint32_t param_count;
} Func_Transaction;

// Define the type of transaction.
typedef enum {
    TX_P2P,
    TX_FUNC
} TransactionType;

// Combined transaction type using a union.
typedef struct {
    TransactionType type;
    union {
        P2P_Transaction p2p;
        Func_Transaction func;
    } data;
} Transaction;

int main(void) {
    // Allocate memory for account addresses.
    uint64_t *addresses = malloc(SMALL_ACCOUNT_COUNT * sizeof(uint64_t));
    if (!addresses) {
        perror("Error allocating addresses");
        exit(EXIT_FAILURE);
    }
    for (unsigned i = 0; i < SMALL_ACCOUNT_COUNT; i++) {
        addresses[i] = i + 1; // Unique 64-bit addresses.
    }

    // Open the transaction file for writing.
    FILE *txFile = fopen(TX_FILE, "wb");
    if (!txFile) {
        perror("Error opening transaction file for writing");
        free(addresses);
        exit(EXIT_FAILURE);
    }
    
    // Allocate memory for a batch of transactions.
    Transaction *batch = malloc(BATCH_SIZE * sizeof(Transaction));
    if (!batch) {
        perror("Error allocating transaction batch");
        free(addresses);
        fclose(txFile);
        exit(EXIT_FAILURE);
    }
    
    // Predefine a list of function names for function transactions.
    const char *function_names[] = {"memset"};
    size_t num_func_names = sizeof(function_names) / sizeof(function_names[0]);
    
    unsigned int base_seed = (unsigned int)time(NULL);
    
    // Generate batches using OpenMP with a unique seed per transaction.
    for (size_t batch_num = 0; batch_num < NUMBER_OF_BATCHES; batch_num++) {
        #pragma omp parallel for schedule(static)
        for (size_t i = 0; i < BATCH_SIZE; i++) {
            // Create a per-transaction seed.
            unsigned int seed = base_seed + (unsigned int)(batch_num * BATCH_SIZE + i);
            // Randomly choose transaction type (0 for P2P, 1 for function transaction).
            int tx_type_choice = rand_r(&seed) % 2;
            if (tx_type_choice == 0) {
                // Generate a P2P Transaction.
                batch[i].type = TX_P2P;
                // Select sender and receiver indices ensuring they are different.
                size_t idx_sender = rand_r(&seed) % SMALL_ACCOUNT_COUNT;
                size_t idx_receiver = rand_r(&seed) % SMALL_ACCOUNT_COUNT;
                while (idx_receiver == idx_sender) {
                    idx_receiver = rand_r(&seed) % SMALL_ACCOUNT_COUNT;
                }
                // With EXPENSIVE_PROB% chance, mark the sender as expensive.
                if (rand_r(&seed) % 100 < EXPENSIVE_PROB)
                    batch[i].data.p2p.sender = addresses[idx_sender] | EXPENSIVE_FLAG;
                else
                    batch[i].data.p2p.sender = addresses[idx_sender];
                batch[i].data.p2p.receiver = addresses[idx_receiver];
                batch[i].data.p2p.amount = rand_r(&seed) % (1 << 16);
            } else {
                batch[i].type = TX_FUNC;
                int name_idx = rand_r(&seed) % num_func_names;
                strncpy(batch[i].data.func.function_name, function_names[name_idx], MAX_FUNC_NAME);
                batch[i].data.func.function_name[MAX_FUNC_NAME - 1] = '\0';
                batch[i].data.func.param_count = 3;
                batch[i].data.func.params[0] = 5;
                batch[i].data.func.params[1] = 6;
                batch[i].data.func.params[2] = 1500;
            }
        }
        // Write the batch to the transaction file.
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