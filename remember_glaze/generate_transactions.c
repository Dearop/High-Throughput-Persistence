#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <time.h>
#include <omp.h>

// --- Definitions ---
#define BATCH_SIZE          (1ULL << 16)
#define NUMBER_OF_BATCHES   125000UL
#define TOTAL_TRANSACTIONS  (BATCH_SIZE * NUMBER_OF_BATCHES)
#define SMALL_ACCOUNT_COUNT  500000000UL       // Target number of accounts

// --- Operation Encoding ---
// Top 4 bits hold the op code, remaining 60 bits hold data.
#define FUNC_MASK   0xF000000000000000UL
#define DATA_MASK   0x0FFFFFFFFFFFFFFFUL
#define ENCODE_OP(op, data) (((uint64_t)(op) << 60) | ((data) & DATA_MASK))

typedef struct {
    uint64_t sender;
    uint64_t receiver;
    uint64_t amount;
} Transaction;

int main(void) {
    FILE *fp = fopen("transactions.bin", "wb");
    if (!fp) {
        perror("fopen for transactions.bin failed");
        exit(EXIT_FAILURE);
    }

    unsigned int base_seed = (unsigned int)time(NULL);
    volatile int global_error_flag = 0; // Shared flag for critical errors

    printf("--- Generating %llu Transactions (%lu Batches) ---\n", (unsigned long long)TOTAL_TRANSACTIONS, NUMBER_OF_BATCHES);

    #pragma omp parallel
    {
        Transaction thread_tx_buffer[BATCH_SIZE];
        // Initialize thread-specific seed for rand_r
        unsigned int thread_seed = base_seed ^ (unsigned int)omp_get_thread_num(); 

        #pragma omp for schedule(dynamic)
        for (uint64_t batch_idx = 0; batch_idx < NUMBER_OF_BATCHES; ++batch_idx) {
            if (global_error_flag) { // Check if another thread encountered a critical error
                continue; 
            }

            for (size_t tx_in_batch_idx = 0; tx_in_batch_idx < BATCH_SIZE; ++tx_in_batch_idx) {
                Transaction current_tx; // Temporary transaction for generation
                
                // Use rand_r for thread-safe random number generation
                double r_val = (double)rand_r(&thread_seed) / RAND_MAX;

                if (r_val < 0.05) { // Range Set
                    uint64_t start = (uint64_t)rand_r(&thread_seed) % SMALL_ACCOUNT_COUNT;
                    uint64_t max_len = SMALL_ACCOUNT_COUNT - start;
                    if (max_len == 0) max_len = 1;
                    if (max_len > 100) {
                        max_len = 100;
                    }
                    uint64_t count = ((uint64_t)rand_r(&thread_seed) % max_len) + 1;
                    current_tx.sender = ENCODE_OP(1, start);
                    current_tx.receiver = ENCODE_OP(1, count);
                    current_tx.amount = (uint64_t)rand_r(&thread_seed) % 1000;

                } else { // P2P Transfer
                    uint64_t sender_index = (uint64_t)rand_r(&thread_seed) % SMALL_ACCOUNT_COUNT;
                    uint64_t receiver_index = (uint64_t)rand_r(&thread_seed) % SMALL_ACCOUNT_COUNT;
                    current_tx.sender = ENCODE_OP(0, sender_index);
                    current_tx.receiver = ENCODE_OP(0, receiver_index);
                    current_tx.amount = ((uint64_t)rand_r(&thread_seed) % 1000) + 1;
                }
                thread_tx_buffer[tx_in_batch_idx] = current_tx;
            } // End loop for generating transactions within a batch

            // Critical section for writing the batch to file
            #pragma omp critical(file_write_transactions)
            {
                if (!global_error_flag) { // Only write if no error has been flagged
                    if (fwrite(thread_tx_buffer, sizeof(Transaction), BATCH_SIZE, fp) != BATCH_SIZE) {
                        perror("fwrite failed to write a full batch");
                        global_error_flag = 1; // Set flag to stop other threads and indicate error
                    }
                }
            } // End critical section
        } // End omp for loop over batches
    } // End omp parallel region
    
    fclose(fp);

    if (global_error_flag) {
        fprintf(stderr, "An error occurred during transaction generation. Output file 'transactions.bin' may be incomplete or corrupted and has been removed.\n");
        remove("transactions.bin"); // Attempt to remove potentially corrupted file
        return EXIT_FAILURE;
    } else {
        printf("--- Finished Generating Transactions ---
");
        printf("Successfully generated %llu transactions (%lu batches) and saved to 'transactions.bin'.
",
               (unsigned long long)TOTAL_TRANSACTIONS, NUMBER_OF_BATCHES);
    }
    
    return 0;
}