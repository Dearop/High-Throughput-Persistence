#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <time.h>
#include <omp.h>

// --- Definitions ---
#define BATCH_SIZE          (1ULL << 16)
#define NUMBER_OF_BATCHES   50000
#define TOTAL_TRANSACTIONS  (BATCH_SIZE * NUMBER_OF_BATCHES)
#define SMALL_ACCOUNT_COUNT  10000000UL       // Target number of accounts

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

void print_usage(const char *program_name) {
    printf("Usage: %s <memset_percentage>\n", program_name);
    printf("\n");
    printf("Arguments:\n");
    printf("  memset_percentage  Percentage of transactions that should be range-set (memset) operations\n");
    printf("                     Must be a number between 0.0 and 100.0\n");
    printf("                     Example: 5.0 means 5%% of transactions will be memset operations\n");
    printf("\n");
    printf("Examples:\n");
    printf("  %s 5.0     # Generate transactions with 5%% memset operations\n", program_name);
    printf("  %s 0.0     # Generate only P2P transfer transactions\n", program_name);
    printf("  %s 25.0    # Generate transactions with 25%% memset operations\n", program_name);
    printf("\n");
    printf("Output:\n");
    printf("  transactions.bin   Binary file containing %llu encoded transactions\n", (unsigned long long)TOTAL_TRANSACTIONS);
}

int main(int argc, char *argv[]) {
    // Parse command-line arguments
    if (argc != 2) {
        fprintf(stderr, "Error: Invalid number of arguments.\n\n");
        print_usage(argv[0]);
        return EXIT_FAILURE;
    }

    char *endptr;
    double memset_percentage = strtod(argv[1], &endptr);
    
    // Validate the memset percentage
    if (*endptr != '\0') {
        fprintf(stderr, "Error: Invalid memset percentage '%s'. Must be a valid number.\n\n", argv[1]);
        print_usage(argv[0]);
        return EXIT_FAILURE;
    }
    
    if (memset_percentage < 0.0 || memset_percentage > 100.0) {
        fprintf(stderr, "Error: Memset percentage %.2f is out of range. Must be between 0.0 and 100.0.\n\n", memset_percentage);
        print_usage(argv[0]);
        return EXIT_FAILURE;
    }

    // Convert percentage to probability (0.0 to 1.0)
    double memset_probability = memset_percentage / 100.0;

    FILE *fp = fopen("transactions.bin", "wb");
    if (!fp) {
        perror("fopen for transactions.bin failed");
        exit(EXIT_FAILURE);
    }

    unsigned int base_seed = (unsigned int)time(NULL);
    volatile int global_error_flag = 0; // Shared flag for critical errors

    printf("=== Transaction Generation Configuration ===\n");
    printf("Total transactions: %llu (%lu batches of %llu each)\n", 
           (unsigned long long)TOTAL_TRANSACTIONS, NUMBER_OF_BATCHES, (unsigned long long)BATCH_SIZE);
    printf("Account range: 0 to %lu\n", SMALL_ACCOUNT_COUNT - 1);
    printf("Memset percentage: %.2f%% (%.4f probability)\n", memset_percentage, memset_probability);
    printf("P2P transfer percentage: %.2f%%\n", 100.0 - memset_percentage);
    printf("Output file: transactions.bin\n");
    printf("\nStarting generation...\n");

    uint64_t total_memset_ops = 0;
    uint64_t total_transfer_ops = 0;

    #pragma omp parallel reduction(+:total_memset_ops,total_transfer_ops)
    {
        Transaction thread_tx_buffer[BATCH_SIZE];
        // Initialize thread-specific seed for rand_r
        unsigned int thread_seed = base_seed ^ (unsigned int)omp_get_thread_num(); 
        uint64_t thread_memset_count = 0;
        uint64_t thread_transfer_count = 0;

        #pragma omp for schedule(dynamic)
        for (uint64_t batch_idx = 0; batch_idx < NUMBER_OF_BATCHES; ++batch_idx) {
            if (global_error_flag) { // Check if another thread encountered a critical error
                continue; 
            }

            for (size_t tx_in_batch_idx = 0; tx_in_batch_idx < BATCH_SIZE; ++tx_in_batch_idx) {
                Transaction current_tx; // Temporary transaction for generation
                
                // Use rand_r for thread-safe random number generation
                double r_val = (double)rand_r(&thread_seed) / RAND_MAX;

                if (r_val < memset_probability) { // Range Set (memset operation)
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
                    thread_memset_count++;

                } else { // P2P Transfer
                    uint64_t sender_index = (uint64_t)rand_r(&thread_seed) % SMALL_ACCOUNT_COUNT;
                    uint64_t receiver_index = (uint64_t)rand_r(&thread_seed) % SMALL_ACCOUNT_COUNT;
                    current_tx.sender = ENCODE_OP(0, sender_index);
                    current_tx.receiver = ENCODE_OP(0, receiver_index);
                    current_tx.amount = ((uint64_t)rand_r(&thread_seed) % 1000) + 1;
                    thread_transfer_count++;
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
        
        total_memset_ops += thread_memset_count;
        total_transfer_ops += thread_transfer_count;
    } // End omp parallel region
    
    fclose(fp);

    if (global_error_flag) {
        fprintf(stderr, "An error occurred during transaction generation. Output file 'transactions.bin' may be incomplete or corrupted and has been removed.\n");
        remove("transactions.bin"); // Attempt to remove potentially corrupted file
        return EXIT_FAILURE;
    } else {
        printf("\n=== Generation Complete ===\n");
        printf("Successfully generated %llu transactions and saved to 'transactions.bin'\n", (unsigned long long)TOTAL_TRANSACTIONS);
        printf("\nOperation breakdown:\n");
        printf("  Memset operations:   %llu (%.2f%%)\n", 
               (unsigned long long)total_memset_ops, 
               (double)total_memset_ops * 100.0 / TOTAL_TRANSACTIONS);
        printf("  Transfer operations: %llu (%.2f%%)\n", 
               (unsigned long long)total_transfer_ops, 
               (double)total_transfer_ops * 100.0 / TOTAL_TRANSACTIONS);
        printf("  Total:               %llu (100.00%%)\n", (unsigned long long)TOTAL_TRANSACTIONS);
        
        // Verify the actual percentage matches the requested percentage
        double actual_memset_percentage = (double)total_memset_ops * 100.0 / TOTAL_TRANSACTIONS;
        double percentage_diff = actual_memset_percentage - memset_percentage;
        printf("\nAccuracy: Requested %.2f%%, actual %.2f%% (difference: %+.2f%%)\n", 
               memset_percentage, actual_memset_percentage, percentage_diff);
    }
    
    return 0;
}