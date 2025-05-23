#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <time.h>

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
        perror("fopen");
        exit(EXIT_FAILURE);
    }

    srand((unsigned)time(NULL));
    Transaction tx;
    Transaction tx_buffer[BATCH_SIZE];
    size_t buffer_count = 0;

    //printf("--- Generating Transactions ---\n");
    for (uint64_t i = 0; i < TOTAL_TRANSACTIONS; i++) {

        double r = (double)rand() / RAND_MAX;
        if (r < 0.05) { // Range Set
            uint64_t start = rand() % SMALL_ACCOUNT_COUNT;
            uint64_t max_len = SMALL_ACCOUNT_COUNT - start; // Corrected max_count to max_len for clarity
            if (max_len == 0) max_len = 1; // Ensure count is at least 1 if start is the last account
            if (max_len > 100) { // Keep original constraint if intended
                max_len = 100;
            }
            uint64_t count = (rand() % max_len) + 1;
            tx.sender = ENCODE_OP(1, start);
            tx.receiver = ENCODE_OP(1, count);
            tx.amount = rand() % 1000; // Value to set

        } else { // P2P Transfer
            uint64_t sender_index = rand() % SMALL_ACCOUNT_COUNT;
            uint64_t receiver_index = rand() % SMALL_ACCOUNT_COUNT;
            // Ensure sender and receiver are different for a meaningful transfer, though not strictly required by problem
            // while (receiver_index == sender_index) { receiver_index = rand() % SMALL_ACCOUNT_COUNT; }
            tx.sender = ENCODE_OP(0, sender_index);
            tx.receiver = ENCODE_OP(0, receiver_index);
            tx.amount = (rand() % 1000) + 1;
        }
        
        //printf("Generated TX %lu: Type: %s, Sender/Start: %lu, Receiver/Count: %lu, Amount/Value: %lu (Raw Sender: 0x%016lx, Raw Receiver: 0x%016lx)\n",
        //       i,
        //       op_type_flag == 0 ? "P2P" : "RangeSet",
        //       display_sender,
        //       display_receiver_or_count,
        //       display_amount,
        //       tx.sender,
        //       tx.receiver);

        tx_buffer[buffer_count++] = tx;

        if (buffer_count == BATCH_SIZE) {
            fwrite(tx_buffer, sizeof(Transaction), BATCH_SIZE, fp);
            buffer_count = 0;
        }
    }
    //printf("--- Finished Generating Transactions ---\n");
    
    fclose(fp);
    return 0;
}