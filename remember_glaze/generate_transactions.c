#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <time.h>

// --- Definitions ---
#define BATCH_SIZE          (1ULL << 16)
#define NUMBER_OF_BATCHES   128
#define TOTAL_TRANSACTIONS  (BATCH_SIZE * NUMBER_OF_BATCHES)
#define SMALL_ACCOUNT_COUNT 2000000UL

// --- Operation Encoding ---
// Top 4 bits hold the op code, remaining 60 bits hold data.
#define FUNC_MASK   0xF000000000000000UL
#define DATA_MASK   0x0FFFFFFFFFFFFFFFUL
#define ENCODE_OP(op, data) (((uint64_t)(op) << 60) | ((data) & DATA_MASK))

typedef struct {
    uint64_t sender;
    uint64_t receiver;
    uint32_t amount;
} Transaction;

int main(void) {
    FILE *fp = fopen("transactions.bin", "wb");
    if (!fp) {
        perror("fopen");
        exit(EXIT_FAILURE);
    }

    srand((unsigned)time(NULL));
    Transaction tx;

    for (uint64_t i = 0; i < TOTAL_TRANSACTIONS; i++) {
        double r = (double)rand() / RAND_MAX;
        if (r < 0.05) {
            uint64_t start = rand() % SMALL_ACCOUNT_COUNT;
            uint64_t max_count = SMALL_ACCOUNT_COUNT - start;
            if (max_count > 100) {
                max_count = 100;
            }
            uint64_t count = (rand() % max_count) + 1;
            tx.sender = ENCODE_OP(1, start);
            tx.receiver = ENCODE_OP(1, count);
            // Use amount as the value to set.
            tx.amount = rand() % 1000;
        } else {
            // Generate a p2p transaction (op code 0).
            uint64_t sender_index = rand() % SMALL_ACCOUNT_COUNT;
            uint64_t receiver_index = rand() % SMALL_ACCOUNT_COUNT;
            tx.sender = ENCODE_OP(0, sender_index);
            tx.receiver = ENCODE_OP(0, receiver_index);
            tx.amount = (rand() % 1000) + 1;
        }
        fwrite(&tx, sizeof(Transaction), 1, fp);
    }
    
    fclose(fp);
    return 0;
}