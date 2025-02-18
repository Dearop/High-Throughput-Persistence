#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <time.h>

#define BATCH_SIZE (1 << 16)  // 2^16 transactions
#define NUMBER_OF_BATCHES 10
#define OUTPUT_FILE "transactions.bin"

typedef struct {
    uint64_t sender;
    uint64_t receiver;
    uint32_t amount;
} Transaction;

void generate_batch(FILE *file, Transaction *batch) {
    for (size_t i = 0; i < BATCH_SIZE; i++) {
        batch[i].sender = ((uint64_t)rand() << 32) | rand();
        batch[i].receiver = ((uint64_t)rand() << 32) | rand();
        batch[i].amount = rand() % (1 << 16); 
    }

    size_t written = fwrite(batch, sizeof(Transaction), BATCH_SIZE, file);
    
    if (written != BATCH_SIZE) {
        perror("Error writing to file");
        fclose(file);
        exit(EXIT_FAILURE);
    }
}

void generate_transactions(const char *filename) {
    // clear past entries inside transactions.bin
    FILE *file = fopen(filename, "wb");
    if (!file) {
        perror("Error opening file");
        exit(EXIT_FAILURE);
    }
    // close after clearing
    fclose(file); 

    // Reopen file for appending
    file = fopen(filename, "ab");
    if (!file) {
        perror("Error opening file for appending");
        exit(EXIT_FAILURE);
    }

    srand(time(NULL));

    Transaction batch[BATCH_SIZE];
    for (size_t i = 0; i < NUMBER_OF_BATCHES; i++) {
        generate_batch(file, batch);
    }

    fclose(file);
    printf("Generated %d transactions and saved to %s\n", NUMBER_OF_BATCHES * BATCH_SIZE, filename);
}

int main() {
    generate_transactions(OUTPUT_FILE);
    return 0;
}