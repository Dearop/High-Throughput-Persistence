#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <time.h>

#define NUM_ACCOUNTS 300000    // Number of accounts to generate
#define STATE_FILE "state_0.bin"
#define INITIAL_BALANCE 1000000ULL  // Default initial balance for each account

// Structure representing an account in the state
typedef struct {
    uint64_t address;
    uint64_t balance;
} Account;

int main(void) {
    FILE *file = fopen(STATE_FILE, "wb");
    if (!file) {
        perror("Error opening state file for writing");
        exit(EXIT_FAILURE);
    }
    
    // Seed the random number generator
    srand(time(NULL));
    
    // Generate and write each account
    for (size_t i = 0; i < NUM_ACCOUNTS; i++) {
        Account acc;
        // Generate a random 64-bit address
        acc.address = ((uint64_t)rand() << 32) | rand();
        acc.balance = INITIAL_BALANCE;
        
        if (fwrite(&acc, sizeof(Account), 1, file) != 1) {
            perror("Error writing account data");
            fclose(file);
            exit(EXIT_FAILURE);
        }
    }
    
    fclose(file);
    printf("State file '%s' generated with %d accounts.\n", STATE_FILE, NUM_ACCOUNTS);
    return 0;
}