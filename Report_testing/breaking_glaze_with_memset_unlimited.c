#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>

#define BATCH_SIZE          (1 << 16)
#define NUMBER_OF_BATCHES   5000
#define SMALL_ACCOUNT_COUNT 5000000UL

// Adjusted State Chunking and Ring Size
#define STATE_CHUNK_SIZE  (512 * 1024) // Target state chunk size of 512 KB
#define STATE_CHUNK_COUNT (STATE_CHUNK_SIZE / sizeof(int64_t))
#define RING_SIZE ((SMALL_ACCOUNT_COUNT + STATE_CHUNK_COUNT - 1) / STATE_CHUNK_COUNT) // Dynamically calculated RING_SIZE
#define TOTAL_STATE_COVERAGE_COUNT (RING_SIZE * STATE_CHUNK_COUNT) // Actual elements in state array

#define MAX_WRITE_SET_COUNT (1000 * BATCH_SIZE)
#define WRITE_SET_CHUNK_SIZE (MAX_WRITE_SET_COUNT * sizeof(WriteSetEntry))

#define CHECKPOINT_MAGIC 0xC0CAC01A

// --- Operation Encoding ---
// Top 4 bits hold the op code, remaining 60 bits hold data.
#define FUNC_MASK   0xF000000000000000UL
#define DATA_MASK   0x0FFFFFFFFFFFFFFFUL
#define GET_OP(encoded_val) (((uint64_t)(encoded_val) & FUNC_MASK) >> 60)
#define GET_DATA(encoded_val) ((uint64_t)(encoded_val) & DATA_MASK)

typedef struct {
    uint32_t magic;
    uint32_t batch_num;
    uint32_t chunk_offset;
    uint32_t state_chunk_count;
    uint32_t write_set_count;
} CheckpointHeader;
#define CHECKPOINT_HEADER_SIZE (sizeof(CheckpointHeader))
#define CHECKPOINT_SLOT_SIZE   (CHECKPOINT_HEADER_SIZE + STATE_CHUNK_SIZE + WRITE_SET_CHUNK_SIZE)

#define TX_FILE "transactions.bin"
#define LOG_FILE "checkpoint_log.dat"

typedef struct {
    uint64_t sender;
    uint64_t receiver;
    uint32_t amount;
} Transaction;

typedef struct {
    uint64_t address;
    int64_t balance;
} WriteSetEntry;

uint64_t fnv1a_hash(int64_t *data, size_t len) {
    uint64_t hash = 14695981039346656037UL;
    for (size_t i = 0; i < len; i++) {
        uint64_t val = (uint64_t)data[i];
        for (int j = 0; j < 8; j++) {
            uint8_t byte = (val >> (j * 8)) & 0xFF;
            hash ^= byte;
            hash *= 1099511628211UL;
        }
    }
    return hash;
}

double get_time_ms() {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec * 1000.0 + ts.tv_nsec / 1e6;
}

void preallocate_log_file(const char *filename) {
    int fd = open(filename, O_RDWR | O_CREAT, 0666);
    if (fd < 0)    
        exit(EXIT_FAILURE);
    struct stat st;
    if (fstat(fd, &st) != 0) { 
        close(fd); 
        exit(EXIT_FAILURE); 
    }
    off_t expected = RING_SIZE * CHECKPOINT_SLOT_SIZE;
    if (st.st_size < expected) {
        if (ftruncate(fd, expected) != 0) {
            close(fd); 
            exit(EXIT_FAILURE); 
        }
        fsync(fd);
    }
    close(fd);
}

void write_state_to_file(const char *filename, int64_t *state, size_t count) {
    FILE *fp = fopen(filename, "w");
    if (!fp) return;
    for (size_t i = 0; i < count; i++)
        fprintf(fp, "%zu: %lld\n", i, (long long)state[i]);
    fclose(fp);
}

int reconstruct_state(int fd, int64_t *state, int *last_batch) {
    for (uint32_t slot = 0; slot < RING_SIZE; slot++) {
        off_t offset = slot * CHECKPOINT_SLOT_SIZE;
        CheckpointHeader header;
        if (pread(fd, &header, sizeof(header), offset) != sizeof(header))
            continue;
        if (header.magic != CHECKPOINT_MAGIC || header.state_chunk_count != STATE_CHUNK_COUNT)
            continue;
        int64_t *chunk = malloc(STATE_CHUNK_SIZE);
        if (!chunk) 
            exit(EXIT_FAILURE);
        if (pread(fd, chunk, STATE_CHUNK_SIZE, offset + CHECKPOINT_HEADER_SIZE) != STATE_CHUNK_SIZE) {
            free(chunk);
            continue;
        }
        memcpy(state + header.chunk_offset, chunk, STATE_CHUNK_SIZE);
        free(chunk);
        *last_batch = header.batch_num;
    }
    return 0;
}

void apply(const Transaction *tx, int64_t *state,
           WriteSetEntry **ws_accum, int *ws_count) {
    uint64_t op_type = GET_OP(tx->sender); // Op is in sender for P2P, and start_addr for memset

    if (op_type == 0) { // P2P Transfer
        uint64_t sender_addr = GET_DATA(tx->sender);
        uint64_t receiver_addr = GET_DATA(tx->receiver);
        uint32_t amount = tx->amount;

        if (sender_addr < SMALL_ACCOUNT_COUNT) {
            state[sender_addr] -= amount;
            uint32_t chunk = sender_addr / STATE_CHUNK_COUNT;
            // Use full MAX_WRITE_SET_COUNT for the chunk's accumulator capacity
            if (ws_count[chunk] < MAX_WRITE_SET_COUNT) {
                ws_accum[chunk][ws_count[chunk]].address = sender_addr;
                ws_accum[chunk][ws_count[chunk]].balance = state[sender_addr];
                ws_count[chunk]++;
            }
        }
        if (receiver_addr < SMALL_ACCOUNT_COUNT) {
            state[receiver_addr] += amount;
            uint32_t chunk = receiver_addr / STATE_CHUNK_COUNT;
            // Use full MAX_WRITE_SET_COUNT for the chunk's accumulator capacity
            if (ws_count[chunk] < MAX_WRITE_SET_COUNT) {
                ws_accum[chunk][ws_count[chunk]].address = receiver_addr;
                ws_accum[chunk][ws_count[chunk]].balance = state[receiver_addr];
                ws_count[chunk]++;
            }
        }
    } else if (op_type == 1) { // Memset Operation
        uint64_t start_addr = GET_DATA(tx->sender); // start_addr is in sender field
        uint64_t count = GET_DATA(tx->receiver);    // count is in receiver field
        int64_t value = (int64_t)tx->amount;        // value to set is in amount field

        for (uint64_t i = 0; i < count; ++i) {
            uint64_t current_addr = start_addr + i;
            if (current_addr < SMALL_ACCOUNT_COUNT) {
                state[current_addr] = value;
                uint32_t chunk = current_addr / STATE_CHUNK_COUNT;
                // Use full MAX_WRITE_SET_COUNT for the chunk's accumulator capacity
                if (ws_count[chunk] < MAX_WRITE_SET_COUNT) {
                    ws_accum[chunk][ws_count[chunk]].address = current_addr;
                    ws_accum[chunk][ws_count[chunk]].balance = state[current_addr];
                    ws_count[chunk]++;
                }
                // No 'else' block needed here, as we're not skipping entries if within MAX_WRITE_SET_COUNT
                // (unless MAX_WRITE_SET_COUNT itself is exhausted for this chunk, which would be a buffer overflow)
            }
        }
    }
}

void write_checkpoint_slot(int fd, int slot, int batch_num, int64_t *state,
                             WriteSetEntry *ws_data, int ws_count) {
    static char *snapshot = NULL;
    if (!snapshot) {
        snapshot = malloc(STATE_CHUNK_SIZE);
        if (!snapshot) 
            exit(EXIT_FAILURE);
    }
    memcpy(snapshot, state + slot * STATE_CHUNK_COUNT, STATE_CHUNK_SIZE);
    CheckpointHeader header = { CHECKPOINT_MAGIC, batch_num, slot * STATE_CHUNK_COUNT,
                                  STATE_CHUNK_COUNT, ws_count };
    off_t offset = slot * CHECKPOINT_SLOT_SIZE;
    ssize_t bytes_written;
    bytes_written = pwrite(fd, snapshot, STATE_CHUNK_SIZE, offset + CHECKPOINT_HEADER_SIZE);
    if (bytes_written != STATE_CHUNK_SIZE) {
        perror("pwrite snapshot failed");
        // Potentially exit or handle error more gracefully
    }
    bytes_written = pwrite(fd, ws_data, WRITE_SET_CHUNK_SIZE, offset + CHECKPOINT_HEADER_SIZE + STATE_CHUNK_SIZE);
    if (bytes_written != WRITE_SET_CHUNK_SIZE) {
        perror("pwrite ws_data failed");
        // Potentially exit
    }
    bytes_written = pwrite(fd, &header, CHECKPOINT_HEADER_SIZE, offset);
    if (bytes_written != CHECKPOINT_HEADER_SIZE) {
        perror("pwrite header failed");
        // Potentially exit
    }
}

/* Comparator for qsort */
int compare_doubles(const void *a, const void *b) {
    double d1 = *(const double *)a;
    double d2 = *(const double *)b;
    if (d1 < d2)
        return -1;
    else if (d1 > d2)
        return 1;
    return 0;
}

int main(int argc, char **argv) {
    int64_t *state = calloc(TOTAL_STATE_COVERAGE_COUNT, sizeof(int64_t));
    if (!state) 
        exit(EXIT_FAILURE);
    WriteSetEntry *ws_accum[RING_SIZE];
    int ws_count[RING_SIZE] = {0};
    for (uint32_t i = 0; i < RING_SIZE; i++) {
        ws_accum[i] = malloc(WRITE_SET_CHUNK_SIZE);
        if (!ws_accum[i])   
            exit(EXIT_FAILURE);
    }

    int recovered_batch = -1;
    int log_fd = open(LOG_FILE, O_RDWR);
    if (log_fd >= 0) {
        reconstruct_state(log_fd, state, &recovered_batch);
        printf("Reconstructed state up to batch %d.\n", recovered_batch);
        uint64_t hash = fnv1a_hash(state, SMALL_ACCOUNT_COUNT);
        FILE *hash_fp = fopen("state_hash.dat", "rb");
        if (hash_fp) {
            uint64_t stored_hash;
            size_t items_read = fread(&stored_hash, sizeof(uint64_t), 1, hash_fp);
            if (items_read != 1 && ferror(hash_fp)) {
                perror("fread from state_hash.dat failed");
            }
            fclose(hash_fp);
            if (hash != stored_hash && items_read == 1) {
                printf("Hash mismatch: computed %lu, stored %lu\n", (unsigned long)hash, (unsigned long)stored_hash);
                write_state_to_file("reconstructed_state.txt", state, SMALL_ACCOUNT_COUNT);
            } else if (items_read != 1 && !feof(hash_fp)) {
                printf("Warning: Could not read stored hash or file was empty.\n");
            }
        }
        close(log_fd);
        if (argc > 1 && strcmp(argv[1], "recover") == 0) {
            for (uint32_t i = 0; i < RING_SIZE; i++) free(ws_accum[i]);
            free(state);
            return 0;
        }
    }

    preallocate_log_file(LOG_FILE);
    log_fd = open(LOG_FILE, O_RDWR);
    if (log_fd < 0) 
        exit(EXIT_FAILURE);
    Transaction *transaction_batch = malloc(BATCH_SIZE * sizeof(Transaction));
    if (!transaction_batch) 
        exit(EXIT_FAILURE);
    FILE *fp_transactions = fopen(TX_FILE, "rb");
    if (!fp_transactions) { 
        free(state);  
        free(transaction_batch); 
        close(log_fd); 
        exit(EXIT_FAILURE); 
    }
    double *batch_times = malloc(NUMBER_OF_BATCHES * sizeof(double));
    if (!batch_times) { 
        free(state); 
        free(transaction_batch); 
        fclose(fp_transactions); 
        close(log_fd); 
        exit(EXIT_FAILURE); 
    }
    
    double total_time = 0.0;
    
    void *empty_ws = calloc(1, WRITE_SET_CHUNK_SIZE);
    if (!empty_ws) 
        exit(EXIT_FAILURE);
    uint64_t start_total = get_time_ms();
    for (unsigned int batch_num = 0; batch_num < NUMBER_OF_BATCHES; batch_num++) {
        double start = get_time_ms();
        size_t items = fread(transaction_batch, sizeof(Transaction), BATCH_SIZE, fp_transactions);
        if (items != BATCH_SIZE) 
            break;
        for (unsigned int i = 0; i < BATCH_SIZE; i++)
            apply(&transaction_batch[i], state, ws_accum, ws_count);
        double end = get_time_ms();
        if ((batch_num + 1) % RING_SIZE == 0) {
            for (uint32_t slot = 0; slot < RING_SIZE; slot++) {
                write_checkpoint_slot(log_fd, slot, batch_num, state, ws_accum[slot], ws_count[slot]);
                ws_count[slot] = 0;
            }
            fsync(log_fd);
        }
        double duration = end - start;
        batch_times[batch_num] = duration;
        total_time += duration;
        printf("Batch %u processed in %.3f ms.\n", batch_num, duration);
    }

    if (NUMBER_OF_BATCHES % RING_SIZE != 0) {
        for (uint32_t slot = 0; slot < RING_SIZE; slot++) {
            CheckpointHeader header = { CHECKPOINT_MAGIC, NUMBER_OF_BATCHES - 1, slot * STATE_CHUNK_COUNT,
                                          STATE_CHUNK_COUNT, 0 };
            off_t offset = slot * CHECKPOINT_SLOT_SIZE;
            ssize_t bytes_written_main;
            bytes_written_main = pwrite(log_fd, state + slot * STATE_CHUNK_COUNT, STATE_CHUNK_SIZE, offset + CHECKPOINT_HEADER_SIZE);
            if (bytes_written_main != STATE_CHUNK_SIZE) {
                perror("pwrite final state chunk failed");
            }
            bytes_written_main = pwrite(log_fd, empty_ws, WRITE_SET_CHUNK_SIZE, offset + CHECKPOINT_HEADER_SIZE + STATE_CHUNK_SIZE);
            if (bytes_written_main != WRITE_SET_CHUNK_SIZE) {
                perror("pwrite final empty_ws failed");
            }
            bytes_written_main = pwrite(log_fd, &header, CHECKPOINT_HEADER_SIZE, offset);
            if (bytes_written_main != CHECKPOINT_HEADER_SIZE) {
                perror("pwrite final header failed");
            }
        }
        fsync(log_fd);
    }

    uint64_t end_total = get_time_ms();
    fclose(fp_transactions);
    close(log_fd);

    double average = total_time / NUMBER_OF_BATCHES;
    double *sorted = malloc(NUMBER_OF_BATCHES * sizeof(double));
    memcpy(sorted, batch_times, NUMBER_OF_BATCHES * sizeof(double));
    qsort(sorted, NUMBER_OF_BATCHES, sizeof(double), compare_doubles);
    double median = sorted[NUMBER_OF_BATCHES / 2];
    double p90 = sorted[(int)(NUMBER_OF_BATCHES * 0.9) - 1];
    double p99 = sorted[(int)(NUMBER_OF_BATCHES * 0.99) - 1];
    printf("\nMetrics (ms): Total %.3f, Avg %.3f, Med %.3f, 90th %.3f, 99th %.3f\n",
           total_time, average, median, p90, p99);

    uint64_t state_hash = fnv1a_hash(state, SMALL_ACCOUNT_COUNT);
    printf("Final state hash: %lu\n", (unsigned long)state_hash);
    FILE *hash_fp = fopen("state_hash.dat", "wb");
    if (hash_fp) {
        size_t written = fwrite(&state_hash, sizeof(uint64_t), 1, hash_fp);
        if (written != 1) {
            perror("fwrite to state_hash.dat failed");
        }
        fclose(hash_fp);
    }

    free(state);
    for (uint32_t i = 0; i < RING_SIZE; i++) free(ws_accum[i]);
    free(batch_times);
    free(sorted);
    free(transaction_batch);
    free(empty_ws);
    printf("Total time: %.3f ms\n", (double)(end_total - start_total));
    return 0;
}
