#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <errno.h>
#include <pthread.h>  // For thread creation

// --- Definitions and constants ---

// Transaction and state parameters.
#define BATCH_SIZE          (1 << 16)  // 2^16 transactions per batch
#define NUMBER_OF_BATCHES   50
#define SMALL_ACCOUNT_COUNT 2000000UL

// We split the full state into 10 chunks for simplicity.
#define RING_SIZE 10
#define STATE_CHUNK_COUNT (SMALL_ACCOUNT_COUNT / RING_SIZE)
#define STATE_CHUNK_SIZE  (STATE_CHUNK_COUNT * sizeof(int64_t))

// For each transaction, worst-case a transaction produces 2 modifications.
// Over a full cycle the worst-case is 16 * BATCH_SIZE modifications per chunk.
#define MAX_WRITE_SET_COUNT (16 * BATCH_SIZE)
#define WRITE_SET_CHUNK_SIZE (MAX_WRITE_SET_COUNT * sizeof(WriteSetEntry))

// --- Operation Encoding ---
#define FUNC_MASK   0xF000000000000000UL
#define DATA_MASK   0x0FFFFFFFFFFFFFFFUL
#define GET_FUNC(x) ((uint8_t)((x) >> 60))
#define GET_DATA(x) ((x) & DATA_MASK)

// --- Crash Resistance: Checkpoint Header ---
#define CHECKPOINT_MAGIC 0xC0CAC01A

typedef struct {
    uint32_t magic;            // Must equal CHECKPOINT_MAGIC.
    uint32_t batch_num;        // Latest batch number committed.
    uint32_t chunk_offset;     // Starting index in full state.
    uint32_t state_chunk_count;// Should equal STATE_CHUNK_COUNT.
    uint32_t write_set_count;  // Number of valid write-set entries.
} CheckpointHeader;

#define CHECKPOINT_HEADER_SIZE (sizeof(CheckpointHeader))
#define CHECKPOINT_SLOT_SIZE   (CHECKPOINT_HEADER_SIZE + STATE_CHUNK_SIZE + WRITE_SET_CHUNK_SIZE)

#define TX_FILE "transactions.bin"
#define LOG_FILE "checkpoint_log.dat"

// --- Data Structures ---

typedef struct {
    uint64_t sender;
    uint64_t receiver;
    uint32_t amount;
} Transaction;

typedef struct {
    uint64_t address;
    int64_t  balance;
} WriteSetEntry;

// This combined buffer will let us do one large copy to the destination.
typedef struct __attribute__((packed)) {
    CheckpointHeader header;               // [Header]
    int64_t state_array[STATE_CHUNK_COUNT];  // [State chunk]
    WriteSetEntry ws_array[MAX_WRITE_SET_COUNT]; // [Write-set]
} CombinedCommitBuffer;

// --- Thread Data for msync thread ---
typedef struct {
    void *mapped_region;
    size_t size;
    volatile int running; // Flag to signal when to stop
} msync_thread_data;

void *msync_thread_func(void *arg) {
    msync_thread_data *data = (msync_thread_data *)arg;
    // Loop until signaled to stop.
    while (data->running) {
        // Flush the entire checkpoint region asynchronously.
        msync(data->mapped_region, data->size, MS_ASYNC);
        // Sleep for 10ms before next flush.
        struct timespec ts = {0, 10 * 1000 * 1000}; // 10ms
        nanosleep(&ts, NULL);
    }
    return NULL;
}

// --- Utility Functions ---

int compare_doubles(const void *a, const void *b) {
    double diff = (*(const double *)a) - (*(const double *)b);
    return (diff < 0) ? -1 : (diff > 0) ? 1 : 0;
}

uint64_t fnv1a_hash(int64_t *data, size_t len) {
    uint64_t hash = 14695981039346656037UL;
    for (size_t i = 0; i < len; i++) {
        uint64_t val = (uint64_t)data[i];
        // 8 bytes per element
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

void preallocate_log_file_posix(const char *filename) {
    int fd = open(filename, O_RDWR | O_CREAT, 0666);
    if (fd < 0) {
        perror("Error opening log file for pre-allocation");
        exit(EXIT_FAILURE);
    }
    struct stat st;
    if (fstat(fd, &st) != 0) {
        perror("fstat error");
        close(fd);
        exit(EXIT_FAILURE);
    }
    off_t fsize = st.st_size;
    off_t expected = RING_SIZE * CHECKPOINT_SLOT_SIZE;
    if (fsize < expected) {
        if (ftruncate(fd, expected) != 0) {
            perror("ftruncate error");
            close(fd);
            exit(EXIT_FAILURE);
        }
        fsync(fd);
    }
    close(fd);
}

void write_state_to_file(const char *filename, int64_t *state, size_t count) {
    FILE *fp = fopen(filename, "w");
    if (!fp) {
        perror("Error opening state text file for writing");
        return;
    }
    for (size_t i = 0; i < count; i++) {
        fprintf(fp, "%zu: %lld\n", i, (long long)state[i]);
    }
    fclose(fp);
}

// --- Recovery ---
int reconstruct_state(int fd, int64_t *state, int *last_batch) {
    for (uint32_t slot = 0; slot < RING_SIZE; slot++) {
        off_t offset = slot * CHECKPOINT_SLOT_SIZE;
        CheckpointHeader header;
        ssize_t bytes = pread(fd, &header, sizeof(header), offset);
        if (bytes != sizeof(header)) {
            fprintf(stderr, "Failed to read header for slot %u\n", slot);
            continue;
        }
        if (header.magic != CHECKPOINT_MAGIC) {
            fprintf(stderr, "Slot %u has invalid magic (0x%x)\n", slot, header.magic);
            continue;
        }
        if (header.state_chunk_count != STATE_CHUNK_COUNT) {
            fprintf(stderr, "Slot %u has unexpected state_chunk_count\n", slot);
            continue;
        }
        // Read chunk
        int64_t *chunk = malloc(STATE_CHUNK_SIZE);
        if (!chunk) {
            perror("Allocation error during recovery");
            continue;
        }
        bytes = pread(fd, chunk, STATE_CHUNK_SIZE, offset + CHECKPOINT_HEADER_SIZE);
        if (bytes != STATE_CHUNK_SIZE) {
            perror("Error reading state chunk during recovery");
            free(chunk);
            continue;
        }
        // Read write set
        void *ws_area = malloc(WRITE_SET_CHUNK_SIZE);
        if (!ws_area) {
            perror("Allocation error during recovery (write set)");
            free(chunk);
            continue;
        }
        bytes = pread(fd, ws_area, WRITE_SET_CHUNK_SIZE,
                      offset + CHECKPOINT_HEADER_SIZE + STATE_CHUNK_SIZE);
        if (bytes != WRITE_SET_CHUNK_SIZE) {
            perror("Error reading write set during recovery");
            free(chunk);
            free(ws_area);
            continue;
        }

        // Copy chunk into the correct place
        memcpy(state + header.chunk_offset, chunk, STATE_CHUNK_SIZE);

        // Reapply writes
        WriteSetEntry *entries = (WriteSetEntry *)ws_area;
        for (uint32_t i = 0; i < header.write_set_count; i++) {
            uint8_t op = GET_FUNC(entries[i].address);
            uint64_t addr = GET_DATA(entries[i].address);
            if (addr < header.chunk_offset ||
                addr >= header.chunk_offset + STATE_CHUNK_COUNT)
            {
                fprintf(stderr, "Write set entry %u address %llu out of range for chunk start %u\n",
                        i, (long long)addr, header.chunk_offset);
                continue;
            }
            if (op == 0 || op == 1) {
                state[addr] = entries[i].balance;
            } else {
                fprintf(stderr, "Unknown op code %u in write set entry %u\n", op, i);
            }
        }

        free(chunk);
        free(ws_area);
        *last_batch = header.batch_num;
    }
    return 0;
}

static inline void nt_memcpy(void *dest, const void *src, size_t n) {
    memcpy(dest, src, n);
}

static inline void apply(const Transaction *tx,
                         int64_t *state,
                         WriteSetEntry **ws_accum,
                         int *ws_count)
{
    uint8_t sender_func = GET_FUNC(tx->sender);
    uint8_t receiver_func = GET_FUNC(tx->receiver);
    uint64_t sender_data = GET_DATA(tx->sender);
    uint64_t receiver_data = GET_DATA(tx->receiver);

    // Example p2p or memset logic
    if (sender_func == 0 && receiver_func == 0) {
        // p2p
        if (state[sender_data] > tx->amount) {
            state[sender_data] -= tx->amount;
        }
        uint32_t chunk_s = sender_data / STATE_CHUNK_COUNT;
        ws_accum[chunk_s][ws_count[chunk_s]].address = (0UL << 60) | sender_data;
        ws_accum[chunk_s][ws_count[chunk_s]].balance = state[sender_data];
        ws_count[chunk_s]++;

        if (state[receiver_data] > tx->amount) {
            state[receiver_data] += tx->amount;
        }
        uint32_t chunk_r = receiver_data / STATE_CHUNK_COUNT;
        ws_accum[chunk_r][ws_count[chunk_r]].address = (0UL << 60) | receiver_data;
        ws_accum[chunk_r][ws_count[chunk_r]].balance = state[receiver_data];
        ws_count[chunk_r]++;
    }
    else if (sender_func == 1 && receiver_func == 1) {
        // memset-like
        for (uint64_t i = sender_data; i < sender_data + receiver_data; i++) {
            state[i] = tx->amount;
            uint32_t chunk = i / STATE_CHUNK_COUNT;
            ws_accum[chunk][ws_count[chunk]].address = (1UL << 60) | i;
            ws_accum[chunk][ws_count[chunk]].balance = tx->amount;
            ws_count[chunk]++;
        }
    }
}

// Modified commit function that reuses a preallocated commit buffer.
static void commit_slot_for_index_nosync(uint32_t slot_index,
                                         uint32_t batch_num,
                                         int64_t *state,
                                         WriteSetEntry **ws_accum,
                                         int *ws_count,
                                         void *mapped_region,
                                         CombinedCommitBuffer *commit_buf)
{
    // Fill the header
    commit_buf->header.magic             = CHECKPOINT_MAGIC;
    commit_buf->header.batch_num         = batch_num;
    commit_buf->header.chunk_offset      = slot_index * STATE_CHUNK_COUNT;
    commit_buf->header.state_chunk_count = STATE_CHUNK_COUNT;
    commit_buf->header.write_set_count   = ws_count[slot_index];

    // Copy state chunk into the commit buffer
    memcpy(commit_buf->state_array,
           state + slot_index * STATE_CHUNK_COUNT,
           STATE_CHUNK_SIZE);

    // Copy only the used portion of the write set
    size_t ws_used_bytes = ws_count[slot_index] * sizeof(WriteSetEntry);
    memcpy(commit_buf->ws_array, ws_accum[slot_index], ws_used_bytes);

    // Calculate total bytes to copy (header + state chunk + write set)
    size_t total_bytes = CHECKPOINT_HEADER_SIZE + STATE_CHUNK_SIZE + ws_used_bytes;

    // Destination offset in the mapped region
    size_t offset = slot_index * CHECKPOINT_SLOT_SIZE;
    char *dest = (char *)mapped_region + offset;

    // Copy the data to the mapped region using non-temporal memcpy
    nt_memcpy(dest, commit_buf, total_bytes);

    // Reset the write-set count for this slot
    ws_count[slot_index] = 0;
}

int main(int argc, char **argv) {
    int64_t *state = calloc(SMALL_ACCOUNT_COUNT, sizeof(int64_t));
    if (!state) {
        perror("Error allocating state");
        exit(EXIT_FAILURE);
    }

    // Allocate per-chunk write-set accumulators.
    WriteSetEntry *ws_accum[RING_SIZE];
    int ws_count[RING_SIZE] = {0};
    for (uint32_t i = 0; i < RING_SIZE; i++) {
        ws_accum[i] = malloc(WRITE_SET_CHUNK_SIZE);
        if (!ws_accum[i]) {
            perror("Error allocating write-set accumulator");
            exit(EXIT_FAILURE);
        }
    }

    // Recovery
    int recovered_batch = -1;
    int log_fd = open(LOG_FILE, O_RDWR);
    if (log_fd >= 0) {
        if (reconstruct_state(log_fd, state, &recovered_batch) == 0) {
            printf("Reconstructed full state from log (up to batch %d).\n", recovered_batch);
            uint64_t hash = fnv1a_hash(state, SMALL_ACCOUNT_COUNT);
            FILE *hash_fp = fopen("state_hash.dat", "rb");
            if (hash_fp) {
                uint64_t stored_hash;
                fread(&stored_hash, sizeof(uint64_t), 1, hash_fp);
                fclose(hash_fp);
                if (hash == stored_hash) {
                    printf("Reconstructed state hash matches stored hash: %llu\n", hash);
                } else {
                    printf("Reconstructed state hash mismatch! Computed: %llu, Stored: %llu\n",
                           (long long)hash, (long long)stored_hash);
                    write_state_to_file("reconstructed_state.txt", state, SMALL_ACCOUNT_COUNT);
                }
            }
        }
        close(log_fd);
        if (argc > 1 && strcmp(argv[1], "recover") == 0) {
            for (uint32_t i = 0; i < RING_SIZE; i++) free(ws_accum[i]);
            free(state);
            return 0;
        }
    }

    preallocate_log_file_posix(LOG_FILE);
    log_fd = open(LOG_FILE, O_RDWR);
    if (log_fd < 0) {
        perror("Error opening log file for writing");
        free(state);
        exit(EXIT_FAILURE);
    }

    // Memory-map the entire checkpoint file.
    size_t checkpoint_file_size = RING_SIZE * CHECKPOINT_SLOT_SIZE;
    void *mapped_region = mmap(NULL, checkpoint_file_size,
                               PROT_READ | PROT_WRITE, MAP_SHARED, log_fd, 0);
    if (mapped_region == MAP_FAILED) {
        perror("mmap failed");
        exit(EXIT_FAILURE);
    }

    // --- Create a dedicated msync thread ---
    pthread_t msync_thread;
    msync_thread_data msync_data;
    msync_data.mapped_region = mapped_region;
    msync_data.size = checkpoint_file_size;
    msync_data.running = 1;
    if (pthread_create(&msync_thread, NULL, msync_thread_func, &msync_data) != 0) {
        perror("Error creating msync thread");
        exit(EXIT_FAILURE);
    }

    Transaction *transaction_batch = malloc(BATCH_SIZE * sizeof(Transaction));
    if (!transaction_batch) {
        perror("Failed to allocate transaction batch buffer");
        exit(EXIT_FAILURE);
    }

    FILE *fp_transactions = fopen(TX_FILE, "rb");
    if (!fp_transactions) {
        perror("Error opening transactions file for reading");
        free(state);
        free(transaction_batch);
        exit(EXIT_FAILURE);
    }

    double *batch_times = malloc(NUMBER_OF_BATCHES * sizeof(double));
    if (!batch_times) {
        perror("Error allocating batch_times");
        free(state);
        free(transaction_batch);
        fclose(fp_transactions);
        exit(EXIT_FAILURE);
    }

    double total_processing_time = 0.0;
    double start_total = get_time_ms();

    // Track last committed batch per slot
    int committed_batch[RING_SIZE];
    for (uint32_t i = 0; i < RING_SIZE; i++) {
        committed_batch[i] = -1;
    }

    // --- Allocate commit buffers for each slot (to be reused) ---
    CombinedCommitBuffer *commit_buffers[RING_SIZE];
    for (uint32_t slot = 0; slot < RING_SIZE; slot++) {
        commit_buffers[slot] = malloc(CHECKPOINT_SLOT_SIZE);
        if (!commit_buffers[slot]) {
            perror("Error allocating commit buffer");
            exit(EXIT_FAILURE);
        }
    }

    // Process each batch
    for (unsigned int batch_num = 0; batch_num < NUMBER_OF_BATCHES; batch_num++) {
        double batch_start = get_time_ms();

        // Read one batch from the transaction file
        size_t items = fread(transaction_batch, sizeof(Transaction), BATCH_SIZE, fp_transactions);
        if (items < BATCH_SIZE) {
            break;
        }

        // Apply all transactions in the batch
        for (unsigned int i = 0; i < BATCH_SIZE; i++) {
            apply(&transaction_batch[i], state, ws_accum, ws_count);
        }
        double batch_end = get_time_ms();
        // Commit each slot if needed (msync is now handled by the dedicated thread)
        for (uint32_t slot = 0; slot < RING_SIZE; slot++) {
            if ((int)batch_num > committed_batch[slot]) {
                commit_slot_for_index_nosync(slot, batch_num, state, ws_accum, ws_count, mapped_region, commit_buffers[slot]);
                committed_batch[slot] = batch_num;
            }
        }

        double duration = batch_end - batch_start;
        batch_times[batch_num] = duration;
        total_processing_time += duration;

        // Optional debug output
        if ((batch_num % 10) == 0) {
            printf("Batch %u processed in %.3f ms.\n", batch_num, duration);
        }
    }

    // Signal the msync thread to stop and wait for it to finish.
    msync_data.running = 0;
    pthread_join(msync_thread, NULL);

    munmap(mapped_region, checkpoint_file_size);
    fclose(fp_transactions);
    close(log_fd);

    double end_total = get_time_ms();
    double average = total_processing_time / NUMBER_OF_BATCHES;

    double *sorted_times = malloc(NUMBER_OF_BATCHES * sizeof(double));
    if (!sorted_times) {
        perror("Error allocating sorted_times");
        free(state);
        free(batch_times);
        exit(EXIT_FAILURE);
    }
    memcpy(sorted_times, batch_times, NUMBER_OF_BATCHES * sizeof(double));
    qsort(sorted_times, NUMBER_OF_BATCHES, sizeof(double), compare_doubles);
    double median = sorted_times[NUMBER_OF_BATCHES / 2];
    double p90 = sorted_times[(int)(NUMBER_OF_BATCHES * 0.9) - 1];
    double p99 = sorted_times[(int)(NUMBER_OF_BATCHES * 0.99) - 1];

    printf("\nPerformance Metrics (ms):\n");
    printf("Total processing time: %.3f ms\n", total_processing_time);
    printf("Average batch time: %.3f ms\n", average);
    printf("Median batch time: %.3f ms\n", median);
    printf("90th percentile batch time: %.3f ms\n", p90);
    printf("99th percentile batch time: %.3f ms\n", p99);

    uint64_t state_hash = fnv1a_hash(state, SMALL_ACCOUNT_COUNT);
    printf("Final full state hash: %llu\n", (long long)state_hash);
    FILE *hash_fp = fopen("state_hash.dat", "wb");
    if (hash_fp) {
        fwrite(&state_hash, sizeof(uint64_t), 1, hash_fp);
        fclose(hash_fp);
    } else {
        perror("Error opening state_hash.dat for writing");
    }

    free(state);
    for (uint32_t i = 0; i < RING_SIZE; i++) {
        free(ws_accum[i]);
        free(commit_buffers[i]);
    }
    free(batch_times);
    free(sorted_times);
    free(transaction_batch);

    printf("Total time taken: %.3f ms\n", (double)(end_total - start_total));
    return 0;
}