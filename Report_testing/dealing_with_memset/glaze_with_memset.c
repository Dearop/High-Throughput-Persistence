#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <errno.h>
#include <inttypes.h>
#include <pthread.h>

#define ACCOUNT_SIZE         8       
#define BATCH_SIZE          (1 << 16)
#define NUMBER_OF_BATCHES   5000
uint64_t SMALL_ACCOUNT_COUNT = 5000000UL; // Made into a variable
#define STATE_CHUNK_SIZE    (512 * 1024)  // 512KB state chunks
#define TARGET_CHUNK_DATA_BYTES   (STATE_CHUNK_SIZE / sizeof(int64_t))
#define ACCOUNTS_PER_STATE_CHUNK (TARGET_CHUNK_DATA_BYTES / ACCOUNT_SIZE)
#define RING_SIZE           ((SMALL_ACCOUNT_COUNT + ACCOUNTS_PER_STATE_CHUNK - 1) / ACCOUNTS_PER_STATE_CHUNK)
#define INITIAL_WS_CAPACITY 1024  
     


// Operation encoding macros
#define FUNC_MASK   0xF000000000000000UL
#define DATA_MASK   0x0FFFFFFFFFFFFFFFUL
#define GET_OP(x)   ((x & FUNC_MASK) >> 60)
#define GET_DATA(x) (x & DATA_MASK)

typedef struct {
    uint64_t sender;
    uint64_t receiver;
    uint64_t amount;
} Transaction;

typedef struct {
    uint64_t address;
    int64_t balance;
} WriteSetEntry;

typedef struct {
    uint32_t batch_num;
    uint32_t state_chunk_count;
    uint32_t write_set_count;
    uint32_t reserved;
} CheckpointHeader;

// Struct for checkpoint data to be passed to the worker thread
typedef struct {
    CheckpointHeader header;
    int64_t* state_snapshot; // A copy of the relevant state part
    WriteSetEntry* write_set_snapshot; // A copy of the write set
    uint32_t write_set_count_snapshot;
} AsyncCheckpointData;

// Global variables for asynchronous checkpointing
pthread_t checkpoint_thread_id;
pthread_mutex_t g_checkpoint_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t g_cond_slot_empty = PTHREAD_COND_INITIALIZER;  // Main waits on this if slot is full
pthread_cond_t g_cond_slot_full = PTHREAD_COND_INITIALIZER;   // Worker waits on this if slot is empty
AsyncCheckpointData g_checkpoint_slot; // The single shared slot
int g_slot_is_full = 0; // 0 = empty, 1 = full
int g_terminate_checkpoint_thread = 0;
int g_log_fd; // Global log file descriptor, to be used by worker thread
int g_async_error_occurred = 0; // Flag for async errors

// Add recovery functions from state_management.c
void write_state_to_file(const char *filename, int64_t *state, size_t count) {
    FILE *fp = fopen(filename, "w");
    if (!fp) return;
    for (size_t i = 0; i < count; i++) {
        fprintf(fp, "%zu: %lld\n", i, (long long)state[i]);
    }
    fclose(fp);
}

typedef struct {
    uint32_t batch_num;
    int64_t *state_chunk;
    WriteSetEntry *write_set;
    uint32_t write_set_count;
} CheckpointRecord;

int compare_checkpoint_record(const void *a, const void *b) {
    return (int)(((CheckpointRecord*)a)->batch_num - ((CheckpointRecord*)b)->batch_num);
}

int reconstruct_state(int fd, int64_t *state, uint32_t *last_batch) {
    struct stat st;
    if (fstat(fd, &st) != 0) return -1;
    
    size_t file_size = st.st_size;
    size_t read_pos = 0;
    CheckpointRecord *records = NULL;
    size_t record_count = 0;

    while (read_pos < file_size) {
        CheckpointHeader header;
        ssize_t bytes_read = pread(fd, &header, sizeof(header), read_pos);
        if (bytes_read != sizeof(header)) {
            fprintf(stderr, "Error reading header: %zd/%zu bytes\n", bytes_read, sizeof(header));
            return -1;
        }
        read_pos += sizeof(header);

        // Read state chunk
        int64_t *state_chunk = malloc(STATE_CHUNK_SIZE);
        bytes_read = pread(fd, state_chunk, STATE_CHUNK_SIZE, read_pos);
        if (bytes_read != STATE_CHUNK_SIZE) {
            fprintf(stderr, "Error reading state chunk: %zd/%d bytes\n", 
                    bytes_read, STATE_CHUNK_SIZE);
            free(state_chunk);
            return -1;
        }
        read_pos += STATE_CHUNK_SIZE;

        // Read write set
        WriteSetEntry *ws = malloc(header.write_set_count * sizeof(WriteSetEntry));
        bytes_read = pread(fd, ws, header.write_set_count * sizeof(WriteSetEntry), read_pos);
        if (bytes_read != (ssize_t)(header.write_set_count * sizeof(WriteSetEntry))) {
            fprintf(stderr, "Error reading write set: %zd/%zu bytes\n",
                    bytes_read, header.write_set_count * sizeof(WriteSetEntry));
            free(ws);
            return -1;
        }
        read_pos += header.write_set_count * sizeof(WriteSetEntry);

        // Store record
        records = realloc(records, (record_count+1)*sizeof(CheckpointRecord));
        records[record_count++] = (CheckpointRecord){
            header.batch_num, state_chunk, ws, header.write_set_count
        };
    }

    if (record_count == 0) return -1;

    qsort(records, record_count, sizeof(CheckpointRecord), compare_checkpoint_record);
    memcpy(state, records[0].state_chunk, STATE_CHUNK_SIZE);
    *last_batch = records[0].batch_num;

    for (size_t i = 1; i < record_count; i++) {
        for (uint32_t j = 0; j < records[i].write_set_count; j++) {
            uint64_t addr = records[i].write_set[j].address;
            if (addr < ACCOUNTS_PER_STATE_CHUNK)
                state[addr] = records[i].write_set[j].balance;
        }
        *last_batch = records[i].batch_num;
    }

    // Cleanup
    for (size_t i = 0; i < record_count; i++) {
        free(records[i].state_chunk);
        free(records[i].write_set);
    }
    free(records);
    
    return 0;
}

uint64_t fnv1a_hash(int64_t *data, size_t len) {
    uint64_t hash = 14695981039346656037UL;
    for (size_t i = 0; i < len; i++) {
        hash ^= data[i];
        hash *= 1099511628211UL;
    }
    return hash;
}

double get_time_ms() {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec * 1000.0 + ts.tv_nsec / 1e6;
}

void preallocate_log_file(const char *filename, size_t size) {
    int fd = open(filename, O_RDWR | O_CREAT, 0666);
    if (fd < 0) {
        perror("open");
        exit(EXIT_FAILURE);
    }
    if (ftruncate(fd, size) != 0) {
        perror("ftruncate");
        close(fd);
        exit(EXIT_FAILURE);
    }
    close(fd);
}

int apply_transaction(const Transaction *tx, int64_t *state, 
                     WriteSetEntry **ws, uint32_t *ws_count, uint32_t *ws_capacity) {
    uint8_t op_type = GET_OP(tx->sender);
    uint32_t initial_ws = *ws_count;

    if (op_type == 1) { // Memset operation
        uint64_t start = GET_DATA(tx->sender);
        uint64_t count = GET_DATA(tx->receiver);
        
        // Check if we need to expand write-set
        if (*ws_count + count > *ws_capacity) {
            *ws_capacity = (*ws_capacity + count) * 2;
            WriteSetEntry *new_ws = realloc(*ws, *ws_capacity * sizeof(WriteSetEntry));
            if (!new_ws) {
                perror("Write-set realloc failed");
                free(*ws);
                exit(EXIT_FAILURE);
            }
            *ws = new_ws;
        }

        for (uint64_t i = 0; i < count; i++) {
            uint64_t addr = (start + i) % SMALL_ACCOUNT_COUNT;
            if (addr >= SMALL_ACCOUNT_COUNT) continue;
            
            state[addr] = tx->amount;
            (*ws)[*ws_count] = (WriteSetEntry){addr, state[addr]};
            (*ws_count)++;
        }
    } else { // P2P transfer
        // Check capacity for 2 new entries
        if (*ws_count + 2 > *ws_capacity) {
            *ws_capacity *= 2;
            WriteSetEntry *new_ws = realloc(*ws, *ws_capacity * sizeof(WriteSetEntry));
            if (!new_ws) {
                perror("Write-set realloc failed");
                free(*ws);
                exit(EXIT_FAILURE);
            }
            *ws = new_ws;
        }

        uint64_t sender = GET_DATA(tx->sender);
        uint64_t receiver = GET_DATA(tx->receiver);

        if (sender < SMALL_ACCOUNT_COUNT) {
            state[sender] -= tx->amount;
            (*ws)[*ws_count] = (WriteSetEntry){sender, state[sender]};
            (*ws_count)++;
        }
        
        if (receiver < SMALL_ACCOUNT_COUNT) {
            state[receiver] += tx->amount;
            (*ws)[*ws_count] = (WriteSetEntry){receiver, state[receiver]};
            (*ws_count)++;
        }
    }
    return *ws_count - initial_ws;
}

// Add performance tracking structures
typedef struct {
    double total_time;
    double avg_time;
    double median_time;
    double p90_time;
    double p99_time;
} PerformanceStats;

// Add static comparator function
static int compare_doubles(const void *a, const void *b) {
    double diff = (*(const double*)a) - (*(const double*)b);
    return (diff < 0) ? -1 : (diff > 0) ? 1 : 0;
}

// Checkpoint worker thread function
void* checkpoint_thread_function(void* arg) {
    (void)arg; // Unused

    while (1) {
        AsyncCheckpointData local_copy_data; 
        int io_error = 0;

        pthread_mutex_lock(&g_checkpoint_mutex);
        while (!g_slot_is_full && !g_terminate_checkpoint_thread) {
            pthread_cond_wait(&g_cond_slot_full, &g_checkpoint_mutex);
        }

        if (g_terminate_checkpoint_thread && !g_slot_is_full) {
            pthread_mutex_unlock(&g_checkpoint_mutex);
            break; // Terminate
        }

        // Copy data from global slot to local_copy_data
        local_copy_data = g_checkpoint_slot; 

        g_slot_is_full = 0; // Mark slot as empty
        pthread_cond_signal(&g_cond_slot_empty); // Signal main thread that slot is empty
        pthread_mutex_unlock(&g_checkpoint_mutex);

        // --- Perform I/O (outside mutex) ---
        ssize_t bytes_written;
        
        // 1. Write header
        bytes_written = write(g_log_fd, &local_copy_data.header, sizeof(local_copy_data.header));
        if (bytes_written != sizeof(local_copy_data.header)) {
            perror("Async: Error writing header");
            io_error = 1;
        }

        // 2. Write state chunk
        if (!io_error) {
            bytes_written = write(g_log_fd, local_copy_data.state_snapshot, STATE_CHUNK_SIZE);
            if (bytes_written != STATE_CHUNK_SIZE) {
                perror("Async: Error writing state chunk");
                io_error = 1;
            }
        }
        if(local_copy_data.state_snapshot) free(local_copy_data.state_snapshot);

        // 3. Write write-set entries
        if (!io_error && local_copy_data.write_set_count_snapshot > 0) {
            size_t total_to_write = local_copy_data.write_set_count_snapshot * sizeof(WriteSetEntry);
            ssize_t written_this_call = 0;
            ssize_t rc;
            char* write_set_ptr = (char*)local_copy_data.write_set_snapshot;
            
            while (written_this_call < total_to_write) {
                rc = write(g_log_fd, write_set_ptr + written_this_call, total_to_write - written_this_call);
                if (rc < 0) {
                    perror("Async: Error writing write set entry");
                    io_error = 1;
                    break; 
                }
                written_this_call += rc;
            }
            if (!io_error && written_this_call != total_to_write) {
                 fprintf(stderr, "Async: Partial write set write: %zd/%zu bytes\n", 
                      written_this_call, total_to_write);
                 io_error = 1;
            }
        }
        if(local_copy_data.write_set_snapshot) free(local_copy_data.write_set_snapshot);
        
        if (!io_error) {
            if (fsync(g_log_fd) != 0) {
                perror("Async: Error fsyncing");
                io_error = 1;
            }
        }
        
        if (io_error) {
            fprintf(stderr, "Async checkpoint thread encountered an I/O error.\n");
            pthread_mutex_lock(&g_checkpoint_mutex);
            g_async_error_occurred = 1;
            pthread_mutex_unlock(&g_checkpoint_mutex);
            // Thread will continue to check termination condition
        }
        // --- End I/O ---
    }
    return NULL;
}

int main(int argc, char **argv) {
    if (argc > 1) {
        char *endptr;
        long long parsed_accounts = strtoll(argv[1], &endptr, 10);
        if (*endptr != '\0' || argv[1] == endptr || parsed_accounts <= 0) {
            fprintf(stderr, "Error: Invalid number of accounts '%s'. Must be a positive integer.\n", argv[1]);
            return EXIT_FAILURE;
        }
        SMALL_ACCOUNT_COUNT = (uint64_t)parsed_accounts;
    }

    int64_t *state = calloc(SMALL_ACCOUNT_COUNT, sizeof(int64_t));
    if (!state) {
        fprintf(stderr, "Failed to allocate state array\n");
        exit(EXIT_FAILURE);
    }
    const char *log_file = "checkpoint_log.dat";
    int fd = open(log_file, O_RDWR | O_CREAT, 0666);
    
    // Recovery logic
    uint32_t recovered_batch = UINT32_MAX;
    int log_fd = open(log_file, O_RDWR);
    if (log_fd >= 0) {
        if (reconstruct_state(log_fd, state, &recovered_batch) == 0) {
            printf("Recovered state from batch %" PRIu32 "\n", recovered_batch);
            printf("State hash: 0x%016" PRIx64 "\n", fnv1a_hash(state, ACCOUNTS_PER_STATE_CHUNK));
        }
        close(log_fd);
    }

    // Performance tracking
    double *batch_times = malloc(NUMBER_OF_BATCHES * sizeof(double));
    if (!batch_times) {
        fprintf(stderr, "Failed to allocate batch times array\n");
        free(state);
        exit(EXIT_FAILURE);
    }
    double total_processing_time = 0;
    uint64_t start_total = get_time_ms();

    // Assign fd to global log_fd for the worker thread
    g_log_fd = fd;

    // Create and start the checkpoint worker thread
    if (pthread_create(&checkpoint_thread_id, NULL, checkpoint_thread_function, NULL) != 0) {
        perror("Failed to create checkpoint thread");
        close(fd);
        free(state);
        free(batch_times);
        exit(EXIT_FAILURE);
    }

    // Open transactions.bin once before the loop
    FILE *tx_fp = fopen("transactions.bin", "rb");
    if (!tx_fp) {
        fprintf(stderr, "Failed to open transactions file\n");
        free(state);
        if (batch_times) free(batch_times);
        exit(EXIT_FAILURE);
    }

    // Enhanced recovery reporting
    if (recovered_batch != UINT32_MAX) {
        printf("=== Recovery Details ===\n");
        printf("Recovered state from batch: %" PRIu32 "\n", recovered_batch);
        printf("State hash: 0x%016" PRIx64 "\n", fnv1a_hash(state, ACCOUNTS_PER_STATE_CHUNK));
        printf("========================\n\n");
    }

    for (uint32_t batch = 0; 
         batch < NUMBER_OF_BATCHES; 
         batch++) {
        double batch_start = get_time_ms();
        
        // Initialize write-set with dynamic capacity
        uint32_t ws_capacity = INITIAL_WS_CAPACITY;
        WriteSetEntry *write_set = malloc(ws_capacity * sizeof(WriteSetEntry));
        if (!write_set) {
            fprintf(stderr, "Failed to allocate initial write set\n");
            exit(EXIT_FAILURE);
        }
        uint32_t ws_count = 0;

        // Read batch
        Transaction tx_batch[BATCH_SIZE];
        if (fseek(tx_fp, batch * BATCH_SIZE * sizeof(Transaction), SEEK_SET) != 0) {
            fprintf(stderr, "Error seeking in transactions file for batch %u\n", batch);
            fclose(tx_fp);
            free(state);
            free(batch_times);
            exit(EXIT_FAILURE);
        }
        size_t read_count = fread(tx_batch, sizeof(Transaction), BATCH_SIZE, tx_fp);
        if (read_count != BATCH_SIZE) {
            fprintf(stderr, "Error reading batch %u: expected %d, got %zu\n",
                    batch, BATCH_SIZE, read_count);
            fclose(tx_fp);
            free(state);
            free(batch_times);
            exit(EXIT_FAILURE);
        }

        // Process transactions
        for (int i = 0; i < BATCH_SIZE; i++) {
            apply_transaction(&tx_batch[i], state, &write_set, &ws_count, &ws_capacity);
        }

        // Persist checkpoint (asynchronously)
        CheckpointHeader header = {
            .batch_num = batch,
            .state_chunk_count = ACCOUNTS_PER_STATE_CHUNK,
            .write_set_count = ws_count
        };
        
        // Prepare data for the asynchronous checkpoint thread
        AsyncCheckpointData data_for_slot;
        data_for_slot.header = header; // struct copy
        data_for_slot.write_set_count_snapshot = ws_count;

        data_for_slot.state_snapshot = malloc(STATE_CHUNK_SIZE);
        if (!data_for_slot.state_snapshot) {
            perror("Failed to malloc state_snapshot for async checkpoint");
            pthread_mutex_lock(&g_checkpoint_mutex);
            g_terminate_checkpoint_thread = 1;
            pthread_cond_signal(&g_cond_slot_full);
            pthread_mutex_unlock(&g_checkpoint_mutex);
            pthread_join(checkpoint_thread_id, NULL);
            close(fd); fclose(tx_fp); free(state); free(batch_times); free(write_set);
            exit(EXIT_FAILURE);
        }
        memcpy(data_for_slot.state_snapshot, state, STATE_CHUNK_SIZE);

        if (ws_count > 0) {
            data_for_slot.write_set_snapshot = malloc(ws_count * sizeof(WriteSetEntry));
            if (!data_for_slot.write_set_snapshot) {
                perror("Failed to malloc write_set_snapshot for async checkpoint");
                free(data_for_slot.state_snapshot);
                pthread_mutex_lock(&g_checkpoint_mutex);
                g_terminate_checkpoint_thread = 1;
                pthread_cond_signal(&g_cond_slot_full);
                pthread_mutex_unlock(&g_checkpoint_mutex);
                pthread_join(checkpoint_thread_id, NULL);
                close(fd); fclose(tx_fp); free(state); free(batch_times); free(write_set);
                exit(EXIT_FAILURE);
            }
            memcpy(data_for_slot.write_set_snapshot, write_set, ws_count * sizeof(WriteSetEntry));
        } else {
            data_for_slot.write_set_snapshot = NULL;
        }

        pthread_mutex_lock(&g_checkpoint_mutex);
        while (g_slot_is_full && !g_terminate_checkpoint_thread) {
            if (g_async_error_occurred) break;
            pthread_cond_wait(&g_cond_slot_empty, &g_checkpoint_mutex);
        }
        
        if (g_async_error_occurred) {
            pthread_mutex_unlock(&g_checkpoint_mutex);
            fprintf(stderr, "Main thread: Detected async error. Aborting batch processing.\n");
            free(data_for_slot.state_snapshot);
            if(data_for_slot.write_set_snapshot) free(data_for_slot.write_set_snapshot);
            break; 
        }

        if (!g_terminate_checkpoint_thread) {
            g_checkpoint_slot = data_for_slot;
            g_slot_is_full = 1;
            pthread_cond_signal(&g_cond_slot_full);
        } else {
            free(data_for_slot.state_snapshot);
            if(data_for_slot.write_set_snapshot) free(data_for_slot.write_set_snapshot);
        }
        pthread_mutex_unlock(&g_checkpoint_mutex);
        
        free(write_set);
        
        double batch_end = get_time_ms();
        double duration = batch_end - batch_start;
        batch_times[batch] = duration;
        total_processing_time += duration;

        printf("[Batch %04u] Processed in %6.2f ms | Write ops: %-6u | Throughput: %6.2f Ktx/s\n",
               batch, duration, ws_count,
               (BATCH_SIZE/1000.0) / (duration/1000.0));
    }

    // Wait for the last checkpoint to be processed and terminate worker thread
    pthread_mutex_lock(&g_checkpoint_mutex);
    while (g_slot_is_full && !g_async_error_occurred) {
        pthread_cond_wait(&g_cond_slot_empty, &g_checkpoint_mutex);
    }
    g_terminate_checkpoint_thread = 1;
    pthread_cond_signal(&g_cond_slot_full);
    pthread_cond_signal(&g_cond_slot_empty);
    pthread_mutex_unlock(&g_checkpoint_mutex);

    pthread_join(checkpoint_thread_id, NULL);

    if (g_async_error_occurred) {
        fprintf(stderr, "Main thread: Asynchronous checkpointing failed. Check logs.\n");
        close(fd); 
        if (tx_fp) fclose(tx_fp);
        free(state);
        free(batch_times);
        exit(EXIT_FAILURE);
    }

    // Calculate performance statistics
    qsort(batch_times, NUMBER_OF_BATCHES, sizeof(double), compare_doubles);

    PerformanceStats stats = {
        .total_time = get_time_ms() - start_total,
        .avg_time = total_processing_time / NUMBER_OF_BATCHES,
        .median_time = batch_times[NUMBER_OF_BATCHES/2],
        .p90_time = batch_times[(int)(NUMBER_OF_BATCHES * 0.9)],
        .p99_time = batch_times[(int)(NUMBER_OF_BATCHES * 0.99)]
    };

    printf("\n=== Performance Summary ===\n");
    printf("Total processing time:  %8.2f ms\n", stats.total_time);
    printf("Average batch time:     %8.2f ms\n", stats.avg_time);
    printf("Median batch time:      %8.2f ms\n", stats.median_time);
    printf("90th percentile:        %8.2f ms\n", stats.p90_time);
    printf("99th percentile:        %8.2f ms\n", stats.p99_time);
    printf("Total throughput:       %8.2f Ktx/s\n", 
           (NUMBER_OF_BATCHES * BATCH_SIZE/1000.0) / (stats.total_time/1000.0));

    // State verification
    uint64_t final_hash = fnv1a_hash(state, ACCOUNTS_PER_STATE_CHUNK);
    printf("\n=== Final State Verification ===\n");
    printf("State hash: 0x%016" PRIx64 "\n", final_hash);
    printf("Saving hash to state_hash.dat\n");
    
    FILE *hash_file = fopen("state_hash.dat", "wb");
    if (hash_file) {
        fwrite(&final_hash, sizeof(uint64_t), 1, hash_file);
        fclose(hash_file);
    }

    close(fd);
    if (tx_fp) fclose(tx_fp);
    free(state);
    free(batch_times);
    return 0;
}