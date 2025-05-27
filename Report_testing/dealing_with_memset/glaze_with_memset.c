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
uint64_t SMALL_ACCOUNT_COUNT = 5000000UL; // Default, can be overridden by argv
#define STATE_CHUNK_SIZE    (512 * 1024)
#define ACCOUNTS_PER_STATE_CHUNK (STATE_CHUNK_SIZE / ACCOUNT_SIZE)

#define INITIAL_WS_CAPACITY 1024

#define RING_SIZE 10 // Explicitly 10, not calculated from account count for this version
#define CHECKPOINT_MAGIC 0xC0FFEE42
#define INVALID_CHECKPOINT_MAGIC 0x0

// These will be calculated in main() based on SMALL_ACCOUNT_COUNT
uint64_t MAX_WRITE_SET_ENTRIES_PER_SLOT_VAR;
size_t MAX_WRITE_SET_BYTES_PER_SLOT_VAR;
size_t CHECKPOINT_SLOT_SIZE_VAR;
size_t TOTAL_LOG_FILE_SIZE_VAR;

#define WRITE_CHUNK_SIZE (64 * 1024 * 1024) // 64MB chunks for looped pwrite

typedef struct {
    uint32_t magic;
    uint32_t batch_num;
    uint32_t write_set_count;
    uint32_t reserved;
} CheckpointHeader;


typedef struct {
    uint64_t address;
    int64_t balance;
} WriteSetEntry;


// Struct for checkpoint data to be passed to the worker thread
typedef struct {
    CheckpointHeader header;
    int64_t* state_snapshot;
    WriteSetEntry* write_set_snapshot;
} AsyncCheckpointData;

// Global variables for asynchronous checkpointing
pthread_t checkpoint_thread_id;
pthread_mutex_t g_checkpoint_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t g_cond_slot_empty = PTHREAD_COND_INITIALIZER;
pthread_cond_t g_cond_slot_full = PTHREAD_COND_INITIALIZER;
AsyncCheckpointData g_checkpoint_slot;
int g_slot_is_full = 0;
int g_terminate_checkpoint_thread = 0;
int g_log_fd;
int g_async_error_occurred = 0;


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
    uint32_t batch_num;
    int64_t *state_chunk_snapshot;
    WriteSetEntry *write_set;
    uint32_t write_set_count;
} CheckpointRecord;


void write_state_to_file(const char *filename, int64_t *state, size_t count) {
    FILE *fp = fopen(filename, "w");
    if (!fp) return;
    for (size_t i = 0; i < count; i++) {
        fprintf(fp, "%zu: %lld\n", i, (long long)state[i]);
    }
    fclose(fp);
}

int compare_checkpoint_record_by_batch_num(const void *a, const void *b) {
    return (int)(((CheckpointRecord*)a)->batch_num - ((CheckpointRecord*)b)->batch_num);
}

int reconstruct_state(int fd, int64_t *state, uint32_t *last_batch_num_recovered) {
    CheckpointRecord *records = malloc(RING_SIZE * sizeof(CheckpointRecord));
    if (!records) {
        perror("Failed to allocate memory for checkpoint records in recovery");
        return -1;
    }
    size_t valid_record_count = 0;
    *last_batch_num_recovered = UINT32_MAX;

    printf("Recovery: Scanning %d slots in ring buffer...\n", RING_SIZE);

    for (uint32_t slot_idx = 0; slot_idx < RING_SIZE; ++slot_idx) {
        off_t slot_base_offset = (off_t)slot_idx * CHECKPOINT_SLOT_SIZE_VAR;
        CheckpointHeader header;

        if (pread(fd, &header, sizeof(header), slot_base_offset) != sizeof(header)) {
            continue;
        }

        if (header.magic != CHECKPOINT_MAGIC) {
            continue;
        }
        
        if (header.write_set_count > MAX_WRITE_SET_ENTRIES_PER_SLOT_VAR) {
            fprintf(stderr, "Recovery: Slot %u (Batch %u) - header.write_set_count (%u) exceeds MAX_WRITE_SET_ENTRIES_PER_SLOT_VAR (%lu). Skipping slot.\n",
                    slot_idx, header.batch_num, header.write_set_count, MAX_WRITE_SET_ENTRIES_PER_SLOT_VAR);
            continue;
        }

        printf("Recovery: Slot %u - Valid header found (Magic: 0x%x, Batch: %u, WS Count: %u)\n",
               slot_idx, header.magic, header.batch_num, header.write_set_count);

        int64_t *state_snapshot = malloc(STATE_CHUNK_SIZE);
        if (!state_snapshot) {
            perror("Recovery: Failed to malloc state_snapshot");
            for(size_t i = 0; i < valid_record_count; ++i) { free(records[i].state_chunk_snapshot); if(records[i].write_set) free(records[i].write_set); }
            free(records);
            return -1;
        }

        off_t state_snapshot_offset = slot_base_offset + sizeof(CheckpointHeader);
        if (pread(fd, state_snapshot, STATE_CHUNK_SIZE, state_snapshot_offset) != STATE_CHUNK_SIZE) {
            fprintf(stderr, "Recovery: Slot %u (Batch %u) - short read for state snapshot. Skipping slot.\n", slot_idx, header.batch_num);
            free(state_snapshot);
            continue;
        }

        WriteSetEntry *ws = NULL;
        if (header.write_set_count > 0) {
            ws = malloc(header.write_set_count * sizeof(WriteSetEntry));
            if (!ws) {
                perror("Recovery: Failed to malloc write_set");
                free(state_snapshot);
                for(size_t i = 0; i < valid_record_count; ++i) { free(records[i].state_chunk_snapshot); if(records[i].write_set) free(records[i].write_set); }
                free(records);
                return -1;
            }
            off_t ws_offset = state_snapshot_offset + STATE_CHUNK_SIZE;
            if (pread(fd, ws, header.write_set_count * sizeof(WriteSetEntry), ws_offset) != (ssize_t)(header.write_set_count * sizeof(WriteSetEntry))) {
                fprintf(stderr, "Recovery: Slot %u (Batch %u) - short read for write set. Skipping slot.\n", slot_idx, header.batch_num);
                free(state_snapshot);
                free(ws);
                continue;
            }
        }
        
        records[valid_record_count++] = (CheckpointRecord){
            header.batch_num, state_snapshot, ws, header.write_set_count
        };
    }

    if (valid_record_count == 0) {
        printf("Recovery: No valid checkpoint records found in any slot.\n");
        free(records);
        return 0;
    }

    printf("Recovery: Found %zu valid checkpoint records. Sorting...\n", valid_record_count);
    qsort(records, valid_record_count, sizeof(CheckpointRecord), compare_checkpoint_record_by_batch_num);

    memcpy(state, records[0].state_chunk_snapshot, STATE_CHUNK_SIZE);
    *last_batch_num_recovered = records[0].batch_num;
    printf("Recovery: Applied base state snapshot from batch %u.\n", *last_batch_num_recovered);
    
    for (size_t i = 0; i < valid_record_count; ++i) {
        if (records[i].batch_num > *last_batch_num_recovered) {
             printf("Recovery: Applying write set from batch %u (WS count: %u) on top of batch %u.\n",
                   records[i].batch_num, records[i].write_set_count, *last_batch_num_recovered);
            for (uint32_t j = 0; j < records[i].write_set_count; ++j) {
                uint64_t addr = records[i].write_set[j].address;
                if (addr < SMALL_ACCOUNT_COUNT) {
                    state[addr] = records[i].write_set[j].balance;
                }
            }
            *last_batch_num_recovered = records[i].batch_num;
        }
    }
    
    printf("Recovery: Final reconstructed state reflects batch %u.\n", *last_batch_num_recovered);

    for (size_t i = 0; i < valid_record_count; ++i) {
        free(records[i].state_chunk_snapshot);
        if (records[i].write_set) free(records[i].write_set);
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

void preallocate_log_file(const char *filename, size_t required_size) {
    int fd = open(filename, O_RDWR | O_CREAT, 0666);
    if (fd < 0) {
        perror("preallocate_log_file: open failed");
        exit(EXIT_FAILURE);
    }
    struct stat st;
    if (fstat(fd, &st) != 0) {
        perror("preallocate_log_file: fstat failed");
        close(fd);
        exit(EXIT_FAILURE);
    }

    if (st.st_size < (off_t)required_size) {
        printf("Preallocating log file %s to %zu bytes (current: %ld bytes).\n", filename, required_size, (long)st.st_size);
        if (ftruncate(fd, required_size) != 0) {
            perror("preallocate_log_file: ftruncate failed attempting to expand");
            errno = 0;
            if (posix_fallocate(fd, 0, required_size) != 0) {
                 if (errno != ENOSPC && errno != EFBIG && errno != EINVAL && errno != EOPNOTSUPP) {
                    perror("preallocate_log_file: posix_fallocate also failed");
                 }
                 if (fstat(fd, &st) == 0 && st.st_size < (off_t)required_size) {
                     fprintf(stderr, "Critical: Failed to resize log file to %zu bytes despite attempts.\n", required_size);
                     close(fd);
                     exit(EXIT_FAILURE);
                 }
            }
        }
        printf("Initializing headers in new/resized log file slots to invalid state.\n");
        CheckpointHeader invalid_header = {INVALID_CHECKPOINT_MAGIC, 0, 0, 0};
        for (uint32_t slot_idx = 0; slot_idx < RING_SIZE; ++slot_idx) {
            off_t slot_base_offset = (off_t)slot_idx * CHECKPOINT_SLOT_SIZE_VAR;
            if (pwrite(fd, &invalid_header, sizeof(CheckpointHeader), slot_base_offset) != (ssize_t)sizeof(CheckpointHeader)) {
                perror("preallocate_log_file: pwrite to initialize header failed");
                close(fd);
                exit(EXIT_FAILURE);
            }
        }
        if (fsync(fd) != 0) {
            perror("preallocate_log_file: fsync after header initialization failed");
        }
        printf("Log file preallocated and headers initialized.\n");
    } else {
         printf("Log file %s already exists with sufficient size (%ld bytes >= %zu bytes).\n", filename, (long)st.st_size, required_size);
    }
    close(fd);
}


int apply_transaction(const Transaction *tx, int64_t *state,
                     WriteSetEntry **ws, uint32_t *ws_count, uint32_t *ws_capacity) {
    uint8_t op_type = GET_OP(tx->sender);
    uint32_t initial_ws_count = *ws_count;

    if (op_type == 1) { // Memset operation
        uint64_t start = GET_DATA(tx->sender);
        uint64_t count = GET_DATA(tx->receiver);
        
        if (*ws_count + count > *ws_capacity) {
            *ws_capacity = (*ws_capacity + count) * 2;
             if (*ws_capacity > MAX_WRITE_SET_ENTRIES_PER_SLOT_VAR) {
                *ws_capacity = MAX_WRITE_SET_ENTRIES_PER_SLOT_VAR;
             }
            WriteSetEntry *new_ws = realloc(*ws, *ws_capacity * sizeof(WriteSetEntry));
            if (!new_ws) {
                perror("Write-set realloc failed for memset");
                exit(EXIT_FAILURE);
            }
            *ws = new_ws;
        }
        
        uint64_t num_to_add = count;
        if (*ws_count + num_to_add > *ws_capacity) {
            num_to_add = *ws_capacity - *ws_count;
        }

        for (uint64_t i = 0; i < num_to_add; i++) {
            uint64_t addr = (start + i); // No modulo, direct addressing if within bounds
            if (addr >= SMALL_ACCOUNT_COUNT) continue;
            
            state[addr] = tx->amount;
            (*ws)[*ws_count] = (WriteSetEntry){addr, state[addr]};
            (*ws_count)++;
        }
    } else { // P2P transfer
        if (*ws_count + 2 > *ws_capacity) {
            *ws_capacity *= 2;
            if (*ws_capacity > MAX_WRITE_SET_ENTRIES_PER_SLOT_VAR) {
                *ws_capacity = MAX_WRITE_SET_ENTRIES_PER_SLOT_VAR;
            }
            WriteSetEntry *new_ws = realloc(*ws, *ws_capacity * sizeof(WriteSetEntry));
            if (!new_ws) {
                perror("Write-set realloc failed for P2P");
                exit(EXIT_FAILURE);
            }
            *ws = new_ws;
        }

        uint64_t sender_addr = GET_DATA(tx->sender);
        uint64_t receiver_addr = GET_DATA(tx->receiver);

        if (*ws_count < *ws_capacity && sender_addr < SMALL_ACCOUNT_COUNT) {
            state[sender_addr] -= tx->amount;
            (*ws)[*ws_count] = (WriteSetEntry){sender_addr, state[sender_addr]};
            (*ws_count)++;
        }
        
        if (*ws_count < *ws_capacity && receiver_addr < SMALL_ACCOUNT_COUNT) {
            state[receiver_addr] += tx->amount;
            (*ws)[*ws_count] = (WriteSetEntry){receiver_addr, state[receiver_addr]};
            (*ws_count)++;
        }
    }
    return *ws_count - initial_ws_count;
}

typedef struct {
    double total_time;
    double avg_time;
    double median_time;
    double p90_time;
    double p99_time;
} PerformanceStats;

static int compare_doubles(const void *a, const void *b) {
    double diff = (*(const double*)a) - (*(const double*)b);
    return (diff < 0) ? -1 : (diff > 0) ? 1 : 0;
}

void* checkpoint_thread_function(void* arg) {
    (void)arg;

    while (1) {
        AsyncCheckpointData local_data_copy;
        int io_error = 0;

        pthread_mutex_lock(&g_checkpoint_mutex);
        while (!g_slot_is_full && !g_terminate_checkpoint_thread) {
            pthread_cond_wait(&g_cond_slot_full, &g_checkpoint_mutex);
        }

        if (g_terminate_checkpoint_thread && !g_slot_is_full) {
            pthread_mutex_unlock(&g_checkpoint_mutex);
            break;
        }

        local_data_copy = g_checkpoint_slot;
        g_slot_is_full = 0;
        pthread_cond_signal(&g_cond_slot_empty);
        pthread_mutex_unlock(&g_checkpoint_mutex);

        uint32_t slot_idx = local_data_copy.header.batch_num % RING_SIZE;
        off_t slot_base_offset = (off_t)slot_idx * CHECKPOINT_SLOT_SIZE_VAR;
        
        off_t current_offset_in_slot = sizeof(CheckpointHeader);

        // 1. Write state snapshot in chunks
        if (!io_error) {
            char *state_ptr = (char*)local_data_copy.state_snapshot;
            size_t remaining_state_bytes = STATE_CHUNK_SIZE;
            off_t state_write_offset = slot_base_offset + current_offset_in_slot;

            while (remaining_state_bytes > 0) {
                size_t bytes_to_write = remaining_state_bytes < WRITE_CHUNK_SIZE ? remaining_state_bytes : WRITE_CHUNK_SIZE;
                ssize_t bytes_written = pwrite(g_log_fd, state_ptr, bytes_to_write, state_write_offset);
                if (bytes_written == -1) {
                    fprintf(stderr, "Async pwrite loop: Error writing state snapshot (batch %u, slot %u). errno: %d (%s)\\n",
                            local_data_copy.header.batch_num, slot_idx, errno, strerror(errno));
                    io_error = 1; break;
                } else if ((size_t)bytes_written != bytes_to_write) {
                    fprintf(stderr, "Async pwrite loop: Partial write for state snapshot (batch %u, slot %u). Wrote %zd of %zu bytes.\\n",
                            local_data_copy.header.batch_num, slot_idx, bytes_written, bytes_to_write);
                    io_error = 1; break;
                }
                state_ptr += bytes_written;
                state_write_offset += bytes_written;
                remaining_state_bytes -= bytes_written;
            }
        }
        if(local_data_copy.state_snapshot) free(local_data_copy.state_snapshot);
        current_offset_in_slot += STATE_CHUNK_SIZE;

        // 2. Write write-set entries in chunks (only if count > 0)
        if (!io_error && local_data_copy.header.write_set_count > 0) {
            char *ws_ptr = (char*)local_data_copy.write_set_snapshot;
            size_t remaining_ws_bytes = local_data_copy.header.write_set_count * sizeof(WriteSetEntry);
            off_t ws_write_offset = slot_base_offset + current_offset_in_slot;
            size_t total_ws_bytes_to_write = remaining_ws_bytes; // For final check

            while (remaining_ws_bytes > 0) {
                size_t bytes_to_write = remaining_ws_bytes < WRITE_CHUNK_SIZE ? remaining_ws_bytes : WRITE_CHUNK_SIZE;
                ssize_t bytes_written = pwrite(g_log_fd, ws_ptr, bytes_to_write, ws_write_offset);
                if (bytes_written == -1) {
                    fprintf(stderr, "Async pwrite loop: Error writing write set (batch %u, slot %u). errno: %d (%s)\\n",
                            local_data_copy.header.batch_num, slot_idx, errno, strerror(errno));
                    io_error = 1; break;
                } else if ((size_t)bytes_written != bytes_to_write) {
                     fprintf(stderr, "Async pwrite loop: Partial write for write set (batch %u, slot %u). Wrote %zd of %zu bytes.\\n",
                            local_data_copy.header.batch_num, slot_idx, bytes_written, bytes_to_write);
                    io_error = 1; break;
                }
                ws_ptr += bytes_written;
                ws_write_offset += bytes_written;
                remaining_ws_bytes -= bytes_written;
            }
            if (!io_error && (ws_write_offset - (slot_base_offset + current_offset_in_slot)) != (off_t)total_ws_bytes_to_write) {
                 fprintf(stderr, "Async pwrite loop: Mismatch in total bytes written for write set (batch %u, slot %u).\\n", local_data_copy.header.batch_num, slot_idx);
                 io_error = 1;
            }
        }
        if(local_data_copy.write_set_snapshot) free(local_data_copy.write_set_snapshot);
        // current_offset_in_slot += local_data_copy.header.write_set_count * sizeof(WriteSetEntry); // Not needed as header is at base
        
        // 3. Write header (LAST, to commit the slot)
        if (!io_error) {
            // Header is written at the beginning of the slot
            ssize_t bytes_written_header = pwrite(g_log_fd, &local_data_copy.header, sizeof(CheckpointHeader), slot_base_offset);
            if (bytes_written_header == -1) {
                fprintf(stderr, "Async pwrite: Error writing header (batch %u, slot %u). errno: %d (%s)\\n",
                        local_data_copy.header.batch_num, slot_idx, errno, strerror(errno));
                io_error = 1;
            } else if ((size_t)bytes_written_header != sizeof(CheckpointHeader)) {
                fprintf(stderr, "Async pwrite: Partial write for header (batch %u, slot %u). Wrote %zd of %zu bytes.\\n",
                        local_data_copy.header.batch_num, slot_idx, bytes_written_header, sizeof(CheckpointHeader));
                io_error = 1;
            }
        }
        
        // 4. Fsync
        if (!io_error) {
            if (fsync(g_log_fd) != 0) {
                fprintf(stderr, "Async: Error fsyncing (batch %u, slot %u). errno: %d (%s)\\n",
                        local_data_copy.header.batch_num, slot_idx, errno, strerror(errno));
                io_error = 1;
            }
        }
        
        if (io_error) {
            fprintf(stderr, "Async checkpoint thread encountered an I/O error for batch %u in slot %u.\n", local_data_copy.header.batch_num, slot_idx);
            pthread_mutex_lock(&g_checkpoint_mutex);
            g_async_error_occurred = 1;
            pthread_cond_signal(&g_cond_slot_empty);
            pthread_mutex_unlock(&g_checkpoint_mutex);
        }
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

    MAX_WRITE_SET_ENTRIES_PER_SLOT_VAR = 100 * SMALL_ACCOUNT_COUNT;
    MAX_WRITE_SET_BYTES_PER_SLOT_VAR = MAX_WRITE_SET_ENTRIES_PER_SLOT_VAR * sizeof(WriteSetEntry);
    CHECKPOINT_SLOT_SIZE_VAR = sizeof(CheckpointHeader) + STATE_CHUNK_SIZE + MAX_WRITE_SET_BYTES_PER_SLOT_VAR;
    TOTAL_LOG_FILE_SIZE_VAR = RING_SIZE * CHECKPOINT_SLOT_SIZE_VAR;

    printf("INFO: Runtime SMALL_ACCOUNT_COUNT set to %lu.\n", SMALL_ACCOUNT_COUNT);
    printf("INFO: Runtime MAX_WRITE_SET_ENTRIES_PER_SLOT_VAR is %lu.\n", MAX_WRITE_SET_ENTRIES_PER_SLOT_VAR);
    printf("INFO: Runtime MAX_WRITE_SET_BYTES_PER_SLOT_VAR is %zu bytes.\n", MAX_WRITE_SET_BYTES_PER_SLOT_VAR);
    printf("INFO: STATE_CHUNK_SIZE is %d bytes.\n", STATE_CHUNK_SIZE);
    printf("INFO: Runtime CHECKPOINT_SLOT_SIZE_VAR is %zu bytes.\n", CHECKPOINT_SLOT_SIZE_VAR);
    printf("INFO: Runtime TOTAL_LOG_FILE_SIZE_VAR is %zu bytes.\n", TOTAL_LOG_FILE_SIZE_VAR);


    int64_t *state = calloc(SMALL_ACCOUNT_COUNT, sizeof(int64_t));
    if (!state) {
        fprintf(stderr, "Failed to allocate state array\n");
        exit(EXIT_FAILURE);
    }
    const char *log_file = "checkpoint_log.dat";
    
    preallocate_log_file(log_file, TOTAL_LOG_FILE_SIZE_VAR);

    g_log_fd = open(log_file, O_RDWR);
    if (g_log_fd < 0) {
        perror("Failed to open log_file after preallocation");
        free(state);
        exit(EXIT_FAILURE);
    }
    
    uint32_t recovered_batch = UINT32_MAX;
    if (reconstruct_state(g_log_fd, state, &recovered_batch) == 0) {
        if (recovered_batch != UINT32_MAX) {
            printf("Recovered state from batch %" PRIu32 "\n", recovered_batch);
            printf("State hash post-recovery: 0x%016" PRIx64 "\n", fnv1a_hash(state, ACCOUNTS_PER_STATE_CHUNK));
        } else {
            printf("No valid checkpoint found to recover from. Starting fresh.\n");
        }
    } else {
        fprintf(stderr, "Recovery process failed. Exiting.\n");
        close(g_log_fd);
        free(state);
        exit(EXIT_FAILURE);
    }


    double *batch_times = malloc(NUMBER_OF_BATCHES * sizeof(double));
    if (!batch_times) {
        fprintf(stderr, "Failed to allocate batch times array\n");
        close(g_log_fd);
        free(state);
        exit(EXIT_FAILURE);
    }
    uint32_t num_batches_processed_this_run = 0;
    double total_processing_time = 0;
    uint64_t start_total = get_time_ms();

    if (pthread_create(&checkpoint_thread_id, NULL, checkpoint_thread_function, NULL) != 0) {
        perror("Failed to create checkpoint thread");
        close(g_log_fd);
        free(state);
        free(batch_times);
        exit(EXIT_FAILURE);
    }


    FILE *tx_fp = fopen("transactions.bin", "rb");
    if (!tx_fp) {
        fprintf(stderr, "Failed to open transactions file\n");
        pthread_mutex_lock(&g_checkpoint_mutex);
        g_terminate_checkpoint_thread = 1;
        g_async_error_occurred = 1;
        pthread_cond_signal(&g_cond_slot_full);
        pthread_cond_signal(&g_cond_slot_empty);
        pthread_mutex_unlock(&g_checkpoint_mutex);
        pthread_join(checkpoint_thread_id, NULL);
        close(g_log_fd);
        free(state);
        if (batch_times) free(batch_times);
        exit(EXIT_FAILURE);
    }

    if (recovered_batch != UINT32_MAX) {
        printf("=== Post-Recovery Details ===\n");
        printf("Starting processing after recovering batch: %" PRIu32 "\n", recovered_batch);
        printf("State hash: 0x%016" PRIx64 "\n", fnv1a_hash(state, ACCOUNTS_PER_STATE_CHUNK));
        printf("=============================\n\n");
    } else {
         printf("=== Starting Fresh (No Recovery) ===\n\n");
    }
    
    uint32_t start_batch_processing = (recovered_batch == UINT32_MAX) ? 0 : recovered_batch + 1;

    for (uint32_t batch = 0; batch < NUMBER_OF_BATCHES; batch++) {
        if (g_async_error_occurred) {
            fprintf(stderr, "Main loop: Detected async error from worker. Halting batch processing.\n");
            break;
        }
        double batch_start = get_time_ms();
        
        uint32_t ws_capacity = INITIAL_WS_CAPACITY;
        WriteSetEntry *write_set = malloc(ws_capacity * sizeof(WriteSetEntry));
        if (!write_set) {
            fprintf(stderr, "Failed to allocate initial write set for batch %u\n", batch);
            pthread_mutex_lock(&g_checkpoint_mutex);
            g_terminate_checkpoint_thread = 1;
            g_async_error_occurred = 1;
            pthread_cond_signal(&g_cond_slot_full);
            pthread_mutex_unlock(&g_checkpoint_mutex);
            break;
        }
        uint32_t ws_count = 0;

        Transaction tx_batch[BATCH_SIZE];
        if (fseek(tx_fp, (long)batch * BATCH_SIZE * sizeof(Transaction), SEEK_SET) != 0) {
            fprintf(stderr, "Error seeking in transactions file for batch %u\n", batch);
            free(write_set);
            pthread_mutex_lock(&g_checkpoint_mutex);
            g_terminate_checkpoint_thread = 1; g_async_error_occurred = 1;
            pthread_cond_signal(&g_cond_slot_full); pthread_mutex_unlock(&g_checkpoint_mutex);
            break;
        }
        size_t read_count = fread(tx_batch, sizeof(Transaction), BATCH_SIZE, tx_fp);
        if (read_count != BATCH_SIZE) {
            fprintf(stderr, "Error reading batch %u: expected %d, got %zu. End of tx file or error.\n",
                    batch, BATCH_SIZE, read_count);
            free(write_set);
             pthread_mutex_lock(&g_checkpoint_mutex);
            g_terminate_checkpoint_thread = 1; g_async_error_occurred = 1;
            pthread_cond_signal(&g_cond_slot_full); pthread_mutex_unlock(&g_checkpoint_mutex);
            break;
        }

        for (int i = 0; i < BATCH_SIZE; i++) {
            apply_transaction(&tx_batch[i], state, &write_set, &ws_count, &ws_capacity);
        }

        AsyncCheckpointData data_for_slot;
        data_for_slot.header.magic = CHECKPOINT_MAGIC;
        data_for_slot.header.batch_num = batch;
        data_for_slot.header.write_set_count = ws_count;
        data_for_slot.header.reserved = 0;

        data_for_slot.state_snapshot = malloc(STATE_CHUNK_SIZE);
        if (!data_for_slot.state_snapshot) {
            perror("Failed to malloc state_snapshot for async checkpoint");
            free(write_set);
            pthread_mutex_lock(&g_checkpoint_mutex);
            g_terminate_checkpoint_thread = 1; g_async_error_occurred = 1;
            pthread_cond_signal(&g_cond_slot_full); pthread_mutex_unlock(&g_checkpoint_mutex);
            break;
        }
        memcpy(data_for_slot.state_snapshot, state, STATE_CHUNK_SIZE);

        if (ws_count > 0) {
            data_for_slot.write_set_snapshot = malloc(ws_count * sizeof(WriteSetEntry));
            if (!data_for_slot.write_set_snapshot) {
                perror("Failed to malloc write_set_snapshot for async checkpoint");
                free(data_for_slot.state_snapshot);
                free(write_set);
                pthread_mutex_lock(&g_checkpoint_mutex);
                g_terminate_checkpoint_thread = 1; g_async_error_occurred = 1;
                pthread_cond_signal(&g_cond_slot_full); pthread_mutex_unlock(&g_checkpoint_mutex);
                break;
            }
            memcpy(data_for_slot.write_set_snapshot, write_set, ws_count * sizeof(WriteSetEntry));
        } else {
            data_for_slot.write_set_snapshot = NULL;
        }
        free(write_set);

        pthread_mutex_lock(&g_checkpoint_mutex);
        while (g_slot_is_full && !g_terminate_checkpoint_thread && !g_async_error_occurred) {
            pthread_cond_wait(&g_cond_slot_empty, &g_checkpoint_mutex);
        }
        
        if (g_async_error_occurred || g_terminate_checkpoint_thread) {
            pthread_mutex_unlock(&g_checkpoint_mutex);
            fprintf(stderr, "Main thread: Async error or termination signal before submitting batch %u. Freeing snapshots.\n", batch);
            free(data_for_slot.state_snapshot);
            if(data_for_slot.write_set_snapshot) free(data_for_slot.write_set_snapshot);
            if (g_async_error_occurred) break;
        } else {
            g_checkpoint_slot = data_for_slot;
            g_slot_is_full = 1;
            pthread_cond_signal(&g_cond_slot_full);
            pthread_mutex_unlock(&g_checkpoint_mutex);
        }
        
        double batch_end = get_time_ms();
        double duration = batch_end - batch_start;
        if (num_batches_processed_this_run < NUMBER_OF_BATCHES) {
            batch_times[num_batches_processed_this_run] = duration;
        }
        num_batches_processed_this_run++;
        total_processing_time += duration;

        printf("[Batch %04u] Processed in %6.2f ms | Write ops: %-6u | Slot: %2u | Throughput: %6.2f Ktx/s\n",
               batch, duration, ws_count, batch % RING_SIZE,
               (BATCH_SIZE/1000.0) / (duration/1000.0));
        
        if (batch >= NUMBER_OF_BATCHES -1) break;
    }

    printf("Main processing loop finished. Shutting down worker thread...\n");
    pthread_mutex_lock(&g_checkpoint_mutex);
    while (g_slot_is_full && !g_async_error_occurred) {
        printf("Main thread: Waiting for the last checkpoint in slot to be picked up by worker...\n");
        pthread_cond_wait(&g_cond_slot_empty, &g_checkpoint_mutex);
    }
    g_terminate_checkpoint_thread = 1;
    pthread_cond_signal(&g_cond_slot_full);
    pthread_cond_signal(&g_cond_slot_empty);
    pthread_mutex_unlock(&g_checkpoint_mutex);

    pthread_join(checkpoint_thread_id, NULL);
    printf("Worker thread joined.\n");

    if (g_async_error_occurred) {
        fprintf(stderr, "Main thread: Asynchronous checkpointing failed. Check logs.\n");
    }

    if (tx_fp) fclose(tx_fp);
    close(g_log_fd);


    PerformanceStats stats = {0};
    stats.total_time = get_time_ms() - start_total;

    if (num_batches_processed_this_run > 0) {
        qsort(batch_times, num_batches_processed_this_run, sizeof(double), compare_doubles);
        stats.avg_time = total_processing_time / num_batches_processed_this_run;
        stats.median_time = batch_times[num_batches_processed_this_run/2];
        
        size_t p90_idx = (size_t)((double)num_batches_processed_this_run * 0.90);
        if (p90_idx >= num_batches_processed_this_run && num_batches_processed_this_run > 0) p90_idx = num_batches_processed_this_run - 1;
        else if (num_batches_processed_this_run == 0) p90_idx = 0; 
        stats.p90_time = (num_batches_processed_this_run > 0) ? batch_times[p90_idx] : 0.0;

        size_t p99_idx = (size_t)((double)num_batches_processed_this_run * 0.99);
        if (p99_idx >= num_batches_processed_this_run && num_batches_processed_this_run > 0) p99_idx = num_batches_processed_this_run - 1;
        else if (num_batches_processed_this_run == 0) p99_idx = 0;
        stats.p99_time = (num_batches_processed_this_run > 0) ? batch_times[p99_idx] : 0.0;
    } else {
        stats.avg_time = 0;
        stats.median_time = 0;
        stats.p90_time = 0;
        stats.p99_time = 0;
    }

    printf("\n=== Performance Summary ===\n");
    printf("Total program time:     %8.2f ms\n", stats.total_time);
    printf("Batch processing time:  %8.2f ms (sum of %u batches)\n", total_processing_time, num_batches_processed_this_run);
    printf("Average batch time:     %8.2f ms\n", stats.avg_time);
    printf("Median batch time:      %8.2f ms\n", stats.median_time);
    printf("90th percentile:        %8.2f ms\n", stats.p90_time);
    printf("99th percentile:        %8.2f ms\n", stats.p99_time);

    if (total_processing_time > 0 && num_batches_processed_this_run > 0) {
        printf("Overall Throughput:     %8.2f Ktx/s (based on sum of batch times)\n",
               (num_batches_processed_this_run * BATCH_SIZE/1000.0) / (total_processing_time/1000.0));
    } else if (num_batches_processed_this_run > 0) {
         printf("Overall Throughput:     N/A (processing time too low or zero)\n");
    }
    else {
        printf("Overall Throughput:     0.00 Ktx/s (No batches processed this run)\n");
    }


    uint64_t final_hash = fnv1a_hash(state, ACCOUNTS_PER_STATE_CHUNK);
    printf("\n=== Final State Verification ===\n");
    printf("Final State hash (first %d accounts): 0x%016" PRIx64 "\n", ACCOUNTS_PER_STATE_CHUNK, final_hash);
    uint32_t last_batch_actually_processed = (num_batches_processed_this_run > 0) ? (start_batch_processing + num_batches_processed_this_run - 1) : recovered_batch;
    if (recovered_batch == UINT32_MAX && num_batches_processed_this_run == 0) { // Case: No recovery, no batches processed
        printf("Saving hash to state_hash.dat (reflects initial zeroed state as no batches were processed or recovered)\n");
    } else {
        printf("Saving hash to state_hash.dat (reflects state after batch %u)\n", last_batch_actually_processed );
    }
    
    FILE *hash_file = fopen("state_hash.dat", "wb");
    if (hash_file) {
        fwrite(&final_hash, sizeof(uint64_t), 1, hash_file);
        fclose(hash_file);
    }

    free(state);
    free(batch_times);
    printf("Execution finished.\n");
    return g_async_error_occurred ? EXIT_FAILURE : EXIT_SUCCESS;
}
