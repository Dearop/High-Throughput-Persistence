#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <errno.h>

#define BATCH_SIZE          (1 << 16)
#define NUMBER_OF_BATCHES   5000
#define SMALL_ACCOUNT_COUNT 20000000UL
#define RING_SIZE           8
#define STATE_CHUNK_SIZE    (512 * 1024)  // 512KB state chunks
#define STATE_CHUNK_COUNT   (STATE_CHUNK_SIZE / sizeof(int64_t))
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
        if (pread(fd, &header, sizeof(header), read_pos) != sizeof(header)) break;
        read_pos += sizeof(header);

        // Read state chunk
        int64_t *state_chunk = malloc(STATE_CHUNK_SIZE);
        pread(fd, state_chunk, STATE_CHUNK_SIZE, read_pos);
        read_pos += STATE_CHUNK_SIZE;

        // Read write set
        WriteSetEntry *ws = malloc(header.write_set_count * sizeof(WriteSetEntry));
        pread(fd, ws, header.write_set_count * sizeof(WriteSetEntry), read_pos);
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
            if (addr < STATE_CHUNK_COUNT)
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

int main(int argc, char **argv) {
    int64_t *state = calloc(SMALL_ACCOUNT_COUNT, sizeof(int64_t));
    const char *log_file = "checkpoint_log.dat";
    int fd = open(log_file, O_RDWR | O_CREAT, 0666);
    
    // Recovery logic
    int recovered_batch = -1;
    int log_fd = open(log_file, O_RDWR);
    if (log_fd >= 0) {
        if (reconstruct_state(log_fd, state, &recovered_batch) == 0) {
            printf("Recovered state from batch %d\n", recovered_batch);
            // Optional: Add hash verification like in state_management.c
        }
        close(log_fd);
    }

    // Performance tracking
    double *batch_times = malloc(NUMBER_OF_BATCHES * sizeof(double));
    double total_processing_time = 0;
    uint64_t start_total = get_time_ms();

    // Enhanced recovery reporting
    if (recovered_batch != -1) {
        printf("=== Recovery Details ===\n");
        printf("Recovered state from batch: %d\n", recovered_batch);
        printf("State hash: 0x%016llx\n", fnv1a_hash(state, STATE_CHUNK_COUNT));
        printf("========================\n\n");
    }

    for (uint32_t batch = (recovered_batch == -1) ? 0 : recovered_batch+1; 
         batch < NUMBER_OF_BATCHES; 
         batch++) {
        double batch_start = get_time_ms();
        
        // Initialize write-set with dynamic capacity
        uint32_t ws_capacity = INITIAL_WS_CAPACITY;
        WriteSetEntry *write_set = malloc(ws_capacity * sizeof(WriteSetEntry));
        uint32_t ws_count = 0;

        // Read batch
        Transaction tx_batch[BATCH_SIZE];
        FILE *fp = fopen("transactions.bin", "rb");
        fseek(fp, batch * BATCH_SIZE * sizeof(Transaction), SEEK_SET);
        fread(tx_batch, sizeof(Transaction), BATCH_SIZE, fp);
        fclose(fp);

        // Process transactions
        for (int i = 0; i < BATCH_SIZE; i++) {
            apply_transaction(&tx_batch[i], state, &write_set, &ws_count, &ws_capacity);
        }

        // Persist checkpoint
        CheckpointHeader header = {
            .batch_num = batch,
            .state_chunk_count = STATE_CHUNK_COUNT,
            .write_set_count = ws_count
        };
        
        // Write to log (sequential append style)
        // 1. Write header
        write(fd, &header, sizeof(header));
        // 2. Write state chunk
        write(fd, state, STATE_CHUNK_SIZE);
        // 3. Write write-set entries
        write(fd, write_set, ws_count * sizeof(WriteSetEntry));
        
        fsync(fd);
        free(write_set);
        
        double batch_end = get_time_ms();
        double duration = batch_end - batch_start;
        batch_times[batch] = duration;
        total_processing_time += duration;

        printf("[Batch %04u] Processed in %6.2f ms | Write ops: %-6u | Throughput: %6.2f Ktx/s\n",
               batch, duration, ws_count,
               (BATCH_SIZE/1000.0) / (duration/1000.0));
    }

    // Calculate performance statistics
    qsort(batch_times, NUMBER_OF_BATCHES, sizeof(double), 
          (int (*)(const void*, const void*))(
              ^(const void *a, const void *b) {
                  double diff = (*(double*)a) - (*(double*)b);
                  return (diff < 0) ? -1 : (diff > 0) ? 1 : 0;
              }));

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
    uint64_t final_hash = fnv1a_hash(state, STATE_CHUNK_COUNT);
    printf("\n=== Final State Verification ===\n");
    printf("State hash: 0x%016llx\n", final_hash);
    printf("Saving hash to state_hash.dat\n");
    
    FILE *hash_file = fopen("state_hash.dat", "wb");
    if (hash_file) {
        fwrite(&final_hash, sizeof(uint64_t), 1, hash_file);
        fclose(hash_file);
    }

    close(fd);
    free(state);
    free(batch_times);
    return 0;
}