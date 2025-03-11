#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <pthread.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <errno.h>

// Definitions and constants.
#define BATCH_SIZE          (1 << 16)            // 2^16 transactions per batch
#define NUMBER_OF_BATCHES   50                 // Number of batches
#define SMALL_ACCOUNT_COUNT 2000000UL            // Total number of accounts

// Ring log parameters.
#define RING_SIZE         8                      // Number of checkpoint slots in the log.
#define STATE_CHUNK_SIZE  (512 * 1024)           // 512KB state chunk per checkpoint.
#define STATE_CHUNK_COUNT (STATE_CHUNK_SIZE / sizeof(int64_t))  // Number of int64_t elements in the state chunk.
    
// Write-set size is the batch of transactions.
#define WRITE_SET_SIZE    (BATCH_SIZE * sizeof(Transaction))

// A checkpoint slot consists of a header, the state chunk, and the write-set.
#define CHECKPOINT_HEADER_SIZE (sizeof(CheckpointHeader))
#define CHECKPOINT_SLOT_SIZE (CHECKPOINT_HEADER_SIZE + STATE_CHUNK_SIZE + WRITE_SET_SIZE)
#define LOG_FILE          "checkpoint_log.dat"   // Single log file with ring structure.

// Dummy transaction structure.
typedef struct {
    uint64_t sender;
    uint64_t receiver;
    uint32_t amount;
} Transaction;

// Checkpoint header structure.
typedef struct {
    uint32_t batch_num;         // Batch number of this checkpoint.
    uint32_t state_chunk_count; // Should equal STATE_CHUNK_COUNT.
    uint32_t write_set_count;   // Should equal BATCH_SIZE.
    uint32_t reserved;          // Reserved/padding.
} CheckpointHeader;

// FNV-1a 64-bit hash function.
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

// Returns current time in milliseconds.
double get_time_ms() {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec * 1000.0 + ts.tv_nsec / 1e6;
}

// Writes the given state chunk to a text file.
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

// Pre-allocate the log file to the expected size using POSIX calls.
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

// ----------------------
// Structures for parallel commit
// ----------------------

// Each commit job contains the batch number, a copy of the state chunk, and pointer to the transaction batch.
typedef struct {
    uint32_t batch_num;
    int64_t *state_snapshot;  // Should have STATE_CHUNK_COUNT elements.
    Transaction *transactions; // Pointer to the (constant) transaction batch.
} CommitJob;

// A simple thread-safe queue for commit jobs.
typedef struct {
    CommitJob **jobs;
    int capacity;
    int size;
    int front;
    int rear;
    pthread_mutex_t mutex;
    pthread_cond_t cond;
    int done; // Flag indicating no more jobs will be enqueued.
} CommitQueue;

// Initialize the commit queue.
void init_commit_queue(CommitQueue *queue, int capacity) {
    queue->jobs = malloc(sizeof(CommitJob*) * capacity);
    if (!queue->jobs) {
        perror("Failed to allocate commit queue");
        exit(EXIT_FAILURE);
    }
    queue->capacity = capacity;
    queue->size = 0;
    queue->front = 0;
    queue->rear = 0;
    queue->done = 0;
    pthread_mutex_init(&queue->mutex, NULL);
    pthread_cond_init(&queue->cond, NULL);
}

// Enqueue a commit job.
void enqueue_job(CommitQueue *queue, CommitJob *job) {
    pthread_mutex_lock(&queue->mutex);
    if (queue->size == queue->capacity) {
        // For simplicity, exit if the queue is full.
        fprintf(stderr, "Commit queue full, exiting.\n");
        exit(EXIT_FAILURE);
    }
    queue->jobs[queue->rear] = job;
    queue->rear = (queue->rear + 1) % queue->capacity;
    queue->size++;
    pthread_cond_signal(&queue->cond);
    pthread_mutex_unlock(&queue->mutex);
}

// Dequeue a commit job. Returns NULL if queue is empty.
CommitJob* dequeue_job(CommitQueue *queue) {
    CommitJob *job = NULL;
    if (queue->size > 0) {
        job = queue->jobs[queue->front];
        queue->front = (queue->front + 1) % queue->capacity;
        queue->size--;
    }
    return job;
}

// Destroy the commit queue.
void destroy_commit_queue(CommitQueue *queue) {
    free(queue->jobs);
    pthread_mutex_destroy(&queue->mutex);
    pthread_cond_destroy(&queue->cond);
}

// ----------------------
// Commit thread function.
// ----------------------
void* commit_thread_func(void* arg) {
    CommitQueue* queue = (CommitQueue*) arg;
    int fd = open(LOG_FILE, O_RDWR);
    if (fd < 0) {
         perror("Error opening log file in commit thread");
         pthread_exit(NULL);
    }
    
    while (1) {
         pthread_mutex_lock(&queue->mutex);
         while (queue->size == 0 && !queue->done) {
              pthread_cond_wait(&queue->cond, &queue->mutex);
         }
         if (queue->size == 0 && queue->done) {
              pthread_mutex_unlock(&queue->mutex);
              break;
         }
         CommitJob *job = dequeue_job(queue);
         pthread_mutex_unlock(&queue->mutex);
         
         if (job) {
             // Compute ring slot offset.
             uint32_t slot_index = job->batch_num % RING_SIZE;
             off_t offset = slot_index * CHECKPOINT_SLOT_SIZE;
             // Prepare checkpoint header.
             CheckpointHeader header;
             header.batch_num = job->batch_num;
             header.state_chunk_count = STATE_CHUNK_COUNT;
             header.write_set_count = BATCH_SIZE;
             header.reserved = 0;
             
             ssize_t bytes_written;
             // Write header.
             bytes_written = pwrite(fd, &header, sizeof(header), offset);
             if (bytes_written != sizeof(header)) {
                perror("Error writing header in commit thread");
             }
             // Write state snapshot.
             bytes_written = pwrite(fd, job->state_snapshot, sizeof(int64_t) * STATE_CHUNK_COUNT, offset + sizeof(header));
             if (bytes_written != sizeof(int64_t) * STATE_CHUNK_COUNT) {
                perror("Error writing state snapshot in commit thread");
             }
             // Write transactions (the write-set).
             bytes_written = pwrite(fd, job->transactions, sizeof(Transaction) * BATCH_SIZE,
                                    offset + sizeof(header) + STATE_CHUNK_SIZE);
             if (bytes_written != sizeof(Transaction) * BATCH_SIZE) {
                perror("Error writing transactions in commit thread");
             }
             fsync(fd);
             free(job->state_snapshot);
             free(job);
         }
    }
    close(fd);
    pthread_exit(NULL);
}

// ----------------------
// For performance metrics: compare two doubles.
int compare_doubles(const void *a, const void *b) {
    double diff = (*(double *)a) - (*(double *)b);
    return (diff < 0) ? -1 : (diff > 0) ? 1 : 0;
}

// ----------------------
// Reconstruction function (unchanged from original).
// ----------------------
int reconstruct_state(int fd, int64_t *state, int *last_batch) {
    uint32_t latest_batch = 0;
    int latest_slot = -1;
    CheckpointHeader header;
    
    // Scan all slots in the ring.
    for (uint32_t i = 0; i < RING_SIZE; i++) {
        off_t offset = i * CHECKPOINT_SLOT_SIZE;
        ssize_t bytes = pread(fd, &header, sizeof(header), offset);
        if (bytes != sizeof(header))
            continue;
        if (header.write_set_count == BATCH_SIZE && header.state_chunk_count == STATE_CHUNK_COUNT) {
            if (header.batch_num >= latest_batch) {
                latest_batch = header.batch_num;
                latest_slot = i;
            }
        }
    }
    if (latest_slot == -1) {
        printf("No valid checkpoint found in log.\n");
        return -1;
    }
    off_t offset = latest_slot * CHECKPOINT_SLOT_SIZE + sizeof(CheckpointHeader);
    ssize_t read_bytes = pread(fd, state, sizeof(int64_t) * STATE_CHUNK_COUNT, offset);
    if (read_bytes != sizeof(int64_t) * STATE_CHUNK_COUNT) {
        perror("Error reading state chunk during reconstruction");
        return -1;
    }
    *last_batch = latest_batch;
    return 0;
}

// ----------------------
// Main function.
// ----------------------
int main(int argc, char **argv) {
    // Allocate the state array.
    int64_t *state = calloc(SMALL_ACCOUNT_COUNT + 1, sizeof(int64_t));
    if (!state) {
        perror("Error allocating state");
        exit(EXIT_FAILURE);
    }
    
    // If an old log file exists, attempt to reconstruct the state.
    int recovered_batch = -1;
    int log_fd = open(LOG_FILE, O_RDWR);
    if (log_fd >= 0) {
        if (reconstruct_state(log_fd, state, &recovered_batch) == 0) {
            printf("Reconstructed state from log up to batch %d.\n", recovered_batch);
            uint64_t hash = fnv1a_hash(state, STATE_CHUNK_COUNT);
            FILE *hash_fp = fopen("state_hash.dat", "rb");
            if (hash_fp) {
                uint64_t stored_hash;
                fread(&stored_hash, sizeof(uint64_t), 1, hash_fp);
                fclose(hash_fp);
                if (hash == stored_hash) {
                    printf("Reconstructed state hash matches stored hash: %llu\n", hash);
                } else {
                    printf("Reconstructed state hash mismatch! Computed: %llu, Stored: %llu\n", hash, stored_hash);
                    write_state_to_file("reconstructed_state.txt", state, STATE_CHUNK_COUNT);
                }
            }
        }
        close(log_fd);
    }
    
    // If running in "recover" mode, exit after reconstruction.
    if (argc > 1 && strcmp(argv[1], "recover") == 0) {
        free(state);
        return 0;
    }
    
    // Pre-allocate the log file (if not already large enough).
    preallocate_log_file_posix(LOG_FILE);
    
    // Allocate memory for a dummy transaction batch.
    Transaction *batchTransactions = malloc(BATCH_SIZE * sizeof(Transaction));
    if (!batchTransactions) {
        perror("Error allocating memory for transaction batch");
        free(state);
        exit(EXIT_FAILURE);
    }
    // Fill the dummy transaction batch with sample values.
    for (size_t i = 0; i < BATCH_SIZE; i++) {
        batchTransactions[i].sender = i % (SMALL_ACCOUNT_COUNT + 1);
        batchTransactions[i].receiver = (i + 1) % (SMALL_ACCOUNT_COUNT + 1);
        batchTransactions[i].amount = 1;
    }
    
    // Initialize commit queue and create commit thread.
    CommitQueue commit_queue;
    // For simplicity, let the queue capacity be large enough to hold all jobs.
    init_commit_queue(&commit_queue, NUMBER_OF_BATCHES);
    
    pthread_t commit_thread;
    if (pthread_create(&commit_thread, NULL, commit_thread_func, &commit_queue) != 0) {
        perror("Error creating commit thread");
        free(state);
        free(batchTransactions);
        exit(EXIT_FAILURE);
    }
    
    // Allocate an array to collect measured processing times for all batches.
    double *batch_times = malloc(NUMBER_OF_BATCHES * sizeof(double));
    if (!batch_times) {
        perror("Error allocating batch_times");
        free(state);
        free(batchTransactions);
        exit(EXIT_FAILURE);
    }
    double total_processing_time = 0.0;
    
    // Start processing loop.
    for (unsigned int batch_num = 0; batch_num < NUMBER_OF_BATCHES; batch_num++) {
        double start_time = get_time_ms();
        
        // Dummy update: add BATCH_SIZE to account 0.
        state[0] += BATCH_SIZE;
        
        // Create a snapshot of the state chunk.
        int64_t *state_snapshot = malloc(STATE_CHUNK_SIZE);
        if (!state_snapshot) {
            perror("Error allocating state snapshot");
            exit(EXIT_FAILURE);
        }
        memcpy(state_snapshot, state, STATE_CHUNK_SIZE);
        
        // Create a commit job.
        CommitJob *job = malloc(sizeof(CommitJob));
        if (!job) {
            perror("Error allocating commit job");
            exit(EXIT_FAILURE);
        }
        job->batch_num = batch_num;
        job->state_snapshot = state_snapshot;
        job->transactions = batchTransactions;  // Constant dummy batch.
        
        // Enqueue the commit job.
        enqueue_job(&commit_queue, job);
        
        double end_time = get_time_ms();
        double batch_duration = end_time - start_time;  // Processing time for this batch.
        batch_times[batch_num] = batch_duration;
        total_processing_time += batch_duration;
        
        printf("Batch %u processed in %.3f ms.\n", batch_num, batch_duration);
    }
    
    // Signal commit thread that no more jobs will be enqueued.
    pthread_mutex_lock(&commit_queue.mutex);
    commit_queue.done = 1;
    pthread_cond_signal(&commit_queue.cond);
    pthread_mutex_unlock(&commit_queue.mutex);
    
    // Wait for commit thread to finish.
    pthread_join(commit_thread, NULL);
    
    // Compute performance metrics.
    double average = total_processing_time / NUMBER_OF_BATCHES;
    double *sorted_times = malloc(NUMBER_OF_BATCHES * sizeof(double));
    if (!sorted_times) {
        perror("Error allocating sorted_times");
        free(state);
        free(batchTransactions);
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
    
    // Compute and print final state chunk hash.
    uint64_t state_hash = fnv1a_hash(state, STATE_CHUNK_COUNT);
    printf("Final state chunk hash: %llu\n", state_hash);
    FILE *hash_fp = fopen("state_hash.dat", "wb");
    if (hash_fp) {
        fwrite(&state_hash, sizeof(uint64_t), 1, hash_fp);
        fclose(hash_fp);
    } else {
        perror("Error opening state_hash.dat for writing");
    }
    
    // Clean up.
    free(state);
    free(batchTransactions);
    free(batch_times);
    free(sorted_times);
    destroy_commit_queue(&commit_queue);
    
    return 0;
}