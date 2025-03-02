#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdbool.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <time.h>
#include <math.h>
#include <pthread.h>
#include <sys/stat.h>

// ---------------- Configuration ----------------

#define BATCH_SIZE          (1 << 16)    // Process up to 65536 transactions per batch
#define MAX_ACCOUNTS        2000000UL    // 2 million accounts total
#define TOTAL_BATCHES       50           // Process 50 batches per run

#define CHUNK_SIZE_ACCOUNTS 32768        // Accounts stored per chunk
#define NUM_CHUNKS          ((MAX_ACCOUNTS + CHUNK_SIZE_ACCOUNTS - 1) / CHUNK_SIZE_ACCOUNTS)
#define INITIAL_BALANCE     1000000UL    // Initial balance for accounts

#define LOG_FILE            "append_delete_log.bin"
#define TX_FILE             "transactions.bin"

// Trigger automatic compaction if the log file exceeds 50 MB
#define MAX_LOG_SIZE        (500 * 1024 * 1024)

// ---------------- Data Structures ----------------

typedef struct {
    uint64_t sender;
    uint64_t receiver;
    uint32_t amount;
} Transaction;

typedef struct {
    uint64_t address;
    uint64_t balance;
} Account;

#pragma pack(push,1)
typedef struct {
    uint8_t   valid_marker;  // 0 => partial/incomplete, 1 => fully written
    uint32_t  chunk_id;      // Which chunk this entry corresponds to
    uint32_t  tx_count;      // Number of transactions in this batch
    Transaction transactions[BATCH_SIZE];      // Batch transactions
    Account     accounts[CHUNK_SIZE_ACCOUNTS];   // Final state snippet for this chunk
} ChunkEntry;
#pragma pack(pop)

// ---------------- Globals ----------------

static Account *g_state = NULL; // In-memory state array

// For each chunk, the offset of its last version in the log file.
static off_t old_offsets[NUM_CHUNKS];

// Count of chunk writes and batches processed.
static uint64_t g_num_chunk_writes = 0;
static uint64_t g_processed_batches = 0;

// Global file descriptor for the log file.
static int g_log_fd = -1;

// A mutex to coordinate writes and compaction.
static pthread_mutex_t log_mutex = PTHREAD_MUTEX_INITIALIZER;

// ---------------- Time / Stats ----------------

static double timespec_diff_ms(const struct timespec *start, const struct timespec *end) {
    double s = (double)(end->tv_sec - start->tv_sec);
    double ns = (double)(end->tv_nsec - start->tv_nsec)/1e9;
    return (s + ns) * 1000.0;
}

static int cmp_doubles(const void *a, const void *b) {
    double da = *(const double *)a;
    double db = *(const double *)b;
    return (da > db) - (da < db);
}

// ---------------- Writing Chunks Safely ----------------

/**
 * Overwrite an old chunk with zeros.
 */
static void invalidate_old_chunk(int fd, off_t old_off) {
    if (old_off < 0) return;
    static char zeros[sizeof(ChunkEntry)];
    memset(zeros, 0, sizeof(zeros));
    if (lseek(fd, old_off, SEEK_SET) == (off_t)-1) {
        perror("lseek invalidate_old_chunk");
        exit(EXIT_FAILURE);
    }
    ssize_t rc = write(fd, zeros, sizeof(zeros));
    if (rc < 0 || (size_t)rc != sizeof(zeros)) {
        perror("write old chunk zeros");
        exit(EXIT_FAILURE);
    }
}

/**
 * Write a new chunk in two phases:
 *   1. Append the chunk with valid_marker = 0, then fsync.
 *   2. Rewrite it with valid_marker = 1, then fsync.
 * Then invalidate any old version of this chunk.
 */
static void safe_write_chunk(int fd, uint32_t chunk_id, Transaction *batch, size_t tx_count) {
    ChunkEntry new_chunk;
    memset(&new_chunk, 0, sizeof(new_chunk));
    new_chunk.valid_marker = 0; 
    new_chunk.chunk_id     = chunk_id;
    new_chunk.tx_count     = (uint32_t)tx_count;
    if (tx_count > BATCH_SIZE)
        tx_count = BATCH_SIZE;
    memcpy(new_chunk.transactions, batch, tx_count * sizeof(Transaction));

    // Copy the final accounts for this chunk from the in-memory state.
    uint64_t start_idx = (uint64_t)chunk_id * CHUNK_SIZE_ACCOUNTS;
    uint64_t end_idx   = start_idx + CHUNK_SIZE_ACCOUNTS;
    if (end_idx > MAX_ACCOUNTS)
        end_idx = MAX_ACCOUNTS;
    uint64_t count = end_idx - start_idx;
    memcpy(new_chunk.accounts, &g_state[start_idx], count * sizeof(Account));

    off_t old_off = old_offsets[chunk_id];

    // Append new chunk at the end-of-file.
    off_t new_off = lseek(fd, 0, SEEK_END);
    if (new_off == (off_t)-1) {
        perror("lseek SEEK_END");
        exit(EXIT_FAILURE);
    }
    // Phase 1: write with valid_marker=0.
    if (write(fd, &new_chunk, sizeof(new_chunk)) != (ssize_t)sizeof(new_chunk)) {
        perror("write partial chunk");
        exit(EXIT_FAILURE);
    }

    // Phase 2: update valid_marker to 1.
    new_chunk.valid_marker = 1;
    if (lseek(fd, new_off, SEEK_SET) == (off_t)-1) {
        perror("lseek to rewrite chunk valid_marker=1");
        exit(EXIT_FAILURE);
    }
    if (write(fd, &new_chunk, sizeof(new_chunk)) != (ssize_t)sizeof(new_chunk)) {
        perror("write finalize chunk");
        exit(EXIT_FAILURE);
    }
    if (fsync(fd) != 0) {
        perror("fsync finalize chunk");
        exit(EXIT_FAILURE);
    }

    // Invalidate the old chunk.
    if (old_off != -1) {
        invalidate_old_chunk(fd, old_off);
    }
    old_offsets[chunk_id] = new_off;
    printf("Chunk: wrote chunk_id=%u with %zu tx at offset=%lld, old=%lld\n",
           chunk_id, tx_count, (long long)new_off, (long long)old_off);
}

/**
 * Round-robin select a chunk_id and persist the transaction batch.
 */
static void incremental_persist(int fd, Transaction *batch, size_t tx_count) {
    uint32_t chunk_id = (uint32_t)(g_num_chunk_writes % NUM_CHUNKS);
    g_num_chunk_writes++;
    safe_write_chunk(fd, chunk_id, batch, tx_count);
}

// ---------------- Recovery ----------------

/**
 * Parse the log file and reconstruct old_offsets and in-memory state.
 */
static void recover_from_file(int fd) {
    for (uint32_t c = 0; c < NUM_CHUNKS; c++) {
        old_offsets[c] = -1;
    }
    off_t end_pos = lseek(fd, 0, SEEK_END);
    if (end_pos == 0) {
        printf("Log file empty, no chunks to recover.\n");
        return;
    }
    if (end_pos < 0) {
        perror("lseek end");
        exit(EXIT_FAILURE);
    }
    size_t csize = sizeof(ChunkEntry);
    off_t offset = 0;
    while (offset + (off_t)csize <= end_pos) {
        if (lseek(fd, offset, SEEK_SET) == (off_t)-1) {
            perror("lseek recover");
            break;
        }
        ChunkEntry chunk;
        ssize_t rc = read(fd, &chunk, csize);
        if (rc < 0) {
            perror("read chunk in recover");
            break;
        }
        if (rc < (ssize_t)csize) {
            break;
        }
        bool all_zero = true;
        const uint8_t *p = (const uint8_t *)&chunk;
        for (size_t i = 0; i < csize; i++) {
            if (p[i] != 0) {
                all_zero = false;
                break;
            }
        }
        if (!all_zero && chunk.valid_marker == 1 && chunk.chunk_id < NUM_CHUNKS) {
            old_offsets[chunk.chunk_id] = offset;
        }
        offset += csize;
    }
    // Rebuild the in-memory state using the valid chunks.
    for (uint32_t c = 0; c < NUM_CHUNKS; c++) {
        off_t offs = old_offsets[c];
        if (offs < 0)
            continue;
        if (lseek(fd, offs, SEEK_SET) == (off_t)-1)
            continue;
        ChunkEntry chunk;
        if (read(fd, &chunk, csize) < (ssize_t)csize)
            continue;
        uint64_t start_idx = (uint64_t)c * CHUNK_SIZE_ACCOUNTS;
        uint64_t end_idx   = start_idx + CHUNK_SIZE_ACCOUNTS;
        if (end_idx > MAX_ACCOUNTS)
            end_idx = MAX_ACCOUNTS;
        uint64_t count = end_idx - start_idx;
        memcpy(&g_state[start_idx], chunk.accounts, count * sizeof(Account));
    }
    printf("Recovery done. Reconstructed final chunk versions from file.\n");
}

// ---------------- Open Log File and Recover ----------------

static void open_log_file_and_recover(const char *filename) {
    g_log_fd = open(filename, O_RDWR | O_CREAT, 0644);
    if (g_log_fd < 0) {
        perror("open log");
        exit(EXIT_FAILURE);
    }
    off_t sz = lseek(g_log_fd, 0, SEEK_END);
    if (sz == 0) {
        printf("Created new log file '%s'.\n", filename);
    } else {
        printf("Opened existing log file '%s' (size=%lld).\n", filename, (long long)sz);
        recover_from_file(g_log_fd);
    }
}

// ---------------- Log Compaction ----------------

/**
 * Compacts the log file by scanning for valid chunks, writing them sequentially
 * into a temporary file, and atomically replacing the old log file.
 */
static void compact_log_file(const char *log_filename, const char *temp_filename) {
    int fd_old = open(log_filename, O_RDONLY);
    if (fd_old < 0) {
        perror("open log file for compaction");
        exit(EXIT_FAILURE);
    }
    int fd_new = open(temp_filename, O_RDWR | O_CREAT | O_TRUNC, 0644);
    if (fd_new < 0) {
        perror("open temporary file for compaction");
        close(fd_old);
        exit(EXIT_FAILURE);
    }

    off_t valid_offsets[NUM_CHUNKS];
    for (uint32_t i = 0; i < NUM_CHUNKS; i++) {
        valid_offsets[i] = -1;
    }

    off_t file_size = lseek(fd_old, 0, SEEK_END);
    if (file_size < 0) {
        perror("lseek file size in compaction");
        close(fd_old);
        close(fd_new);
        exit(EXIT_FAILURE);
    }
    size_t chunk_size = sizeof(ChunkEntry);
    off_t offset = 0;
    while (offset + chunk_size <= file_size) {
        if (lseek(fd_old, offset, SEEK_SET) == (off_t)-1) {
            perror("lseek in compaction");
            break;
        }
        ChunkEntry chunk;
        ssize_t rc = read(fd_old, &chunk, chunk_size);
        if (rc < (ssize_t)chunk_size)
            break;
        bool all_zero = true;
        const uint8_t *p = (const uint8_t *)&chunk;
        for (size_t i = 0; i < chunk_size; i++) {
            if (p[i] != 0) { all_zero = false; break; }
        }
        if (!all_zero && chunk.valid_marker == 1 && chunk.chunk_id < NUM_CHUNKS) {
            valid_offsets[chunk.chunk_id] = offset;
        }
        offset += chunk_size;
    }

    // Write the valid chunks sequentially to the new file.
    off_t new_file_offset = 0;
    for (uint32_t c = 0; c < NUM_CHUNKS; c++) {
        if (valid_offsets[c] < 0)
            continue;
        if (lseek(fd_old, valid_offsets[c], SEEK_SET) == (off_t)-1) {
            perror("lseek for reading chunk in compaction");
            continue;
        }
        ChunkEntry chunk;
        if (read(fd_old, &chunk, chunk_size) < (ssize_t)chunk_size) {
            perror("read chunk in compaction");
            continue;
        }
        if (lseek(fd_new, new_file_offset, SEEK_SET) == (off_t)-1) {
            perror("lseek for writing chunk in compaction");
            continue;
        }
        if (write(fd_new, &chunk, chunk_size) != (ssize_t)chunk_size) {
            perror("write chunk in compaction");
            continue;
        }
        new_file_offset += chunk_size;
    }
    if (fsync(fd_new) != 0) {
        perror("fsync new compacted file");
    }
    close(fd_old);
    close(fd_new);

    if (rename(temp_filename, log_filename) != 0) {
        perror("rename compacted file to log file");
        exit(EXIT_FAILURE);
    }
    printf("Log compaction completed successfully.\n");
}

// ---------------- Automatic Compaction Check ----------------

/**
 * Check the log file size and trigger compaction if it exceeds MAX_LOG_SIZE.
 * This function assumes that log_mutex is held by the caller.
 */
static void maybe_compact_log(void) {
    struct stat st;
    if (fstat(g_log_fd, &st) != 0) {
        perror("fstat in maybe_compact_log");
        return;
    }
    if ((size_t)st.st_size < MAX_LOG_SIZE)
        return; // No need to compact

    printf("Log file size (%lld bytes) exceeds threshold (%d bytes). Triggering compaction...\n",
           (long long)st.st_size, MAX_LOG_SIZE);

    // Close the current log file.
    close(g_log_fd);

    // Run compaction.
    compact_log_file(LOG_FILE, "temp_log.bin");

    // Reopen the compacted log file.
    g_log_fd = open(LOG_FILE, O_RDWR);
    if (g_log_fd < 0) {
        perror("open log file after compaction");
        exit(EXIT_FAILURE);
    }
    // Rebuild old_offsets from the compacted file.
    recover_from_file(g_log_fd);
}

// ---------------- Main ----------------

int main(void) {
    // 1) Allocate the in-memory state.
    g_state = (Account *)malloc(MAX_ACCOUNTS * sizeof(Account));
    if (!g_state) {
        perror("malloc g_state");
        exit(EXIT_FAILURE);
    }
    memset(g_state, 0, MAX_ACCOUNTS * sizeof(Account));

    // 2) Open the log file and recover state.
    pthread_mutex_lock(&log_mutex);
    open_log_file_and_recover(LOG_FILE);
    pthread_mutex_unlock(&log_mutex);

    // For chunks that were not recovered, initialize accounts.
    for (uint32_t c = 0; c < NUM_CHUNKS; c++) {
        if (old_offsets[c] < 0) {
            uint64_t start_idx = (uint64_t)c * CHUNK_SIZE_ACCOUNTS;
            uint64_t end_idx = start_idx + CHUNK_SIZE_ACCOUNTS;
            if (end_idx > MAX_ACCOUNTS)
                end_idx = MAX_ACCOUNTS;
            for (uint64_t i = start_idx; i < end_idx; i++) {
                g_state[i].address = i;
                g_state[i].balance = INITIAL_BALANCE;
            }
        }
    }
    printf("In-memory state loaded/recovered.\n");

    // 3) Open the transaction file.
    FILE *txf = fopen(TX_FILE, "rb");
    if (!txf) {
        perror("fopen transactions");
        close(g_log_fd);
        free(g_state);
        exit(EXIT_FAILURE);
    }

    double *batch_times = (double *)malloc(TOTAL_BATCHES * sizeof(double));
    if (!batch_times) {
        perror("malloc batch_times");
        fclose(txf);
        close(g_log_fd);
        free(g_state);
        exit(EXIT_FAILURE);
    }
    Transaction *batch_buf = (Transaction *)malloc(BATCH_SIZE * sizeof(Transaction));
    if (!batch_buf) {
        perror("malloc batch_buf");
        free(batch_times);
        fclose(txf);
        close(g_log_fd);
        free(g_state);
        exit(EXIT_FAILURE);
    }

    double total_elapsed_ms = 0.0;
    // 4) Main loop: process batches.
    for (uint64_t iteration = 0; iteration < TOTAL_BATCHES; iteration++) {
        struct timespec start, end;
        clock_gettime(CLOCK_MONOTONIC, &start);

        // Read a batch of transactions.
        size_t read_count = fread(batch_buf, sizeof(Transaction), BATCH_SIZE, txf);
        if (read_count < BATCH_SIZE) {
            if (feof(txf)) {
                rewind(txf);
                read_count = fread(batch_buf, sizeof(Transaction), BATCH_SIZE, txf);
            } else {
                perror("Error reading transactions");
                free(batch_buf); free(batch_times);
                fclose(txf); close(g_log_fd); free(g_state);
                exit(EXIT_FAILURE);
            }
        }

        // Apply the transactions in memory.
        for (size_t i = 0; i < read_count; i++) {
            Transaction *tx = &batch_buf[i];
            if (g_state[tx->sender].balance >= tx->amount) {
                g_state[tx->sender].balance  -= tx->amount;
                g_state[tx->receiver].balance += tx->amount;
            }
        }

        // Lock the log for safe writing and possible compaction.
        pthread_mutex_lock(&log_mutex);
        incremental_persist(g_log_fd, batch_buf, read_count);

        // After persisting, check if the log file size is too big.
        maybe_compact_log();
        pthread_mutex_unlock(&log_mutex);

        clock_gettime(CLOCK_MONOTONIC, &end);
        double ms = timespec_diff_ms(&start, &end);
        batch_times[iteration] = ms;
        total_elapsed_ms += ms;
        g_processed_batches++;
        printf("Batch %llu / %llu processed in %.3f ms\n",
               (unsigned long long)(iteration + 1),
               (unsigned long long)TOTAL_BATCHES, ms);
    }

    // 5) Statistics.
    double avg_ms = total_elapsed_ms / (double)TOTAL_BATCHES;
    printf("\nProcessed %d batches total.\n", (int)TOTAL_BATCHES);
    printf("Total time:  %.3f ms\n", total_elapsed_ms);
    printf("Avg batch:   %.3f ms\n", avg_ms);

    qsort(batch_times, TOTAL_BATCHES, sizeof(double), cmp_doubles);
    double median_ms;
    if (TOTAL_BATCHES % 2 == 0) {
        int mid = (int)(TOTAL_BATCHES / 2);
        median_ms = (batch_times[mid - 1] + batch_times[mid]) / 2.0;
    } else {
        median_ms = batch_times[TOTAL_BATCHES / 2];
    }
    int idx_90 = (int)ceil(0.90 * TOTAL_BATCHES) - 1;
    if (idx_90 < 0) idx_90 = 0;
    if (idx_90 >= (int)TOTAL_BATCHES) idx_90 = (int)TOTAL_BATCHES - 1;
    double p90 = batch_times[idx_90];
    int idx_99 = (int)ceil(0.99 * TOTAL_BATCHES) - 1;
    if (idx_99 < 0) idx_99 = 0;
    if (idx_99 >= (int)TOTAL_BATCHES) idx_99 = (int)TOTAL_BATCHES - 1;
    double p99 = batch_times[idx_99];

    printf("\nLatency stats:\n");
    printf("  Median:  %.3f ms\n", median_ms);
    printf("  p90:     %.3f ms\n", p90);
    printf("  p99:     %.3f ms\n", p99);

    free(batch_buf);
    free(batch_times);
    fclose(txf);
    close(g_log_fd);
    free(g_state);

    return 0;
}