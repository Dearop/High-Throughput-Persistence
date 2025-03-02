#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <time.h>
#include <errno.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <math.h>      // For ceil()
#include <stdbool.h>

// For convenience with older C standards
#ifndef O_LARGEFILE
#define O_LARGEFILE 0
#endif

#define BATCH_SIZE         (1 << 16)   // 65536 transactions per batch
#define INITIAL_BALANCE    1000000UL
#define MAX_ACCOUNTS       2000000UL   // 2 million accounts
#define TOTAL_BATCHES      50          // Process 50 total batches

// Chunked approach
#define CHUNK_SIZE_ACCOUNTS 16384      // ~256 KB if each Account is 16 bytes
#define NUM_CHUNKS          ((MAX_ACCOUNTS + CHUNK_SIZE_ACCOUNTS - 1) / CHUNK_SIZE_ACCOUNTS)

#define CHUNK_LOG_FILE  "chunk_log.bin"
#define TX_FILE         "transactions.bin"

// --------------------------------------------------------------------
// Data structures
// --------------------------------------------------------------------
typedef struct {
    uint64_t sender;
    uint64_t receiver;
    uint32_t amount;
} Transaction;

typedef struct {
    uint64_t address;
    uint64_t balance;
} Account;

typedef struct {
    uint32_t chunk_id;  
    uint32_t reserved;  // for alignment
    Account  accounts[CHUNK_SIZE_ACCOUNTS];
} ChunkEntry;

// --------------------------------------------------------------------
// Globals
// --------------------------------------------------------------------
static Account *g_state = NULL;

// old_offsets[c] = offset of the latest version of chunk c in the file
// or -1 if none
static off_t old_offsets[NUM_CHUNKS];

// For incremental-chunk logic
static uint64_t g_num_chunks_written = 0;  // how many chunk writes so far
static uint64_t g_processed_batches  = 0;  // total transaction batches processed

// --------------------------------------------------------------------
// Time Utility
// --------------------------------------------------------------------
static double timespec_diff_ms(const struct timespec *start, const struct timespec *end) {
    double secs  = (double)(end->tv_sec - start->tv_sec);
    double nsecs = (double)(end->tv_nsec - start->tv_nsec) / 1e9;
    return (secs + nsecs) * 1000.0;
}

static int cmp_doubles(const void *a, const void *b) {
    double da = *(const double *)a;
    double db = *(const double *)b;
    return (da > db) - (da < db);
}

// --------------------------------------------------------------------
// Overwrite old chunk offset with zeros
// --------------------------------------------------------------------
static void invalidate_old_chunk(int fd, off_t old_offset) {
    if (old_offset < 0) {
        return;
    }
    size_t chunk_size = sizeof(ChunkEntry);
    if (lseek(fd, old_offset, SEEK_SET) == (off_t)-1) {
        perror("lseek to old chunk offset");
        exit(EXIT_FAILURE);
    }
    static char zeros[sizeof(ChunkEntry)];
    memset(zeros, 0, chunk_size);
    ssize_t rc = write(fd, zeros, chunk_size);
    if (rc < 0 || (size_t)rc != chunk_size) {
        perror("overwrite old chunk with zeros");
        exit(EXIT_FAILURE);
    }
}

// --------------------------------------------------------------------
// Write a new chunk version
// Steps:
//   1) Overwrite old version with zeros
//   2) Append new version at file end
//   3) fsync
//   4) Update old_offsets
// --------------------------------------------------------------------
static void write_chunk(int fd, uint32_t chunk_id) {
    // 1) If there's an old version, invalidate it
    if (old_offsets[chunk_id] != -1) {
        invalidate_old_chunk(fd, old_offsets[chunk_id]);
    }

    // 2) Prepare new chunk
    ChunkEntry chunk_buf;
    memset(&chunk_buf, 0, sizeof(chunk_buf));
    chunk_buf.chunk_id = chunk_id;

    // Determine which accounts belong in this chunk
    uint64_t start_idx = (uint64_t)chunk_id * CHUNK_SIZE_ACCOUNTS;
    uint64_t end_idx   = start_idx + CHUNK_SIZE_ACCOUNTS;
    if (end_idx > MAX_ACCOUNTS) {
        end_idx = MAX_ACCOUNTS;
    }
    uint64_t count = end_idx - start_idx;

    // Copy from g_state
    memcpy(chunk_buf.accounts, &g_state[start_idx], count * sizeof(Account));

    // 3) append new chunk at file end
    off_t new_offset = lseek(fd, 0, SEEK_END);
    if (new_offset == (off_t)-1) {
        perror("lseek SEEK_END");
        exit(EXIT_FAILURE);
    }

    ssize_t to_write = sizeof(ChunkEntry);
    ssize_t written = write(fd, &chunk_buf, to_write);
    if (written < 0 || (size_t)written != (size_t)to_write) {
        perror("Error writing chunk to chunk_log");
        exit(EXIT_FAILURE);
    }

    // fsync for durability
    if (fsync(fd) != 0) {
        perror("fsync chunk_log");
        exit(EXIT_FAILURE);
    }

    // 4) Update offset table
    old_offsets[chunk_id] = new_offset;

    printf("Wrote chunk_id=%u at offset=%lld.\n", chunk_id, (long long)new_offset);
}

// --------------------------------------------------------------------
// Write the next chunk in a round-robin fashion
// --------------------------------------------------------------------
static void incremental_persist(int fd) {
    uint32_t chunk_id = (uint32_t)(g_num_chunks_written % NUM_CHUNKS);
    write_chunk(fd, chunk_id);
    g_num_chunks_written++;
}

// --------------------------------------------------------------------
// Attempt to recover from existing chunk_log
// Steps:
//   1) For each record (fixed size) in the file, read it
//   2) If not all zeros, parse chunk_id, store offset
//   3) After scanning, for each chunk, read the last offset
//      and apply it to g_state
// --------------------------------------------------------------------
static void recover_chunks_from_file(int fd) {
    // Initialize offset table to -1
    for (uint32_t c = 0; c < NUM_CHUNKS; c++) {
        old_offsets[c] = -1;
    }

    off_t file_size = lseek(fd, 0, SEEK_END);
    if (file_size == (off_t)-1) {
        perror("lseek to get file size");
        exit(EXIT_FAILURE);
    }

    // If file is empty, nothing to recover
    if (file_size == 0) {
        printf("No chunks to recover. File is empty.\n");
        return;
    }

    // Read from start to end in fixed-size increments
    off_t offset = 0;
    size_t chunk_size = sizeof(ChunkEntry);

    // Temporary buffer
    ChunkEntry chunk;
    memset(&chunk, 0, sizeof(chunk));

    while (offset + (off_t)chunk_size <= file_size) {
        // Seek to offset
        if (lseek(fd, offset, SEEK_SET) == (off_t)-1) {
            perror("lseek in recover");
            exit(EXIT_FAILURE);
        }
        // read chunk_size
        ssize_t rc = read(fd, &chunk, chunk_size);
        if (rc < 0) {
            perror("read in recover");
            exit(EXIT_FAILURE);
        }
        if (rc < (ssize_t)chunk_size) {
            // partial read at end => done
            break;
        }

        // Check if chunk is zeroed out
        bool all_zero = true;
        const uint8_t *p = (const uint8_t *)&chunk;
        for (size_t i = 0; i < chunk_size; i++) {
            if (p[i] != 0) {
                all_zero = false;
                break;
            }
        }
        if (!all_zero) {
            // We have a chunk_id. We rely on chunk_id < NUM_CHUNKS check for validity
            if (chunk.chunk_id < NUM_CHUNKS) {
                // Keep track of the offset of the last version of this chunk
                old_offsets[chunk.chunk_id] = offset;
            }
        }

        offset += chunk_size;
    }

    // Now we read the last version of each chunk into memory
    for (uint32_t c = 0; c < NUM_CHUNKS; c++) {
        if (old_offsets[c] == -1) {
            // no chunk => presumably not yet written
            continue;
        }
        // read chunk from old_offsets[c]
        if (lseek(fd, old_offsets[c], SEEK_SET) == (off_t)-1) {
            perror("lseek to read last chunk version");
            exit(EXIT_FAILURE);
        }
        ChunkEntry last_chunk;
        ssize_t rc = read(fd, &last_chunk, chunk_size);
        if (rc < 0 || rc < (ssize_t)chunk_size) {
            // partial read => skip
            fprintf(stderr, "Error reading final chunk for c=%u at offset=%lld\n", c, (long long)old_offsets[c]);
            continue;
        }
        // copy into g_state
        uint64_t start_idx = (uint64_t)c * CHUNK_SIZE_ACCOUNTS;
        uint64_t end_idx   = start_idx + CHUNK_SIZE_ACCOUNTS;
        if (end_idx > MAX_ACCOUNTS) {
            end_idx = MAX_ACCOUNTS;
        }
        uint64_t count = end_idx - start_idx;
        memcpy(&g_state[start_idx], last_chunk.accounts, count * sizeof(Account));
    }
    printf("Recovery complete. Reconstructed the latest chunk versions from file.\n");
}

// --------------------------------------------------------------------
// Opens or creates chunk_log.bin in R/W mode, calls recovery
// --------------------------------------------------------------------
static int open_chunk_log_and_recover(void) {
    // O_RDWR so we can do random writes and read
    int fd = open(CHUNK_LOG_FILE, O_RDWR | O_CREAT, 0644);
    if (fd < 0) {
        perror("open chunk_log.bin");
        exit(EXIT_FAILURE);
    }

    off_t end_pos = lseek(fd, 0, SEEK_END);
    if (end_pos == 0) {
        printf("Created new log file '%s' (empty).\n", CHUNK_LOG_FILE);
        // no previous data => old_offsets remain -1
    } else {
        printf("Opened existing log file '%s' (size=%lld) for recovery.\n",
               CHUNK_LOG_FILE, (long long)end_pos);
        // Recover
        recover_chunks_from_file(fd);
    }
    return fd;
}

// --------------------------------------------------------------------
// Initialize or load g_state, then run main transaction loop
// --------------------------------------------------------------------
int main(void) {
    // Allocate in-memory state
    g_state = (Account *)malloc(MAX_ACCOUNTS * sizeof(Account));
    if (!g_state) {
        perror("malloc g_state");
        exit(EXIT_FAILURE);
    }
    // Start all at zero
    for (uint64_t i = 0; i < MAX_ACCOUNTS; i++) {
        g_state[i].address = i;
        g_state[i].balance = 0;
    }

    // Open chunk_log.bin, recover the last known chunk versions
    int chunk_log_fd = open_chunk_log_and_recover();

    // At this point, g_state has the last version of each chunk from the log
    // But let's also initialize any accounts that have never been written
    // with our "fresh state" default if desired (i.e. if a chunk never existed).
    // For simplicity, let's just do it here:
    for (uint32_t c = 0; c < NUM_CHUNKS; c++) {
        if (old_offsets[c] == -1) {
            // chunk never written => set those accounts to initial
            uint64_t start_idx = (uint64_t)c * CHUNK_SIZE_ACCOUNTS;
            uint64_t end_idx   = start_idx + CHUNK_SIZE_ACCOUNTS;
            if (end_idx > MAX_ACCOUNTS) {
                end_idx = MAX_ACCOUNTS;
            }
            for (uint64_t idx = start_idx; idx < end_idx; idx++) {
                g_state[idx].address = idx;
                g_state[idx].balance = INITIAL_BALANCE;
            }
        }
    }
    printf("In-memory state is now ready. Some chunks recovered, others fresh.\n");

    // Transaction file
    FILE *tx_file = fopen(TX_FILE, "rb");
    if (!tx_file) {
        perror("Error opening transactions file");
        close(chunk_log_fd);
        free(g_state);
        exit(EXIT_FAILURE);
    }

    // We'll track each batch's time
    double *batch_times = (double *)malloc(TOTAL_BATCHES * sizeof(double));
    if (!batch_times) {
        perror("malloc batch_times");
        fclose(tx_file);
        close(chunk_log_fd);
        free(g_state);
        exit(EXIT_FAILURE);
    }

    // Temporary buffer for transactions
    Transaction *batch = (Transaction *)malloc(BATCH_SIZE * sizeof(Transaction));
    if (!batch) {
        perror("malloc batch buffer");
        free(batch_times);
        fclose(tx_file);
        close(chunk_log_fd);
        free(g_state);
        exit(EXIT_FAILURE);
    }

    double total_elapsed_ms = 0.0;

    // Process transactions in batches
    for (uint64_t iteration = 0; iteration < TOTAL_BATCHES; iteration++) {
        struct timespec start, end;
        clock_gettime(CLOCK_MONOTONIC, &start);

        // read up to BATCH_SIZE transactions
        size_t read_count = fread(batch, sizeof(Transaction), BATCH_SIZE, tx_file);
        if (read_count < BATCH_SIZE) {
            if (feof(tx_file)) {
                rewind(tx_file);
                read_count = fread(batch, sizeof(Transaction), BATCH_SIZE, tx_file);
            } else {
                perror("Error reading transaction batch");
                free(batch);
                free(batch_times);
                fclose(tx_file);
                close(chunk_log_fd);
                free(g_state);
                exit(EXIT_FAILURE);
            }
        }

        // apply in-memory
        for (size_t i = 0; i < read_count; i++) {
            Transaction *tx = &batch[i];
            if (g_state[tx->sender].balance >= tx->amount) {
                g_state[tx->sender].balance  -= tx->amount;
                g_state[tx->receiver].balance += tx->amount;
            }
        }

        // incrementally persist a new chunk
        incremental_persist(chunk_log_fd);

        clock_gettime(CLOCK_MONOTONIC, &end);
        double elapsed_ms = timespec_diff_ms(&start, &end);
        batch_times[iteration] = elapsed_ms;
        total_elapsed_ms += elapsed_ms;

        g_processed_batches++;
        printf("Batch %llu of %llu processed in %.3f ms\n",
               (unsigned long long)(iteration + 1),
               (unsigned long long)TOTAL_BATCHES,
               elapsed_ms);
    }

    double avg_ms = total_elapsed_ms / (double)TOTAL_BATCHES;
    printf("\nProcessed %u batches total.\n", (unsigned)TOTAL_BATCHES);
    printf("Total time: %.3f ms\n", total_elapsed_ms);
    printf("Avg batch: %.3f ms\n", avg_ms);

    // Sort batch times for median/p90/p99
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
    if (idx_90 >= (int)TOTAL_BATCHES) idx_90 = TOTAL_BATCHES - 1;
    double p90_ms = batch_times[idx_90];
    int idx_99 = (int)ceil(0.99 * TOTAL_BATCHES) - 1;
    if (idx_99 < 0) idx_99 = 0;
    if (idx_99 >= (int)TOTAL_BATCHES) idx_99 = TOTAL_BATCHES - 1;
    double p99_ms = batch_times[idx_99];

    printf("\nLatency stats for %d batches:\n", TOTAL_BATCHES);
    printf("  Median: %.3f ms\n", median_ms);
    printf("  p90:    %.3f ms\n", p90_ms);
    printf("  p99:    %.3f ms\n", p99_ms);

    // Cleanup
    free(batch);
    free(batch_times);
    fclose(tx_file);
    close(chunk_log_fd);
    free(g_state);

    return 0;
}