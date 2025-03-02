#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <time.h>
#include <errno.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <math.h>
#include <stdbool.h>

#ifndef O_LARGEFILE
#define O_LARGEFILE 0
#endif

#define BATCH_SIZE         (1 << 16)   // 65536 transactions per batch
#define INITIAL_BALANCE    1000000UL
#define MAX_ACCOUNTS       2000000UL   // 2 million accounts
#define TOTAL_BATCHES      50          // Process 50 total batches

// Chunk approach
#define CHUNK_SIZE_ACCOUNTS 16384      // ~256 KB if each Account is 16 bytes
#define NUM_CHUNKS          ((MAX_ACCOUNTS + CHUNK_SIZE_ACCOUNTS - 1) / CHUNK_SIZE_ACCOUNTS)

#define CHUNK_LOG_FILE  "chunk_log.bin"
#define TX_FILE         "transactions.bin"

typedef struct {
    uint64_t sender;
    uint64_t receiver;
    uint32_t amount;
} Transaction;

typedef struct {
    uint64_t address;
    uint64_t balance;
} Account;

/**
 * Each chunk entry has:
 *  - chunk_id
 *  - tx_count: how many transactions in 'transactions'
 *  - transactions: up to BATCH_SIZE of them
 *  - accounts: the final chunk of accounts after applying transactions
 */
typedef struct {
    uint32_t chunk_id;
    uint32_t tx_count;  
    Transaction transactions[BATCH_SIZE];
    Account  accounts[CHUNK_SIZE_ACCOUNTS];
} ChunkEntry;

// --------------------------------------------------------------------
// Globals
// --------------------------------------------------------------------
static Account *g_state = NULL;

// We'll track the offset of the last version of each chunk
static off_t old_offsets[NUM_CHUNKS];

// For incremental-chunk logic
static uint64_t g_num_chunks_written = 0;
static uint64_t g_processed_batches  = 0;

// --------------------------------------------------------------------
// Utility
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

// Overwrite old chunk offset with zeros (only after new chunk is safely written).
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
// Safe chunk write
//   1) Append new chunk first
//   2) fsync
//   3) THEN invalidate old chunk
//   4) Update old_offsets
// --------------------------------------------------------------------
static void write_chunk(int fd, uint32_t chunk_id,
                        Transaction *batch, size_t tx_count)
{
    // We'll remember the old offset but NOT immediately overwrite it
    off_t old_offset = old_offsets[chunk_id];

    // Prepare new chunk
    ChunkEntry chunk_buf;
    memset(&chunk_buf, 0, sizeof(chunk_buf));
    chunk_buf.chunk_id = chunk_id;
    chunk_buf.tx_count = (uint32_t)tx_count;

    if (tx_count > BATCH_SIZE) {
        tx_count = BATCH_SIZE; // safety clamp
    }
    // copy the transactions
    memcpy(chunk_buf.transactions, batch, tx_count * sizeof(Transaction));

    // copy final accounts for this chunk
    uint64_t start_idx = (uint64_t)chunk_id * CHUNK_SIZE_ACCOUNTS;
    uint64_t end_idx   = start_idx + CHUNK_SIZE_ACCOUNTS;
    if (end_idx > MAX_ACCOUNTS) {
        end_idx = MAX_ACCOUNTS;
    }
    uint64_t count = end_idx - start_idx;
    memcpy(chunk_buf.accounts, &g_state[start_idx], count * sizeof(Account));

    // 1) Append to file
    off_t new_offset = lseek(fd, 0, SEEK_END);
    if (new_offset == (off_t)-1) {
        perror("lseek SEEK_END");
        exit(EXIT_FAILURE);
    }

    size_t chunk_size = sizeof(ChunkEntry);
    ssize_t written = write(fd, &chunk_buf, chunk_size);
    if (written < 0 || (size_t)written != chunk_size) {
        perror("Error writing chunk (transactions + accounts) to chunk_log");
        exit(EXIT_FAILURE);
    }

    // 2) fsync new chunk
    if (fsync(fd) != 0) {
        perror("fsync new chunk");
        exit(EXIT_FAILURE);
    }

    // Now we have a successfully written chunk at new_offset.
    // 3) Invalidate old chunk (only if it existed).
    if (old_offset != -1) {
        invalidate_old_chunk(fd, old_offset);
    }

    // 4) Update offset table
    old_offsets[chunk_id] = new_offset;

    printf("Wrote chunk_id=%u with %zu tx at offset=%lld, old offset was=%lld.\n",
           chunk_id, tx_count, (long long)new_offset, (long long)old_offset);
}

// Incrementally pick chunk_id, write the chunk
static void incremental_persist(int fd, Transaction *batch, size_t tx_count) {
    uint32_t chunk_id = (uint32_t)(g_num_chunks_written % NUM_CHUNKS);
    write_chunk(fd, chunk_id, batch, tx_count);
    g_num_chunks_written++;
}

// --------------------------------------------------------------------
// Recovery
// --------------------------------------------------------------------
static void recover_chunks_from_file(int fd) {
    for (uint32_t c = 0; c < NUM_CHUNKS; c++) {
        old_offsets[c] = -1;
    }

    off_t file_size = lseek(fd, 0, SEEK_END);
    if (file_size <= 0) {
        printf("No chunks to recover (file empty or error).\n");
        return;
    }
    size_t chunk_size = sizeof(ChunkEntry);
    off_t offset = 0;
    while (offset + (off_t)chunk_size <= file_size) {
        if (lseek(fd, offset, SEEK_SET) == (off_t)-1) {
            perror("lseek in recover");
            break;
        }
        ChunkEntry chunk;
        memset(&chunk, 0, sizeof(chunk));
        ssize_t rc = read(fd, &chunk, chunk_size);
        if (rc < 0) {
            perror("read in recover");
            break;
        }
        if (rc < (ssize_t)chunk_size) {
            // partial => done
            break;
        }
        // check if zero
        bool all_zero = true;
        const uint8_t *p = (const uint8_t *)&chunk;
        for (size_t i = 0; i < chunk_size; i++) {
            if (p[i] != 0) {
                all_zero = false;
                break;
            }
        }
        if (!all_zero) {
            if (chunk.chunk_id < NUM_CHUNKS) {
                old_offsets[chunk.chunk_id] = offset;
            }
        }
        offset += chunk_size;
    }

    // read back each chunk's final version
    for (uint32_t c = 0; c < NUM_CHUNKS; c++) {
        off_t offs = old_offsets[c];
        if (offs == -1) continue;
        if (lseek(fd, offs, SEEK_SET) == (off_t)-1) continue;
        ChunkEntry chunk;
        ssize_t rc = read(fd, &chunk, chunk_size);
        if (rc < (ssize_t)chunk_size) continue;

        // apply to g_state
        uint64_t start_idx = (uint64_t)c * CHUNK_SIZE_ACCOUNTS;
        uint64_t end_idx   = start_idx + CHUNK_SIZE_ACCOUNTS;
        if (end_idx > MAX_ACCOUNTS) {
            end_idx = MAX_ACCOUNTS;
        }
        uint64_t count = end_idx - start_idx;
        memcpy(&g_state[start_idx], chunk.accounts, count * sizeof(Account));
    }

    printf("Recovery complete. Reconstructed the latest chunk versions.\n");
}

// open chunk log, recover
static int open_chunk_log_and_recover(void) {
    int fd = open(CHUNK_LOG_FILE, O_RDWR | O_CREAT, 0644);
    if (fd < 0) {
        perror("open chunk_log.bin");
        exit(EXIT_FAILURE);
    }
    off_t sz = lseek(fd, 0, SEEK_END);
    if (sz == 0) {
        printf("Created new log file '%s'.\n", CHUNK_LOG_FILE);
    } else {
        printf("Opened existing log file '%s' (size=%lld).\n",
               CHUNK_LOG_FILE, (long long)sz);
        recover_chunks_from_file(fd);
    }
    return fd;
}

// --------------------------------------------------------------------
// MAIN
// --------------------------------------------------------------------
int main(void) {
    // 1) Allocate big array
    g_state = (Account *)malloc(MAX_ACCOUNTS * sizeof(Account));
    if (!g_state) {
        perror("malloc g_state");
        exit(EXIT_FAILURE);
    }
    memset(g_state, 0, MAX_ACCOUNTS * sizeof(Account));

    // 2) open + recover
    int chunk_log_fd = open_chunk_log_and_recover();

    // 3) For any chunk that doesn't exist, init those accounts
    for (uint32_t c = 0; c < NUM_CHUNKS; c++) {
        if (old_offsets[c] == -1) {
            uint64_t start_idx = (uint64_t)c * CHUNK_SIZE_ACCOUNTS;
            uint64_t end_idx   = start_idx + CHUNK_SIZE_ACCOUNTS;
            if (end_idx > MAX_ACCOUNTS) end_idx = MAX_ACCOUNTS;
            for (uint64_t idx = start_idx; idx < end_idx; idx++) {
                g_state[idx].address = idx;
                g_state[idx].balance = INITIAL_BALANCE;
            }
        }
    }
    printf("State loaded. Some chunks recovered, others new.\n");

    // 4) open TX file
    FILE *tx_file = fopen(TX_FILE, "rb");
    if (!tx_file) {
        perror("tx_file");
        close(chunk_log_fd);
        free(g_state);
        exit(EXIT_FAILURE);
    }

    // 5) for stats
    double *batch_times = (double*)malloc(TOTAL_BATCHES * sizeof(double));
    if (!batch_times) {
        perror("batch_times");
        fclose(tx_file);
        close(chunk_log_fd);
        free(g_state);
        exit(EXIT_FAILURE);
    }
    Transaction *batch_buf = (Transaction*)malloc(BATCH_SIZE * sizeof(Transaction));
    if (!batch_buf) {
        perror("batch_buf");
        free(batch_times);
        fclose(tx_file);
        close(chunk_log_fd);
        free(g_state);
        exit(EXIT_FAILURE);
    }

    double total_elapsed_ms = 0.0;

    // 6) main loop
    for (uint64_t iteration = 0; iteration < TOTAL_BATCHES; iteration++) {
        struct timespec start, end;
        clock_gettime(CLOCK_MONOTONIC, &start);

        // read transactions
        size_t read_count = fread(batch_buf, sizeof(Transaction), BATCH_SIZE, tx_file);
        if (read_count < BATCH_SIZE) {
            if (feof(tx_file)) {
                rewind(tx_file);
                read_count = fread(batch_buf, sizeof(Transaction), BATCH_SIZE, tx_file);
            } else {
                perror("Error reading batch");
                free(batch_buf);
                free(batch_times);
                fclose(tx_file);
                close(chunk_log_fd);
                free(g_state);
                exit(EXIT_FAILURE);
            }
        }

        // apply
        for (size_t i = 0; i < read_count; i++) {
            Transaction *tx = &batch_buf[i];
            if (g_state[tx->sender].balance >= tx->amount) {
                g_state[tx->sender].balance -= tx->amount;
                g_state[tx->receiver].balance += tx->amount;
            }
        }

        // persist
        incremental_persist(chunk_log_fd, batch_buf, read_count);

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

    // median/p90/p99
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

    // cleanup
    free(batch_buf);
    free(batch_times);
    fclose(tx_file);
    close(chunk_log_fd);
    free(g_state);

    return 0;
}