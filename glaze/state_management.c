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

// ---------------- Configuration ----------------

// We'll process up to 65536 transactions per batch
#define BATCH_SIZE          (1 << 16)

// We have 2 million accounts total
#define MAX_ACCOUNTS        2000000UL

// We'll do 50 total batches
#define TOTAL_BATCHES       50

// We store accounts in chunks of 32768 => ~512 KB
#define CHUNK_SIZE_ACCOUNTS 32768
#define NUM_CHUNKS          ((MAX_ACCOUNTS + CHUNK_SIZE_ACCOUNTS - 1) / CHUNK_SIZE_ACCOUNTS)

// The initial balance for any account that doesn't yet exist in the log
#define INITIAL_BALANCE     1000000UL

// We'll store the log in a single file
#define LOG_FILE            "append_delete_log.bin"
#define TX_FILE             "transactions.bin"


// ---------------- Data Structures ----------------

// A single transaction: sender, receiver, amount
typedef struct {
    uint64_t sender;
    uint64_t receiver;
    uint32_t amount;
} Transaction;

// Each account has an address and a balance
typedef struct {
    uint64_t address;
    uint64_t balance;
} Account;

/**
 * We'll define each chunk entry to contain:
 *   - valid_marker    (1 => fully written, 0 => partial)
 *   - chunk_id        (which chunk does this correspond to?)
 *   - tx_count        (# of transactions in this batch)
 *   - transactions[]  (the batch of transactions)
 *   - accounts[]      (the final state snippet for that chunk)
 *
 * This can be very large: e.g., 65536 transactions + 16384 accounts
 */
#pragma pack(push,1)
typedef struct {
    uint8_t   valid_marker;  
    uint32_t  chunk_id;
    uint32_t  tx_count;
    Transaction transactions[BATCH_SIZE];      // up to 65536 transactions
    Account     accounts[CHUNK_SIZE_ACCOUNTS]; // final accounts for this chunk
} ChunkEntry;
#pragma pack(pop)

// ---------------- Globals ----------------

// We'll keep our entire state array in memory
static Account *g_state = NULL;

// We'll track the offset of the last version of each chunk. old_offsets[chunk_id] = offset in file
static off_t old_offsets[NUM_CHUNKS];

// We'll track how many chunk writes we've performed
static uint64_t g_num_chunk_writes = 0;

// We'll track how many total batches we've processed
static uint64_t g_processed_batches = 0;

// We'll keep a global file descriptor for the log
static int g_log_fd = -1;


// -------------- Time / Stats --------------

static double timespec_diff_ms(const struct timespec *start, const struct timespec *end)
{
    double s = (double)(end->tv_sec - start->tv_sec);
    double ns= (double)(end->tv_nsec - start->tv_nsec)/1e9;
    return (s + ns) * 1000.0;
}

static int cmp_doubles(const void *a, const void *b)
{
    double da = *(const double *)a;
    double db = *(const double *)b;
    return (da > db) - (da < db);
}


// -------------- Writing Chunks Safely --------------

/**
 * We'll write a new chunk version in a two-phase manner:
 *  1) Append new chunk at the end with valid_marker=0, then fsync
 *  2) Rewrite that chunk's valid_marker=1, then fsync
 *  3) Overwrite (invalidate) the old chunk if it exists
 *  4) Update old_offsets[chunk_id]
 *
 * If a crash happens mid-write, the old chunk is still intact
 * until we do step (3).
 */
static void invalidate_old_chunk(int fd, off_t old_off)
{
    if (old_off < 0) return;
    // We'll zero out the entire region for safety
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
 * Write a new chunk version with chunk_id, plus the final accounts and transactions
 */
static void safe_write_chunk(int fd, uint32_t chunk_id,
                             Transaction *batch, size_t tx_count)
{
    // 1) Build new chunk
    ChunkEntry new_chunk;
    memset(&new_chunk, 0, sizeof(new_chunk));
    new_chunk.valid_marker = 0; // for partial
    new_chunk.chunk_id     = chunk_id;
    new_chunk.tx_count     = (uint32_t)tx_count;

    if (tx_count > BATCH_SIZE) {
        tx_count = BATCH_SIZE; // clamp
    }
    // copy transactions
    memcpy(new_chunk.transactions, batch, tx_count*sizeof(Transaction));

    // copy final accounts for that chunk
    uint64_t start_idx = (uint64_t)chunk_id * CHUNK_SIZE_ACCOUNTS;
    uint64_t end_idx   = start_idx + CHUNK_SIZE_ACCOUNTS;
    if (end_idx > MAX_ACCOUNTS) end_idx = MAX_ACCOUNTS;
    uint64_t count = end_idx - start_idx;
    memcpy(new_chunk.accounts, &g_state[start_idx], count*sizeof(Account));

    // 2) old offset
    off_t old_off = old_offsets[chunk_id];

    // 3) find end-of-file offset
    off_t new_off = lseek(fd, 0, SEEK_END);
    if (new_off == (off_t)-1) {
        perror("lseek SEEK_END");
        exit(EXIT_FAILURE);
    }

    // 4) Phase 1: write chunk with valid_marker=0
    if (write(fd, &new_chunk, sizeof(new_chunk)) != (ssize_t)sizeof(new_chunk)) {
        perror("write partial chunk");
        exit(EXIT_FAILURE);
    }
    // fsync
    if (fsync(fd) != 0) {
        perror("fsync partial chunk");
        exit(EXIT_FAILURE);
    }

    // 5) Phase 2: valid_marker=1
    new_chunk.valid_marker = 1;
    if (lseek(fd, new_off, SEEK_SET) == (off_t)-1) {
        perror("lseek to rewrite chunk valid_marker=1");
        exit(EXIT_FAILURE);
    }
    if (write(fd, &new_chunk, sizeof(new_chunk)) != (ssize_t)sizeof(new_chunk)) {
        perror("write finalize chunk");
        exit(EXIT_FAILURE);
    }
    // fsync
    if (fsync(fd) != 0) {
        perror("fsync finalize chunk");
        exit(EXIT_FAILURE);
    }

    // 6) Now we can safely invalidate the old chunk
    if (old_off != -1) {
        invalidate_old_chunk(fd, old_off);
    }

    // 7) update old_offsets
    old_offsets[chunk_id] = new_off;

    printf("Chunk: wrote chunk_id=%u with %zu tx at offset=%lld, old=%lld\n",
           chunk_id, tx_count, (long long)new_off, (long long)old_off);
}


// -------------- Incremental Persist --------------

/**
 * We'll pick chunk_id = (g_num_chunk_writes % NUM_CHUNKS) for round-robin
 * Then do safe_write_chunk
 */
static void incremental_persist(int fd, Transaction *batch, size_t tx_count)
{
    uint32_t chunk_id = (uint32_t)(g_num_chunk_writes % NUM_CHUNKS);
    g_num_chunk_writes++;

    safe_write_chunk(fd, chunk_id, batch, tx_count);
}


// -------------- Recovery --------------

/**
 * We'll parse the entire file in increments of sizeof(ChunkEntry),
 * ignoring zero or invalid entries, and track the last offset for each chunk_id.
 * Then read that offset and apply accounts to g_state.
 */
static void recover_from_file(int fd)
{
    // init old_offsets
    for (uint32_t c=0; c<NUM_CHUNKS; c++) {
        old_offsets[c] = -1;
    }

    // get file size
    off_t end_pos = lseek(fd, 0, SEEK_END);
    if (end_pos == 0) {
        printf("Log file empty, no chunks to recover.\n");
        return;
    }
    if (end_pos < 0) {
        perror("lseek end");
        exit(EXIT_FAILURE);
    }

    // read from offset=0 to end in increments
    size_t csize = sizeof(ChunkEntry);
    off_t offset = 0;
    while (offset + (off_t)csize <= end_pos) {
        // read chunk
        if (lseek(fd, offset, SEEK_SET) == (off_t)-1) {
            perror("lseek recover");
            break;
        }
        ChunkEntry chunk;
        ssize_t rc = read(fd, &chunk, csize);
        if (rc<0) {
            perror("read chunk in recover");
            break;
        }
        if (rc < (ssize_t)csize) {
            // partial => done
            break;
        }
        // check if all zero
        bool all_zero = true;
        const uint8_t *p = (const uint8_t*)&chunk;
        for (size_t i=0; i<csize; i++) {
            if (p[i]!=0) {
                all_zero=false;
                break;
            }
        }
        if (!all_zero && chunk.valid_marker==1 && chunk.chunk_id<NUM_CHUNKS) {
            // we have a valid chunk => record offset
            old_offsets[chunk.chunk_id] = offset;
        }
        offset += csize;
    }

    // now read final chunk for each chunk_id
    for (uint32_t c=0; c<NUM_CHUNKS; c++) {
        off_t offs = old_offsets[c];
        if (offs<0) continue;
        if (lseek(fd, offs, SEEK_SET)==(off_t)-1) continue;
        ChunkEntry chunk;
        if (read(fd, &chunk, csize) < (ssize_t)csize) continue;
        // copy accounts
        uint64_t start_idx = (uint64_t)c*CHUNK_SIZE_ACCOUNTS;
        uint64_t end_idx   = start_idx+CHUNK_SIZE_ACCOUNTS;
        if (end_idx>MAX_ACCOUNTS) end_idx=MAX_ACCOUNTS;
        uint64_t count = end_idx-start_idx;
        memcpy(&g_state[start_idx], chunk.accounts, count*sizeof(Account));
    }
    printf("Recovery done. Reconstructed final chunk versions from file.\n");
}


// -------------- open_log_file_and_recover --------------

static void open_log_file_and_recover(const char *filename)
{
    g_log_fd = open(filename, O_RDWR | O_CREAT, 0644);
    if (g_log_fd<0) {
        perror("open log");
        exit(EXIT_FAILURE);
    }
    off_t sz = lseek(g_log_fd, 0, SEEK_END);
    if (sz==0) {
        printf("Created new log file '%s'.\n", filename);
    } else {
        printf("Opened existing log file '%s' (size=%lld).\n", filename, (long long)sz);
        recover_from_file(g_log_fd);
    }
}


// -------------- main --------------

int main(void)
{
    // 1) Allocate entire state
    g_state = (Account*)malloc(MAX_ACCOUNTS*sizeof(Account));
    if (!g_state) {
        perror("malloc g_state");
        exit(EXIT_FAILURE);
    }
    // zero out
    memset(g_state, 0, MAX_ACCOUNTS*sizeof(Account));

    // 2) open log file, recover
    open_log_file_and_recover(LOG_FILE);

    // For any chunk that wasn't found, set those addresses to INITIAL_BALANCE
    for (uint32_t c=0; c<NUM_CHUNKS; c++) {
        if (old_offsets[c] < 0) {
            // chunk doesn't exist => init 
            uint64_t start_idx = (uint64_t)c*CHUNK_SIZE_ACCOUNTS;
            uint64_t end_idx   = start_idx + CHUNK_SIZE_ACCOUNTS;
            if (end_idx>MAX_ACCOUNTS) end_idx=MAX_ACCOUNTS;
            for (uint64_t i=start_idx; i<end_idx; i++) {
                g_state[i].address = i;
                g_state[i].balance = INITIAL_BALANCE;
            }
        }
    }
    printf("In-memory state loaded/recovered. Some chunks fresh, others from log.\n");

    // 3) open transaction file
    FILE *txf = fopen(TX_FILE, "rb");
    if (!txf) {
        perror("fopen transactions");
        close(g_log_fd);
        free(g_state);
        exit(EXIT_FAILURE);
    }

    // We'll store each batch's processing time
    double *batch_times = (double*)malloc(TOTAL_BATCHES*sizeof(double));
    if (!batch_times) {
        perror("malloc batch_times");
        fclose(txf);
        close(g_log_fd);
        free(g_state);
        exit(EXIT_FAILURE);
    }

    // We'll read each batch into this buffer
    Transaction *batch_buf = (Transaction*)malloc(BATCH_SIZE*sizeof(Transaction));
    if (!batch_buf) {
        perror("malloc batch_buf");
        free(batch_times);
        fclose(txf);
        close(g_log_fd);
        free(g_state);
        exit(EXIT_FAILURE);
    }

    double total_elapsed_ms=0.0;

    // 4) main loop
    for (uint64_t iteration=0; iteration<TOTAL_BATCHES; iteration++) {
        struct timespec start, end;
        clock_gettime(CLOCK_MONOTONIC, &start);

        // read BATCH_SIZE transactions from file
        size_t read_count = fread(batch_buf, sizeof(Transaction), BATCH_SIZE, txf);
        if (read_count < BATCH_SIZE) {
            // if EOF, wrap
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

        // apply in memory
        for (size_t i=0; i<read_count; i++) {
            Transaction *tx = &batch_buf[i];
            // naive direct indexing
            if (g_state[tx->sender].balance >= tx->amount) {
                g_state[tx->sender].balance  -= tx->amount;
                g_state[tx->receiver].balance+= tx->amount;
            }
        }

        // persist chunk with final accounts + transactions
        incremental_persist(g_log_fd, batch_buf, read_count);

        clock_gettime(CLOCK_MONOTONIC, &end);
        double ms = timespec_diff_ms(&start, &end);
        batch_times[iteration] = ms;
        total_elapsed_ms += ms;
        g_processed_batches++;

        printf("Batch %llu / %llu processed in %.3f ms\n",
               (unsigned long long)(iteration+1),
               (unsigned long long)TOTAL_BATCHES,
               ms);
    }

    // 5) Stats
    double avg_ms = total_elapsed_ms / (double)TOTAL_BATCHES;
    printf("\nProcessed %d batches total.\n", (int)TOTAL_BATCHES);
    printf("Total time:  %.3f ms\n", total_elapsed_ms);
    printf("Avg batch:   %.3f ms\n", avg_ms);

    // sort for median/p90/p99
    qsort(batch_times, TOTAL_BATCHES, sizeof(double), cmp_doubles);

    double median_ms;
    if (TOTAL_BATCHES % 2==0) {
        int mid=(int)(TOTAL_BATCHES/2);
        median_ms=(batch_times[mid-1]+batch_times[mid])/2.0;
    } else {
        median_ms=batch_times[TOTAL_BATCHES/2];
    }

    int idx_90=(int)ceil(0.90*TOTAL_BATCHES)-1;
    if (idx_90<0) idx_90=0;
    if (idx_90>=(int)TOTAL_BATCHES) idx_90=(int)TOTAL_BATCHES-1;
    double p90=batch_times[idx_90];

    int idx_99=(int)ceil(0.99*TOTAL_BATCHES)-1;
    if (idx_99<0) idx_99=0;
    if (idx_99>=(int)TOTAL_BATCHES) idx_99=(int)TOTAL_BATCHES-1;
    double p99=batch_times[idx_99];

    printf("\nLatency stats:\n");
    printf("  Median:  %.3f ms\n", median_ms);
    printf("  p90:     %.3f ms\n", p90);
    printf("  p99:     %.3f ms\n", p99);

    // 6) cleanup
    free(batch_buf);
    free(batch_times);
    fclose(txf);
    close(g_log_fd);
    free(g_state);

    return 0;
}