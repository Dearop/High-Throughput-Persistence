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

// ============ Configuration ============

// Each batch processes up to 65536 transactions
#define BATCH_SIZE (1 << 16)

// 2 million accounts in total
#define MAX_ACCOUNTS 2000000UL

// We'll run 50 total batches by default
#define TOTAL_BATCHES 50

// We'll store accounts in "chunks" of 16384 addresses each (~256 KB).
#define CHUNK_SIZE_ACCOUNTS 16384
#define NUM_CHUNKS ((MAX_ACCOUNTS + CHUNK_SIZE_ACCOUNTS - 1) / CHUNK_SIZE_ACCOUNTS)

// The ring can store 2 * NUM_CHUNKS entries (size your ring as you like).
#define RING_CAPACITY (NUM_CHUNKS * 2)

// The initial balance for all accounts
#define INITIAL_BALANCE 1000000UL

// Filenames
#define RING_FILE "ring_log.bin"
#define TX_FILE   "transactions.bin"


// ============ Data Structures ============

// Each transaction: sender, receiver, amount
typedef struct {
    uint64_t sender;
    uint64_t receiver;
    uint32_t amount;
} Transaction;

// Each account: address and balance
typedef struct {
    uint64_t address;
    uint64_t balance;
} Account;

/**
 * On-disk ring entry:
 *   - valid_marker   (1 => valid, 0 => partial)
 *   - chunk_id       
 *   - tx_count       (# of transactions in this batch)
 *   - transactions[] (the batch's transactions)
 *   - accounts[]     (the final chunk of accounts after applying them)
 *
 * For example, if BATCH_SIZE=65536, 
 * we store that many transactions plus CHUNK_SIZE_ACCOUNTS accounts.
 */
#pragma pack(push,1)
typedef struct {
    uint8_t   valid_marker; 
    uint32_t  chunk_id;
    uint32_t  tx_count;
    Transaction transactions[BATCH_SIZE];
    Account     accounts[CHUNK_SIZE_ACCOUNTS];
} ChunkEntry;
#pragma pack(pop)

/**
 * We'll store ring metadata (64 bytes) at offset 0 in the file:
 *   signature[8] 
 *   version
 *   ring_capacity
 *   ring_head
 *   ring_tail
 *   etc.
 */
#pragma pack(push,1)
typedef struct {
    char     signature[8];    // "CHNKLOG"
    uint32_t version;
    uint32_t ring_capacity;  
    uint64_t ring_head;
    uint64_t ring_tail;
    uint8_t  reserved[32];   // total 64 bytes
} RingMetadata;
#pragma pack(pop)


// ============ Globals ============

// Our in-memory array of all accounts
static Account *g_state = NULL;

// We'll keep track of the final offsets for each chunk if we want direct indexing.
// Not strictly required for ring approach, but included for reference.
static off_t chunk_offsets[NUM_CHUNKS];

// We'll track how many chunk writes we've done
static uint64_t g_num_writes = 0;
static uint64_t g_processed_batches = 0;

// For ring I/O
static int ring_fd = -1;
static RingMetadata g_ring_meta;


// ============ Forward Declarations ============
// Because we reference these in main or other functions:

static void ring_write_chunk(ChunkEntry *entry);
static void ring_recover(void);
static void ring_open_and_recover(void);
static void incremental_persist(int fd, Transaction *batch, size_t tx_count);


// ============ Time / Stats ============

static double timespec_diff_ms(const struct timespec *start, const struct timespec *end) {
    double s = (double)(end->tv_sec - start->tv_sec);
    double ns = (double)(end->tv_nsec - start->tv_nsec)/1e9;
    return (s + ns) * 1000.0;
}

// Qsort comparator for doubles
static int cmp_doubles(const void *a, const void *b) {
    double da = *(const double *)a;
    double db = *(const double *)b;
    return (da > db) - (da < db);
}


// ============ Ring Metadata I/O ============

static void ring_read_metadata(int fd, RingMetadata *meta) {
    if (lseek(fd, 0, SEEK_SET) == (off_t)-1) {
        perror("lseek ring_read_metadata");
        exit(EXIT_FAILURE);
    }
    ssize_t rc = read(fd, meta, sizeof(*meta));
    if (rc < 0) {
        perror("read ring metadata");
        exit(EXIT_FAILURE);
    }
    // partial => brand new
    if (rc < (ssize_t)sizeof(*meta)) {
        memset(meta, 0, sizeof(*meta));
        memcpy(meta->signature, "CHNKLOG", 7);
        meta->version        = 1;
        meta->ring_capacity  = RING_CAPACITY;
        meta->ring_head      = 0;
        meta->ring_tail      = 0;
    }
}

static void ring_write_metadata(int fd, const RingMetadata *meta) {
    if (lseek(fd, 0, SEEK_SET) == (off_t)-1) {
        perror("lseek ring_write_metadata");
        exit(EXIT_FAILURE);
    }
    ssize_t rc = write(fd, meta, sizeof(*meta));
    if (rc < 0 || rc < (ssize_t)sizeof(*meta)) {
        perror("write ring metadata");
        exit(EXIT_FAILURE);
    }
    fsync(fd);
}

// Return the file offset in the ring for a given slot index
static inline off_t ring_slot_offset(uint64_t slot_index) {
    // 64 bytes of metadata at offset 0
    return 64 + (off_t)(slot_index % g_ring_meta.ring_capacity) * (off_t)sizeof(ChunkEntry);
}


// ============ Two-Phase chunk write to the ring ============

/**
 * ring_write_chunk:
 * 
 * We'll store a chunk in two phases:
 *   - write with valid_marker=0 and fsync
 *   - rewrite valid_marker=1 and fsync
 * Then we move ring_tail forward in metadata.
 */
static void ring_write_chunk(ChunkEntry *entry) {
    // ring approach
    uint64_t head = g_ring_meta.ring_head;
    uint64_t tail = g_ring_meta.ring_tail;
    uint64_t cap  = g_ring_meta.ring_capacity;

    // check if ring is full
    uint64_t next_tail = (tail + 1) % cap;
    if (next_tail == head) {
        // discard oldest
        head = (head + 1) % cap;
        g_ring_meta.ring_head = head;
    }

    off_t offset = ring_slot_offset(tail);

    // Phase 1: write valid_marker=0
    entry->valid_marker = 0;
    if (lseek(ring_fd, offset, SEEK_SET) == (off_t)-1) {
        perror("lseek ring_write_chunk phase1");
        exit(EXIT_FAILURE);
    }
    size_t chunk_size = sizeof(ChunkEntry);
    ssize_t rc = write(ring_fd, entry, chunk_size);
    if (rc < 0 || rc < (ssize_t)chunk_size) {
        perror("write ring chunk phase1");
        exit(EXIT_FAILURE);
    }
    if (fsync(ring_fd) != 0) {
        perror("fsync ring chunk phase1");
        exit(EXIT_FAILURE);
    }

    // Phase2: mark valid
    entry->valid_marker = 1;
    if (lseek(ring_fd, offset, SEEK_SET) == (off_t)-1) {
        perror("lseek ring_write_chunk phase2");
        exit(EXIT_FAILURE);
    }
    rc = write(ring_fd, entry, chunk_size);
    if (rc < 0 || rc < (ssize_t)chunk_size) {
        perror("write ring chunk phase2");
        exit(EXIT_FAILURE);
    }
    if (fsync(ring_fd) != 0) {
        perror("fsync ring chunk phase2");
        exit(EXIT_FAILURE);
    }

    // move tail forward
    g_ring_meta.ring_tail = next_tail;
    ring_write_metadata(ring_fd, &g_ring_meta);

    printf("Ring: Wrote chunk_id=%u at ring slot=%llu, offset=%lld\n",
           entry->chunk_id, (unsigned long long)tail, (long long)offset);
}


// ============ Recovery: read from head..tail, keep last chunk for each ID ============

static void ring_recover(void) {
    uint64_t head = g_ring_meta.ring_head;
    uint64_t tail = g_ring_meta.ring_tail;
    uint64_t cap  = g_ring_meta.ring_capacity;
    if (head == tail) {
        printf("No ring data to recover.\n");
        return;
    }

    // track final offsets
    for (uint32_t i=0; i<NUM_CHUNKS; i++) {
        chunk_offsets[i] = -1;
    }

    // Walk the ring
    uint64_t idx = head;
    while (idx != tail) {
        off_t offset = ring_slot_offset(idx);
        if (lseek(ring_fd, offset, SEEK_SET) == (off_t)-1) {
            perror("lseek ring slot");
            break;
        }
        ChunkEntry chunk;
        ssize_t rc = read(ring_fd, &chunk, sizeof(chunk));
        if (rc < 0) {
            perror("read ring slot");
            break;
        }
        if (rc < (ssize_t)sizeof(chunk)) {
            break;
        }
        if (chunk.valid_marker == 1) {
            if (chunk.chunk_id < NUM_CHUNKS) {
                chunk_offsets[chunk.chunk_id] = offset;
            }
        }
        idx = (idx + 1) % cap;
    }

    // Now read final chunk for each ID
    for (uint32_t c=0; c<NUM_CHUNKS; c++) {
        off_t offs = chunk_offsets[c];
        if (offs < 0) continue;
        if (lseek(ring_fd, offs, SEEK_SET) == (off_t)-1) continue;
        ChunkEntry chunk;
        ssize_t rc = read(ring_fd, &chunk, sizeof(chunk));
        if (rc < (ssize_t)sizeof(chunk)) continue;
        // copy chunk.accounts into g_state
        uint64_t start_idx = (uint64_t)c * CHUNK_SIZE_ACCOUNTS;
        uint64_t end_idx   = start_idx + CHUNK_SIZE_ACCOUNTS;
        if (end_idx > MAX_ACCOUNTS) end_idx = MAX_ACCOUNTS;
        uint64_t count = end_idx - start_idx;
        memcpy(&g_state[start_idx], chunk.accounts, count*sizeof(Account));
    }

    printf("Ring recovery complete. Loaded final chunk versions.\n");
}


// ============ ring_open_and_recover ============

static void ring_open_and_recover(void) {
    ring_fd = open(RING_FILE, O_RDWR | O_CREAT, 0644);
    if (ring_fd < 0) {
        perror("open ring file");
        exit(EXIT_FAILURE);
    }
    off_t sz = lseek(ring_fd, 0, SEEK_END);
    if (sz<0) {
        perror("lseek ring end");
        exit(EXIT_FAILURE);
    }

    RingMetadata tmp;
    ring_read_metadata(ring_fd, &tmp);

    if (memcmp(tmp.signature, "CHNKLOG", 7) != 0) {
        // brand new
        memcpy(tmp.signature, "CHNKLOG", 7);
        tmp.version        = 1;
        tmp.ring_capacity  = RING_CAPACITY;
        tmp.ring_head      = 0;
        tmp.ring_tail      = 0;
        ring_write_metadata(ring_fd, &tmp);
        printf("Created new ring file '%s'.\n", RING_FILE);
    } else {
        printf("Opened ring file '%s'. head=%llu, tail=%llu, capacity=%u\n",
               RING_FILE,
               (unsigned long long)tmp.ring_head,
               (unsigned long long)tmp.ring_tail,
               tmp.ring_capacity);
    }

    g_ring_meta = tmp;

    if (g_ring_meta.ring_head != g_ring_meta.ring_tail) {
        ring_recover();
    } else {
        printf("Ring is empty, no old data to recover.\n");
    }
}


// ============ incremental_persist ============

static void incremental_persist(int fd, Transaction *batch, size_t tx_count) {
    // We'll do chunk_id = (g_num_writes % NUM_CHUNKS) for round-robin
    uint32_t chunk_id = (uint32_t)(g_num_writes % NUM_CHUNKS);
    g_num_writes++;

    // fill ChunkEntry
    static ChunkEntry entry;
    memset(&entry, 0, sizeof(entry));
    entry.chunk_id = chunk_id;
    entry.tx_count = (uint32_t)tx_count;

    // copy the transaction data
    if (tx_count > BATCH_SIZE) {
        tx_count = BATCH_SIZE;
    }
    memcpy(entry.transactions, batch, tx_count*sizeof(Transaction));

    // copy final accounts for that chunk
    uint64_t start_idx = (uint64_t)chunk_id * CHUNK_SIZE_ACCOUNTS;
    uint64_t end_idx   = start_idx + CHUNK_SIZE_ACCOUNTS;
    if (end_idx > MAX_ACCOUNTS) {
        end_idx = MAX_ACCOUNTS;
    }
    uint64_t count = end_idx - start_idx;
    memcpy(entry.accounts, &g_state[start_idx], count*sizeof(Account));

    ring_write_chunk(&entry);
}


// ============ main ============

int main(void) {
    // 1) Allocate big array
    g_state = (Account*)malloc(MAX_ACCOUNTS*sizeof(Account));
    if (!g_state) {
        perror("malloc g_state");
        exit(EXIT_FAILURE);
    }
    // Zero out
    memset(g_state, 0, MAX_ACCOUNTS*sizeof(Account));

    // 2) Open ring, recover
    ring_open_and_recover();

    // For any chunk that doesn't exist, init with INITIAL_BALANCE
    for (uint32_t c=0; c<NUM_CHUNKS; c++) {
        if (chunk_offsets[c] < 0) {
            uint64_t start_idx = (uint64_t)c*CHUNK_SIZE_ACCOUNTS;
            uint64_t end_idx   = start_idx + CHUNK_SIZE_ACCOUNTS;
            if (end_idx > MAX_ACCOUNTS) end_idx = MAX_ACCOUNTS;
            for (uint64_t i=start_idx; i<end_idx; i++) {
                g_state[i].address = i;
                g_state[i].balance = INITIAL_BALANCE;
            }
        }
    }
    printf("In-memory state ready (some chunks from ring, others fresh).\n");

    // 3) Open transaction file
    FILE *txf = fopen(TX_FILE, "rb");
    if (!txf) {
        perror("fopen TX_FILE");
        close(ring_fd);
        free(g_state);
        exit(EXIT_FAILURE);
    }

    // We'll track time for each batch
    double *batch_times = (double*)malloc(TOTAL_BATCHES*sizeof(double));
    if (!batch_times) {
        perror("malloc batch_times");
        fclose(txf);
        close(ring_fd);
        free(g_state);
        exit(EXIT_FAILURE);
    }

    Transaction *batch_buf = (Transaction*)malloc(BATCH_SIZE*sizeof(Transaction));
    if (!batch_buf) {
        perror("malloc batch_buf");
        free(batch_times);
        fclose(txf);
        close(ring_fd);
        free(g_state);
        exit(EXIT_FAILURE);
    }

    double total_elapsed_ms = 0.0;

    // 4) Process 50 batches
    for (uint64_t iteration=0; iteration<TOTAL_BATCHES; iteration++) {
        struct timespec start, end;
        clock_gettime(CLOCK_MONOTONIC, &start);

        // read a batch
        size_t read_count = fread(batch_buf, sizeof(Transaction), BATCH_SIZE, txf);
        if (read_count < BATCH_SIZE) {
            if (feof(txf)) {
                rewind(txf);
                read_count = fread(batch_buf, sizeof(Transaction), BATCH_SIZE, txf);
            } else {
                perror("Error reading TX batch");
                free(batch_buf); free(batch_times);
                fclose(txf); close(ring_fd); free(g_state);
                exit(EXIT_FAILURE);
            }
        }

        // apply in memory
        for (size_t i=0; i<read_count; i++) {
            Transaction *tx = &batch_buf[i];
            if (g_state[tx->sender].balance >= tx->amount) {
                g_state[tx->sender].balance  -= tx->amount;
                g_state[tx->receiver].balance+= tx->amount;
            }
        }

        // store chunk (batch + final accounts) 
        incremental_persist(ring_fd, batch_buf, read_count);

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
    printf("Total time: %.3f ms\n", total_elapsed_ms);
    printf("Avg batch: %.3f ms\n", avg_ms);

    // sort for median/p90/p99
    qsort(batch_times, TOTAL_BATCHES, sizeof(double), cmp_doubles);

    double median_ms;
    if (TOTAL_BATCHES % 2 == 0) {
        int mid = (int)(TOTAL_BATCHES/2);
        median_ms = (batch_times[mid-1] + batch_times[mid]) / 2.0;
    } else {
        median_ms = batch_times[TOTAL_BATCHES/2];
    }

    int idx_90 = (int)ceil(0.90*TOTAL_BATCHES) -1;
    if (idx_90<0) idx_90=0;
    if (idx_90>=(int)TOTAL_BATCHES) idx_90=(int)TOTAL_BATCHES-1;
    double p90 = batch_times[idx_90];

    int idx_99 = (int)ceil(0.99*TOTAL_BATCHES) -1;
    if (idx_99<0) idx_99=0;
    if (idx_99>=(int)TOTAL_BATCHES) idx_99=(int)TOTAL_BATCHES-1;
    double p99 = batch_times[idx_99];

    printf("\nLatency stats:\n");
    printf("  Median: %.3f ms\n", median_ms);
    printf("  p90:    %.3f ms\n", p90);
    printf("  p99:    %.3f ms\n", p99);

    // 6) Cleanup
    free(batch_buf);
    free(batch_times);
    fclose(txf);
    close(ring_fd);
    free(g_state);

    return 0;
}