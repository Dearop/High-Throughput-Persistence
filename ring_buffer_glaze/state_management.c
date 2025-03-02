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
#include <zlib.h>    // for crc32 calculation

// ---------------------- Configuration ----------------------

#define BATCH_SIZE          (1 << 16)    // 65536 transactions per batch
#define INITIAL_BALANCE     1000000UL
#define MAX_ACCOUNTS        2000000UL    // 2 million accounts
#define TOTAL_BATCHES       50           // total # of batches to process

#define CHUNK_SIZE_ACCOUNTS 16384        // ~256 KB chunk if each Account=16 bytes
#define RING_CAPACITY       (CHUNK_SIZE_ACCOUNTS * 2)

#define RING_FILE           "chunk_ring.dat"
#define TX_FILE             "transactions.bin"

// ---------------------- Data Structures ----------------------

typedef struct {
    uint64_t sender;
    uint64_t receiver;
    uint32_t amount;
} Transaction;

typedef struct {
    uint64_t address;
    uint64_t balance;
} Account;

#pragma pack(push, 1)
typedef struct {
    uint8_t  valid_marker;  // 1 => fully written & valid, 0 => partial/invalid
    uint32_t crc32;
    uint32_t chunk_id;
    uint32_t data_len;
    Account  accounts[CHUNK_SIZE_ACCOUNTS];
} ChunkOnDisk;
#pragma pack(pop)

#pragma pack(push, 1)
typedef struct {
    char     signature[8];   // "CHNKRING"
    uint32_t version;
    uint32_t ring_capacity;  
    uint64_t ring_head;
    uint64_t ring_tail;
    uint8_t  reserved[32];   // pad to 64 bytes total
} RingMetadata;
#pragma pack(pop)

// ---------------------- Globals ----------------------
static Account *g_state = NULL;          // in-memory state
static uint64_t g_num_chunk_writes = 0;  // how many chunk writes
static uint64_t g_processed_batches = 0; // how many batches processed so far

// ---------------------- Utility: Timing ----------------------
static double timespec_diff_ms(const struct timespec *start, const struct timespec *end)
{
    double secs  = (double)(end->tv_sec - start->tv_sec);
    double nsecs = (double)(end->tv_nsec - start->tv_nsec) / 1e9;
    return (secs + nsecs) * 1000.0;
}

static int cmp_doubles(const void *a, const void *b)
{
    double da = *(const double *)a;
    double db = *(const double *)b;
    return (da > db) - (da < db);
}

// ---------------------- Fresh State ----------------------
static void init_fresh_state(void)
{
    g_state = (Account *)malloc(MAX_ACCOUNTS * sizeof(Account));
    if (!g_state) {
        perror("malloc g_state");
        exit(EXIT_FAILURE);
    }
    for (uint64_t i = 0; i < MAX_ACCOUNTS; i++) {
        g_state[i].address = i;
        g_state[i].balance = INITIAL_BALANCE;
    }
}

// ---------------------- Offsets in the Ring ----------------------
static inline off_t ring_offset_of_slot(uint64_t slot_idx)
{
    return 64 + (off_t)(slot_idx % RING_CAPACITY) * (off_t)sizeof(ChunkOnDisk);
}

// ---------------------- Metadata I/O ----------------------
static void ring_read_metadata(int fd, RingMetadata *meta)
{
    if (lseek(fd, 0, SEEK_SET) == (off_t)-1) {
        perror("lseek ring_read_metadata");
        exit(EXIT_FAILURE);
    }
    ssize_t rc = read(fd, meta, sizeof(*meta));
    if (rc < 0) {
        perror("read ring metadata");
        exit(EXIT_FAILURE);
    }
    // partial read => treat as new
    if (rc < (ssize_t)sizeof(*meta)) {
        memset(meta, 0, sizeof(*meta));
        memcpy(meta->signature, "CHNKRING", 8);
        meta->version       = 1;
        meta->ring_capacity = RING_CAPACITY;
        meta->ring_head     = 0;
        meta->ring_tail     = 0;
    }
}

static void ring_write_metadata(int fd, const RingMetadata *meta)
{
    if (lseek(fd, 0, SEEK_SET) == (off_t)-1) {
        perror("lseek ring_write_metadata");
        exit(EXIT_FAILURE);
    }
    ssize_t rc = write(fd, meta, sizeof(*meta));
    if (rc < 0 || rc < (ssize_t)sizeof(*meta)) {
        perror("write ring metadata");
        exit(EXIT_FAILURE);
    }
    if (fsync(fd) != 0) {
        perror("fsync ring metadata");
        exit(EXIT_FAILURE);
    }
}

// ---------------------- Two-Phase Write of a Chunk ----------------------
static void ring_write_chunk(int fd, uint64_t slot, ChunkOnDisk *chunk)
{
    off_t offset = ring_offset_of_slot(slot);

    // Mark invalid
    chunk->valid_marker = 0;

    // Step 1
    if (lseek(fd, offset, SEEK_SET) == (off_t)-1) {
        perror("lseek ring_write_chunk step1");
        exit(EXIT_FAILURE);
    }
    ssize_t to_write = sizeof(ChunkOnDisk);
    ssize_t rc = write(fd, chunk, to_write);
    if (rc < 0 || rc < to_write) {
        perror("write chunk step1");
        exit(EXIT_FAILURE);
    }

    // compute CRC ignoring valid_marker
    ChunkOnDisk tmp = *chunk;
    tmp.valid_marker = 0;
    tmp.crc32        = 0;
    uint8_t *p       = (uint8_t *)&tmp;
    p       += 1;
    size_t len = sizeof(tmp) - 1;

    uLong c = crc32(0L, Z_NULL, 0);
    c = crc32(c, p, (uInt)len);
    chunk->crc32        = (uint32_t)c;
    chunk->valid_marker = 1;

    // Step 2
    if (lseek(fd, offset, SEEK_SET) == (off_t)-1) {
        perror("lseek ring_write_chunk step2");
        exit(EXIT_FAILURE);
    }
    rc = write(fd, chunk, to_write);
    if (rc < 0 || rc < to_write) {
        perror("write chunk step2");
        exit(EXIT_FAILURE);
    }

    if (fsync(fd) != 0) {
        perror("fsync chunk finalize");
        exit(EXIT_FAILURE);
    }
}

// ---------------------- ring_store_chunk ----------------------
static void ring_store_chunk(int fd, ChunkOnDisk *chunk, RingMetadata *meta)
{
    uint64_t head = meta->ring_head;
    uint64_t tail = meta->ring_tail;
    uint64_t cap  = meta->ring_capacity;

    uint64_t next_tail = (tail + 1) % cap;
    if (next_tail == head) {
        // ring full => discard oldest
        head = (head + 1) % cap;
        meta->ring_head = head;
    }

    ring_write_chunk(fd, tail, chunk);
    meta->ring_tail = next_tail;
    ring_write_metadata(fd, meta);
}

// ---------------------- incremental_persist ----------------------
static void incremental_persist(int fd, RingMetadata *meta)
{
    ChunkOnDisk chunk;
    memset(&chunk, 0, sizeof(chunk));

    uint64_t possible_chunks = (MAX_ACCOUNTS + CHUNK_SIZE_ACCOUNTS - 1) / CHUNK_SIZE_ACCOUNTS;
    uint32_t cid = (uint32_t)(g_num_chunk_writes % possible_chunks);
    chunk.chunk_id = cid;

    uint64_t start_idx = (uint64_t)cid * CHUNK_SIZE_ACCOUNTS;
    uint64_t end_idx   = start_idx + CHUNK_SIZE_ACCOUNTS;
    if (end_idx > MAX_ACCOUNTS) {
        end_idx = MAX_ACCOUNTS;
    }
    uint64_t count = end_idx - start_idx;
    chunk.data_len = (uint32_t)count;
    memcpy(chunk.accounts, &g_state[start_idx], count * sizeof(Account));

    ring_store_chunk(fd, &chunk, meta);

    printf("Ring: wrote chunk_id=%u (write #%llu)\n",
           cid, (unsigned long long)g_num_chunk_writes);

    g_num_chunk_writes++;
}

// ---------------------- ring_recover ----------------------
static bool ring_recover(int fd)
{
    RingMetadata meta;
    ring_read_metadata(fd, &meta);

    // Validate ring
    if (memcmp(meta.signature, "CHNKRING", 8) != 0 || meta.version < 1) {
        printf("Ring file invalid or version < 1. No ring recovery done.\n");
        return false;
    }

    uint64_t possible_chunks = (MAX_ACCOUNTS + CHUNK_SIZE_ACCOUNTS - 1) / CHUNK_SIZE_ACCOUNTS;
    off_t *last_offsets = (off_t*)malloc(possible_chunks * sizeof(off_t));
    if (!last_offsets) {
        perror("malloc last_offsets");
        return false;
    }
    for (uint64_t i = 0; i < possible_chunks; i++) {
        last_offsets[i] = -1;
    }

    uint64_t head = meta.ring_head;
    uint64_t tail = meta.ring_tail;
    uint64_t cap  = meta.ring_capacity;

    uint64_t valid_chunks_found = 0;

    // walk from head to tail
    uint64_t idx = head;
    while (idx != tail) {
        off_t slot_off = ring_offset_of_slot(idx);
        if (lseek(fd, slot_off, SEEK_SET) == (off_t)-1) {
            perror("lseek ring slot");
            break;
        }
        ChunkOnDisk chunk;
        ssize_t rc = read(fd, &chunk, sizeof(chunk));
        if (rc < (ssize_t)sizeof(chunk)) {
            idx = (idx + 1) % cap;
            continue;
        }

        if (chunk.valid_marker == 1) {
            // check CRC
            ChunkOnDisk tmp = chunk;
            tmp.valid_marker = 0;
            uint8_t *p = (uint8_t *)&tmp;
            p += 1; 
            size_t len = sizeof(tmp) - 1;

            uint32_t old_crc = tmp.crc32;
            tmp.crc32 = 0;

            uLong c = crc32(0L, Z_NULL, 0);
            c = crc32(c, p, (uInt)len);

            if ((uint32_t)c == old_crc) {
                valid_chunks_found++;
                if (chunk.chunk_id < possible_chunks) {
                    last_offsets[chunk.chunk_id] = slot_off;
                }
            }
        }
        idx = (idx + 1) % cap;
    }

    if (valid_chunks_found == 0) {
        printf("Ring recovery found no valid chunks.\n");
        free(last_offsets);
        return false;
    }

    // If we found valid chunks, apply them
    for (uint64_t cid = 0; cid < possible_chunks; cid++) {
        off_t off = last_offsets[cid];
        if (off < 0) continue;
        if (lseek(fd, off, SEEK_SET) == (off_t)-1) continue;
        ChunkOnDisk chunk;
        if (read(fd, &chunk, sizeof(chunk)) < (ssize_t)sizeof(chunk)) {
            continue;
        }
        // apply
        uint64_t start_idx = cid * CHUNK_SIZE_ACCOUNTS;
        uint64_t end_idx   = start_idx + chunk.data_len;
        if (end_idx > MAX_ACCOUNTS) {
            end_idx = MAX_ACCOUNTS;
        }
        memcpy(&g_state[start_idx], chunk.accounts, (end_idx - start_idx) * sizeof(Account));
    }

    free(last_offsets);
    printf("Ring recovery complete. Applied %llu valid chunks.\n",
           (unsigned long long)valid_chunks_found);
    return true;
}

// ---------------------- open_ring_file ----------------------
static int open_ring_file(bool do_recovery, bool *used_fresh_state)
{
    int fd = open(RING_FILE, O_RDWR | O_CREAT, 0644);
    if (fd < 0) {
        perror("open ring file");
        exit(EXIT_FAILURE);
    }

    RingMetadata meta;
    ring_read_metadata(fd, &meta);

    if (memcmp(meta.signature, "CHNKRING", 8) != 0) {
        // brand new
        memcpy(meta.signature, "CHNKRING", 8);
        meta.version       = 1;
        meta.ring_capacity = RING_CAPACITY;
        meta.ring_head     = 0;
        meta.ring_tail     = 0;
        ring_write_metadata(fd, &meta);
        printf("Created new ring file '%s'.\n", RING_FILE);
    } else {
        printf("Opened existing ring file. head=%llu, tail=%llu, capacity=%u\n",
               (unsigned long long)meta.ring_head,
               (unsigned long long)meta.ring_tail,
               meta.ring_capacity);
    }

    // If do_recovery = true, try to parse existing chunks
    if (do_recovery) {
        bool success = ring_recover(fd);
        if (success) {
            // We actually used the old data => not a fresh state
            *used_fresh_state = false;
        }
        else {
            // We remain with fresh state
            *used_fresh_state = true;
        }
    }

    ring_write_metadata(fd, &meta);
    return fd;
}

// ---------------------- MAIN ----------------------
int main(void)
{
    // We'll keep track of whether we actually used the fresh state
    bool used_fresh_state = true;

    // 1) Allocate memory + fill with fresh data
    init_fresh_state();

    // 2) Attempt to open + recover from ring
    int ring_fd = open_ring_file(true, &used_fresh_state);

    // Print a message based on whether we ended up using the fresh state
    if (used_fresh_state) {
        printf("Using fresh in-memory state of %lu accounts.\n",
               (unsigned long)MAX_ACCOUNTS);
    } else {
        printf("Successfully recovered old state from ring.\n");
    }

    // 3) Open transaction file
    FILE *tx_file = fopen(TX_FILE, "rb");
    if (!tx_file) {
        perror("Error opening transactions file");
        close(ring_fd);
        free(g_state);
        exit(EXIT_FAILURE);
    }

    // We'll track batch times
    double *batch_times = (double *)malloc(TOTAL_BATCHES * sizeof(double));
    if (!batch_times) {
        perror("malloc batch_times");
        fclose(tx_file);
        close(ring_fd);
        free(g_state);
        exit(EXIT_FAILURE);
    }

    Transaction *batch = (Transaction *)malloc(BATCH_SIZE * sizeof(Transaction));
    if (!batch) {
        perror("malloc batch");
        free(batch_times);
        fclose(tx_file);
        close(ring_fd);
        free(g_state);
        exit(EXIT_FAILURE);
    }

    // read current metadata once
    RingMetadata meta;
    ring_read_metadata(ring_fd, &meta);

    double total_elapsed_ms = 0.0;

    // 4) Process transactions in batches
    for (uint64_t iteration = 0; iteration < TOTAL_BATCHES; iteration++) {
        struct timespec start, end;
        clock_gettime(CLOCK_MONOTONIC, &start);

        // read BATCH_SIZE transactions
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
                close(ring_fd);
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

        // store next chunk
        incremental_persist(ring_fd, &meta);

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

    // 5) Print summary
    double avg_ms = total_elapsed_ms / (double)TOTAL_BATCHES;
    printf("\nProcessed %d batches total.\n", (int)TOTAL_BATCHES);
    printf("Total time: %.3f ms\n", total_elapsed_ms);
    printf("Avg batch: %.3f ms\n", avg_ms);

    // Sort times and compute median/p90/p99
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

    printf("\nLatency stats for %d batches:\n", (int)TOTAL_BATCHES);
    printf("  Median: %.3f ms\n", median_ms);
    printf("  p90:    %.3f ms\n", p90_ms);
    printf("  p99:    %.3f ms\n", p99_ms);

    // 6) Cleanup
    free(batch);
    free(batch_times);
    fclose(tx_file);
    close(ring_fd);
    free(g_state);

    return 0;
}