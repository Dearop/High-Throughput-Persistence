#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdbool.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <zlib.h>

//
// CONFIG
//
#define INITIAL_BALANCE     1000000UL
#define MAX_ACCOUNTS        2000000UL
#define CHUNK_SIZE_ACCOUNTS 16384
#define RING_CAPACITY       (CHUNK_SIZE_ACCOUNTS * 2)
#define RING_FILE           "chunk_ring.dat"

typedef struct {
    uint64_t address;
    uint64_t balance;
} Account;

/**
 * On-disk chunk format, identical to your main code
 */
#pragma pack(push, 1)
typedef struct {
    uint8_t  valid_marker; 
    uint32_t crc32;
    uint32_t chunk_id;
    uint32_t data_len;
    Account  accounts[CHUNK_SIZE_ACCOUNTS];
} ChunkOnDisk;
#pragma pack(pop)

/**
 * Ring metadata, same as in your main code
 */
#pragma pack(push, 1)
typedef struct {
    char     signature[8];  
    uint32_t version;
    uint32_t ring_capacity;
    uint64_t ring_head;
    uint64_t ring_tail;
    uint8_t  reserved[32]; 
} RingMetadata;
#pragma pack(pop)

// Global in-memory array
static Account *g_state = NULL;

//
// Return the file offset of a ring slot
//
static inline off_t ring_offset_of_slot(uint64_t slot_idx) {
    // 64 bytes of metadata at start
    return 64 + (off_t)(slot_idx % RING_CAPACITY) * (off_t)sizeof(ChunkOnDisk);
}

//
// Read the ring metadata
//
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
    // partial => fill defaults
    if (rc < (ssize_t)sizeof(*meta)) {
        memset(meta, 0, sizeof(*meta));
        memcpy(meta->signature, "CHNKRING", 8);
        meta->version       = 1;
        meta->ring_capacity = RING_CAPACITY;
        meta->ring_head     = 0;
        meta->ring_tail     = 0;
    }
}

/**
 * Attempt to rebuild final state from ring, storing into g_state
 *
 * Return true if recovery found any valid chunk,
 * false if it found none or ring file was invalid.
 */
static bool ring_recover(int fd) {
    // read metadata
    RingMetadata meta;
    ring_read_metadata(fd, &meta);

    // check signature
    if (memcmp(meta.signature, "CHNKRING", 8) != 0 || meta.version < 1) {
        printf("Ring file invalid or missing. No recovery.\n");
        return false;
    }

    // We'll define the same "possible chunks" logic as in main
    uint64_t possible_chunks = (MAX_ACCOUNTS + CHUNK_SIZE_ACCOUNTS - 1) / CHUNK_SIZE_ACCOUNTS;

    // track the last offset for each chunk_id
    off_t *last_offsets = (off_t *)malloc(possible_chunks * sizeof(off_t));
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
            // partial => skip
            idx = (idx + 1) % cap;
            continue;
        }
        // check valid_marker + CRC
        if (chunk.valid_marker == 1) {
            ChunkOnDisk tmp = chunk;
            tmp.valid_marker = 0;
            uint8_t *p = (uint8_t *)&tmp;
            p += 1;
            size_t len = sizeof(tmp) - 1;

            uint32_t old_crc = tmp.crc32;
            tmp.crc32        = 0;

            uLong c = crc32(0L, Z_NULL, 0);
            c = crc32(c, p, (uInt)len);

            if ((uint32_t)c == old_crc) {
                // valid chunk
                valid_chunks_found++;
                if (chunk.chunk_id < possible_chunks) {
                    last_offsets[chunk.chunk_id] = slot_off;
                }
            }
        }
        idx = (idx + 1) % cap;
    }

    if (valid_chunks_found == 0) {
        printf("No valid chunk found in ring. Recovery yields nothing.\n");
        free(last_offsets);
        return false;
    }

    // apply the last offset for each chunk
    for (uint64_t cid = 0; cid < possible_chunks; cid++) {
        off_t off = last_offsets[cid];
        if (off < 0) continue;
        if (lseek(fd, off, SEEK_SET) == (off_t)-1) continue;
        ChunkOnDisk chunk;
        if (read(fd, &chunk, sizeof(chunk)) < (ssize_t)sizeof(chunk)) {
            continue;
        }
        // copy to g_state
        uint64_t start_idx = cid * CHUNK_SIZE_ACCOUNTS;
        uint64_t end_idx   = start_idx + chunk.data_len;
        if (end_idx > MAX_ACCOUNTS) end_idx = MAX_ACCOUNTS;
        memcpy(&g_state[start_idx], chunk.accounts, (end_idx - start_idx) * sizeof(Account));
    }

    free(last_offsets);

    printf("Ring recovery: Found %llu valid chunks. State is loaded.\n", 
           (unsigned long long)valid_chunks_found);
    return true;
}

/**
 * A small function that checks if the final state is correct.
 * For example, we might check:
 *   - The sum of all balances equals some expected number
 *   - The average or a random sample is in a valid range
 */
static bool verify_state_consistency(void) {
    // For demonstration, let's compute the sum of all balances.
    // In your real code, you might do more sophisticated checks.

    uint64_t total_sum = 0;
    for (uint64_t i = 0; i < MAX_ACCOUNTS; i++) {
        total_sum += g_state[i].balance;
    }

    printf("Final sum of all account balances: %llu\n", 
           (unsigned long long)total_sum);

    // If you had a known expected sum (e.g., initial total = #ACCOUNTS * INITIAL_BALANCE),
    // plus or minus the net effect of transactions, you could compare. 
    // For now, we just ensure it doesn't overflow or is obviously incorrect:
    if (total_sum < MAX_ACCOUNTS * 50000UL) {
        // Arbitrary check
        printf("Suspiciously low sum, might be incorrect.\n");
        return false;
    }
    if (total_sum > MAX_ACCOUNTS * 2UL * INITIAL_BALANCE) {
        printf("Suspiciously high sum, might be incorrect.\n");
        return false;
    }
    // Otherwise, we guess it's correct
    return true;
}

/**
 * MAIN
 * - Allocates g_state
 * - Opens ring
 * - Attempts ring recovery
 * - If ring is invalid or empty, we might remain with uninitialized balances
 *   (or you could re-init them to 0 or 1 million, depending on your logic)
 * - Then we do a consistency check
 */
int main(void) {
    // 1. Allocate memory for g_state. If you want fresh init to something, do it here:
    g_state = (Account *)malloc(MAX_ACCOUNTS * sizeof(Account));
    if (!g_state) {
        perror("malloc g_state");
        exit(EXIT_FAILURE);
    }
    // optionally zero them out
    memset(g_state, 0, MAX_ACCOUNTS * sizeof(Account));

    // 2. Open ring file
    int fd = open(RING_FILE, O_RDONLY);
    if (fd < 0) {
        perror("open ring file for verify");
        printf("No ring file found, can't verify. Exiting.\n");
        free(g_state);
        return 1;
    }

    // 3. Attempt to recover final state from ring
    bool success = ring_recover(fd);
    close(fd);

    if (!success) {
        printf("Recovery was unsuccessful or empty. Can't verify old state.\n");
        free(g_state);
        return 1;
    }

    // 4. Do final checks
    bool ok = verify_state_consistency();
    if (!ok) {
        printf("State consistency check FAILED.\n");
        free(g_state);
        return 1;
    }

    // 5. If all is good:
    printf("State consistency check PASSED. Transactions appear applied.\n");

    free(g_state);
    return 0;
}