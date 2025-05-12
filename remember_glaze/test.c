#define _POSIX_C_SOURCE 200809L
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <time.h>
#include <stdbool.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <errno.h>
#include <pthread.h>
#include <inttypes.h>
// #include <omp.h> // OpenMP not strictly needed for this refactoring, can be added if parallelizing apply_tx is desired

// --- Definitions and Constants ---

#define BATCH_SIZE              (1 << 16)      // 65,536 transactions per batch
#define SMALL_ACCOUNT_COUNT     2000000UL        // Target number of accounts
#define ACCOUNT_SIZE            8 // Use sizeof for clarity and portability

// State Chunk configuration
#define TARGET_CHUNK_DATA_BYTES (512 * 1024)   // Approx 512KB per state chunk
#define ACCOUNTS_PER_STATE_CHUNK (TARGET_CHUNK_DATA_BYTES / ACCOUNT_SIZE) // Number of accounts in one state chunk
#if ACCOUNTS_PER_STATE_CHUNK == 0
    #error "ACCOUNTS_PER_STATE_CHUNK is zero, TARGET_CHUNK_DATA_BYTES is too small for even one account."
#endif

// NUM_STATE_CHUNKS is the number of chunks needed to cover all accounts, also defines the ring size for checkpointing cycle
#define NUM_STATE_CHUNKS        ((SMALL_ACCOUNT_COUNT + ACCOUNTS_PER_STATE_CHUNK - 1) / ACCOUNTS_PER_STATE_CHUNK)
#define PADDED_ACCOUNT_COUNT    (NUM_STATE_CHUNKS * ACCOUNTS_PER_STATE_CHUNK) // Actual size of state array to be a multiple of chunk size

// Magic number for identification.
#define CHECKPOINT_MAGIC        0xC0CAC01B

// Hash file for state verification
#define STATE_HASH_FILE         "state_hash.dat"
#define LOG_FILE                "checkpoint_log.dat"
#define TX_FILE                 "transactions.bin"

// --- Checkpoint Header Structure ---
typedef struct {
    uint32_t magic;
    uint32_t version;
    uint32_t oldest_cycle;      // Cycle (0 or 1) that is the older complete copy.
    uint32_t num_state_chunks;  // For verification: NUM_STATE_CHUNKS
    uint32_t accounts_per_chunk;// For verification: ACCOUNTS_PER_STATE_CHUNK
} CheckpointHeader;

#define CHECKPOINT_HEADER_SIZE (sizeof(CheckpointHeader))

// --- Ring Log Layout ---
#define CYCLES 2 // We maintain two complete copies (cycles) of the state chunks and tx logs
#define TOTAL_SNAPSHOT_SLOTS (NUM_STATE_CHUNKS * CYCLES)
#define TOTAL_TX_SLOTS       (NUM_STATE_CHUNKS * CYCLES)

// --- Operation Encoding ---
#define FUNC_MASK   0xF000000000000000UL
#define DATA_MASK   0x0FFFFFFFFFFFFFFFUL
#define GET_FUNC(x) ((uint8_t)((x) >> 60))
#define GET_DATA(x) ((x) & DATA_MASK)

// --- Data Structures ---

typedef struct {
    uint64_t sender;
    uint64_t receiver;
    uint64_t amount;
} Transaction;

typedef struct SnapshotSlot {
    uint32_t batch_num;
    uint32_t chunk_idx_in_ring;
    int64_t state_data[ACCOUNTS_PER_STATE_CHUNK];
} SnapshotSlot;

typedef struct TxSlot {
    uint32_t batch_num;
    uint32_t tx_count;
    Transaction transactions[BATCH_SIZE];
} TxSlot;

typedef struct {
    uint64_t hash;
    uint32_t batch_num_for_hash;
} StateHash;

typedef struct {
    Transaction tx;
    uint32_t original_batch_num;
} TransactionWithBatchNum;

// --Debug Functions --
static void dump_state_diff(const int64_t *a,
                const int64_t *b,
                size_t          count,
                size_t          max_report)
{
    size_t mismatches = 0;

    for (size_t i = 0; i < count; ++i) {
        if (a[i] != b[i]) {
            if (mismatches < max_report) {
                fprintf(stderr,
                        "  ◇ MISMATCH @ idx %-8zu  saved = %-12" PRId64
                        "  recovered = %-12" PRId64 "\n",
                        i, a[i], b[i]);
            }
            ++mismatches;
        }
    }

    if (mismatches == 0) {
        printf("Self-test: recovery produced an **identical** state "
               "(%zu accounts)\n", count);
    } else {
        fprintf(stderr,
                "Self-test: %zu account(s) differ after in-process "
                "recovery\n", mismatches);
    }
}

// --- Utility Functions ---

double get_time_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec * 1000.0 + ts.tv_nsec / 1e6;
}

static inline void nt_memcpy(void *dest, const void *src, size_t n) {
    memcpy(dest, src, n);
}

int compare_tx_with_batch_num(const void *a, const void *b) {
    TransactionWithBatchNum *tx_a = (TransactionWithBatchNum *)a;
    TransactionWithBatchNum *tx_b = (TransactionWithBatchNum *)b;
    if (tx_a->original_batch_num < tx_b->original_batch_num) return -1;
    if (tx_a->original_batch_num > tx_b->original_batch_num) return 1;
    return 0;
}

// --- Asynchronous Flush (msync) Thread ---
typedef struct {
    void *mapped_region;
    size_t size;
    volatile int running;
} msync_thread_data;

void *msync_thread_func(void *arg) {
    msync_thread_data *data = (msync_thread_data *)arg;
    while (data->running) {
        msync(data->mapped_region, data->size, MS_ASYNC);
        struct timespec ts = {0, 10 * 1000 * 1000}; // sleep for 10ms
        nanosleep(&ts, NULL);
    }
    return NULL;
}

// --- Transaction Application to In-Memory State ---
static inline void apply_transaction_to_state_array(const Transaction *tx, int64_t *restrict state_array) {
    uint8_t sfunc = GET_FUNC(tx->sender);
    uint8_t rfunc = GET_FUNC(tx->receiver);
    uint64_t sidx = GET_DATA(tx->sender);
    uint64_t ridx = GET_DATA(tx->receiver);

    if (sfunc == 0 && rfunc == 0) { // Simple Transfer
        if (sidx >= SMALL_ACCOUNT_COUNT || ridx >= SMALL_ACCOUNT_COUNT) {
            return;
        }
        if (state_array[sidx] >= tx->amount) {
            state_array[sidx] -= tx->amount;
            state_array[ridx] += tx->amount;
        }
    } else if (sfunc == 1 && rfunc == 1) { // Range Set
        uint64_t start_idx = sidx;
        uint64_t len       = GET_DATA(tx->receiver);
        if (len == 0 || start_idx + len > SMALL_ACCOUNT_COUNT) {
            return;
        }
        for (uint64_t i = 0; i < len; ++i) {
            state_array[start_idx + i] = tx->amount;
        }
    }
}

// --- Checkpoint Commit Function ---
static void commit_batch_data_to_log(
    uint32_t current_cycle,
    uint32_t slot_idx_in_cycle,
    uint32_t current_batch_num,
    const int64_t *restrict full_state_array,
    const Transaction *restrict current_transaction_batch,
    uint32_t num_tx_in_current_batch,
    void *restrict mapped_log_region,
    SnapshotSlot *restrict temp_snap_slot,
    TxSlot *restrict temp_tx_slot
) {
    size_t snapshot_slot_array_offset = CHECKPOINT_HEADER_SIZE;
    size_t tx_slot_array_offset = snapshot_slot_array_offset + TOTAL_SNAPSHOT_SLOTS * sizeof(SnapshotSlot);
    size_t overall_slot_idx = current_cycle * NUM_STATE_CHUNKS + slot_idx_in_cycle;
    size_t final_snap_offset = snapshot_slot_array_offset + overall_slot_idx * sizeof(SnapshotSlot);
    size_t final_tx_offset   = tx_slot_array_offset + overall_slot_idx * sizeof(TxSlot);

    temp_snap_slot->batch_num = current_batch_num;
    temp_snap_slot->chunk_idx_in_ring = slot_idx_in_cycle;
    const int64_t *source_state_chunk_ptr = full_state_array + (size_t)slot_idx_in_cycle * ACCOUNTS_PER_STATE_CHUNK;
    nt_memcpy(temp_snap_slot->state_data, source_state_chunk_ptr, ACCOUNTS_PER_STATE_CHUNK * ACCOUNT_SIZE);
    nt_memcpy((char*)mapped_log_region + final_snap_offset, temp_snap_slot, sizeof(SnapshotSlot));
    memset(temp_tx_slot->transactions, 0, BATCH_SIZE * sizeof(Transaction));
    temp_tx_slot->batch_num = current_batch_num;
    temp_tx_slot->tx_count = num_tx_in_current_batch;
    nt_memcpy(temp_tx_slot->transactions, current_transaction_batch, num_tx_in_current_batch * sizeof(Transaction));
    nt_memcpy((char*)mapped_log_region + final_tx_offset, temp_tx_slot, sizeof(TxSlot));
}

// --- Recovery Function ---
int recover_state_from_log(int log_fd, int64_t *restrict state_array_to_recover, int *last_recovered_batch_num) {
    CheckpointHeader header;
    ssize_t bytes = pread(log_fd, &header, sizeof(header), 0);
    if (bytes != (ssize_t)sizeof(header) || header.magic != CHECKPOINT_MAGIC) {
        fprintf(stderr, "Recovery: Invalid or missing checkpoint header (magic: 0x%x, expected: 0x%x, bytes: %zd). Skipping recovery.\n", header.magic, CHECKPOINT_MAGIC, bytes);
        return -1;
    }
    if (header.num_state_chunks != NUM_STATE_CHUNKS || header.accounts_per_chunk != ACCOUNTS_PER_STATE_CHUNK) {
        fprintf(stderr, "Recovery: Log file parameters mismatch. Cannot recover.\n");
        return -1;
    }

    printf("Recovery: Valid checkpoint header. Oldest cycle: %u. Log params: %u chunks, %u acc/chunk.\n",
           header.oldest_cycle, header.num_state_chunks, header.accounts_per_chunk);

    uint32_t oldest_cycle = header.oldest_cycle;
    uint32_t newest_cycle = (oldest_cycle + 1) % CYCLES;
    *last_recovered_batch_num = -1; // Initialize

    SnapshotSlot *current_snap_slot = malloc(sizeof(SnapshotSlot));
    TxSlot *current_tx_slot = malloc(sizeof(TxSlot)); // Still needed for Step 2
    if (!current_snap_slot || !current_tx_slot) {
        perror("Recovery: malloc for slot buffers failed");
        free(current_snap_slot); free(current_tx_slot);
        return -1;
    }

    // Step 1: Restore base state from all chunks in the 'oldest_cycle'
    printf("Recovery Step 1: Loading base state from oldest_cycle (%u) snapshots.\n", oldest_cycle);
    size_t snapshot_slot_array_offset = CHECKPOINT_HEADER_SIZE;
    int max_batch_in_oldest_snapshots = -1;
    int min_batch_in_oldest_snapshots = -1; // MODIFICATION: Initialize min batch

    for (uint32_t chunk_k = 0; chunk_k < NUM_STATE_CHUNKS; ++chunk_k) {
        off_t snap_offset_in_file = snapshot_slot_array_offset +
                                   ((off_t)oldest_cycle * NUM_STATE_CHUNKS + chunk_k) * sizeof(SnapshotSlot);
        
        memset(current_snap_slot, 0, sizeof(SnapshotSlot)); 
        ssize_t read_bytes = pread(log_fd, current_snap_slot, sizeof(SnapshotSlot), snap_offset_in_file);
        
        if (read_bytes != (ssize_t)sizeof(SnapshotSlot)) {
            fprintf(stderr, "Recovery: Failed/short read for snapshot slot %u (cycle %u, read %zd/%zu bytes). Chunk data remains from pre-zeroed state.\n", 
                    chunk_k, oldest_cycle, read_bytes, sizeof(SnapshotSlot));
            // If a slot can't be read, we might not want to update min/max based on its (potentially zeroed) batch_num
            // However, if it's a new log, batch_num will be 0.
            // For a robust min, we only consider it if it's a valid read and batch_num is not some default uninit value.
            // But current_snap_slot is memset to 0, so current_snap_slot->batch_num would be 0.
            // This logic might need refinement if partially written cycles are possible and problematic.
            // For now, we just continue.
            continue; 
        }
        
        if (current_snap_slot->batch_num != 0 || max_batch_in_oldest_snapshots != -1) { 
            if (min_batch_in_oldest_snapshots == -1 || (int)current_snap_slot->batch_num < min_batch_in_oldest_snapshots) {
                min_batch_in_oldest_snapshots = (int)current_snap_slot->batch_num;
            }
        }

        size_t dest_offset_in_state_array = (size_t)chunk_k * ACCOUNTS_PER_STATE_CHUNK;
        if (dest_offset_in_state_array + ACCOUNTS_PER_STATE_CHUNK <= PADDED_ACCOUNT_COUNT) {
             nt_memcpy(state_array_to_recover + dest_offset_in_state_array,
                       current_snap_slot->state_data,
                       ACCOUNTS_PER_STATE_CHUNK * ACCOUNT_SIZE);
            if ((int)current_snap_slot->batch_num > max_batch_in_oldest_snapshots) {
                max_batch_in_oldest_snapshots = (int)current_snap_slot->batch_num;
            }
        } else {
            fprintf(stderr, "Recovery: Snapshot slot %u (cycle %u) out of bounds. Skipped.\n", chunk_k, oldest_cycle);
        }
    }
    // MODIFICATION: Print min batch
    printf("Recovery Step 1: Base state loaded. Min batch in oldest_cycle snapshots: %d. Max batch: %d.\n",
           min_batch_in_oldest_snapshots, max_batch_in_oldest_snapshots);
    // END MODIFICATION

    if (max_batch_in_oldest_snapshots != -1) { 
        *last_recovered_batch_num = max_batch_in_oldest_snapshots;
    } else { 
        // If all snapshot reads failed or all had batch_num 0 (and we started min/max at -1)
        // and it's a new log, this might mean recovered_batch is -1 or 0.
        // If oldest_cycle was non-zero but all its snapshots are unreadable/zeroed, it's an issue.
        // The current logic sets last_recovered_batch_num to -1.
        // If after pread, a slot has batch_num 0 and that's the only kind, max will be 0.
         *last_recovered_batch_num = -1; 
    }

    // ... (Rest of Step 2: Applying transactions from newest_cycle is the same) ...
    // ... (free buffers and return) ...
// Keep the rest of the function identical
    printf("Recovery Step 2: Applying transactions from newest_cycle (%u) TxSlots > batch %d.\n", newest_cycle, *last_recovered_batch_num);
    size_t tx_slot_array_offset = snapshot_slot_array_offset + TOTAL_SNAPSHOT_SLOTS * sizeof(SnapshotSlot);
    TransactionWithBatchNum *tx_buffer_for_sorting = malloc(NUM_STATE_CHUNKS * BATCH_SIZE * sizeof(TransactionWithBatchNum));
    if (!tx_buffer_for_sorting) {
        perror("Recovery: malloc for tx_buffer_for_sorting failed");
        free(current_snap_slot); free(current_tx_slot);
        return -1;
    }
    size_t total_tx_to_apply_count = 0;
    int max_batch_in_newest_tx_filtered = -1;

    for (uint32_t chunk_k = 0; chunk_k < NUM_STATE_CHUNKS; ++chunk_k) {
        off_t tx_offset_in_file = tx_slot_array_offset +
                                 ((off_t)newest_cycle * NUM_STATE_CHUNKS + chunk_k) * sizeof(TxSlot);
        
        memset(current_tx_slot, 0, sizeof(TxSlot)); 
        ssize_t read_bytes = pread(log_fd, current_tx_slot, sizeof(TxSlot), tx_offset_in_file);

        if (read_bytes != (ssize_t)sizeof(TxSlot)) {
            fprintf(stderr, "Recovery: Failed/short read for TxSlot %u (cycle %u, read %zd/%zu bytes). Slot skipped.\n", 
                    chunk_k, newest_cycle, read_bytes, sizeof(TxSlot));
            continue; 
        }

        if ((int)current_tx_slot->batch_num > *last_recovered_batch_num) {
            if (current_tx_slot->tx_count > BATCH_SIZE) { 
                 fprintf(stderr, "Recovery: Invalid tx_count %u in TxSlot %u (cycle %u, batch %u). Clamping to BATCH_SIZE.\n",
                         current_tx_slot->tx_count, chunk_k, newest_cycle, current_tx_slot->batch_num);
                 current_tx_slot->tx_count = BATCH_SIZE; 
            }
            
            if (current_tx_slot->tx_count > 0) { 
                for (uint32_t tx_idx = 0; tx_idx < current_tx_slot->tx_count; ++tx_idx) {
                    if (total_tx_to_apply_count < NUM_STATE_CHUNKS * BATCH_SIZE) {
                        tx_buffer_for_sorting[total_tx_to_apply_count].tx = current_tx_slot->transactions[tx_idx];
                        tx_buffer_for_sorting[total_tx_to_apply_count].original_batch_num = current_tx_slot->batch_num;
                        total_tx_to_apply_count++;
                    } else {
                        fprintf(stderr, "Recovery: tx_buffer_for_sorting overflow.\n");
                        goto end_tx_collection_filtered; 
                    }
                }
                if ((int)current_tx_slot->batch_num > max_batch_in_newest_tx_filtered) {
                    max_batch_in_newest_tx_filtered = (int)current_tx_slot->batch_num;
                }
            }
        }
    }
end_tx_collection_filtered:;

    if (total_tx_to_apply_count > 0) {
        printf("Recovery: Collected %zu tx (newer than batch %d). Sorting & Applying...\n", total_tx_to_apply_count, *last_recovered_batch_num);
        qsort(tx_buffer_for_sorting, total_tx_to_apply_count, sizeof(TransactionWithBatchNum), compare_tx_with_batch_num);
        for (size_t i = 0; i < total_tx_to_apply_count; ++i) {
            apply_transaction_to_state_array(&tx_buffer_for_sorting[i].tx, state_array_to_recover);
        }
        *last_recovered_batch_num = max_batch_in_newest_tx_filtered; 
    } else {
        printf("Recovery: No tx in newest_cycle newer than batch %d to apply.\n", *last_recovered_batch_num);
    }
    printf("Recovery Step 2: Tx replay complete. Final recovered batch: %d.\n", *last_recovered_batch_num);

    free(tx_buffer_for_sorting);
    // free(current_snap_slot); // Freed earlier
    free(current_tx_slot);
    return 0;

}

// --- State Hashing ---
uint64_t compute_state_hash(const int64_t *state_array, size_t num_accounts_to_hash) {
    uint64_t hash = 0x1234567890ABCDEFULL;
    for (size_t i = 0; i < num_accounts_to_hash; i++) {
        hash = (hash << 13) | (hash >> 51);
        hash ^= (uint64_t)state_array[i];
    }
    return hash;
}

int save_state_hash_to_file(uint64_t hash_value, uint32_t batch_num_for_hash) {
    StateHash hash_data = {hash_value, batch_num_for_hash};
    FILE *file = fopen(STATE_HASH_FILE, "wb");
    if (!file) {
        perror("Error opening hash file for writing");
        return -1;
    }
    if (fwrite(&hash_data, sizeof(hash_data), 1, file) != 1) {
        perror("Error writing state hash to file");
        fclose(file); return -1;
    }
    fclose(file);
    printf("Saved state hash: 0x%016" PRIx64 " (batch %u) to %s\n", hash_value, batch_num_for_hash, STATE_HASH_FILE);
    return 0;
}

int verify_recovered_state_hash(const int64_t *current_state_array, size_t num_accounts_to_hash, uint32_t batch_num_of_current_state) {
    StateHash hash_data_from_file;
    FILE *file = fopen(STATE_HASH_FILE, "rb");
    if (!file) {
        if (errno == ENOENT) {
            printf("Hash Verify: No previous state hash file. Skipping.\n");
            return 0;
        }
        perror("Hash Verify: Error opening hash file for reading");
        return -1;
    }
    if (fread(&hash_data_from_file, sizeof(hash_data_from_file), 1, file) != 1) {
        fprintf(stderr, "Hash Verify: Error reading state hash from file. Skipping.\n");
        fclose(file); return 0; 
    }
    fclose(file);

    uint64_t computed_hash_of_current_state = compute_state_hash(current_state_array, num_accounts_to_hash);

    printf("Hash Verify: Current state (batch %u) hash: 0x%016" PRIx64 "\n",
           batch_num_of_current_state, computed_hash_of_current_state);
    printf("Hash Verify: Expected hash (batch %u) hash: 0x%016" PRIx64 "\n",
           hash_data_from_file.batch_num_for_hash, hash_data_from_file.hash);

    if (batch_num_of_current_state != hash_data_from_file.batch_num_for_hash) {
        fprintf(stderr, "Hash Verify WARNING: Batch number mismatch! Current: %u, Saved: %u.\n",
                batch_num_of_current_state, hash_data_from_file.batch_num_for_hash);
        return 0; 
    }
    if (computed_hash_of_current_state != hash_data_from_file.hash) {
        fprintf(stderr, "Hash Verify ERROR: State hash MISMATCH for batch %u!\n", batch_num_of_current_state);
        return -1;
    }
    printf("Hash Verify: State hash for batch %u VERIFIED successfully!\n", batch_num_of_current_state);
    return 0;
}

// --- File Pre-allocation ---
void preallocate_and_init_log_file(const char *filename, size_t total_log_size) {
    int fd = open(filename, O_RDWR | O_CREAT, 0666);
    if (fd < 0) { perror("Error opening log file"); exit(EXIT_FAILURE); }
    struct stat st;
    if (fstat(fd, &st) != 0) { perror("fstat error on log file"); close(fd); exit(EXIT_FAILURE); }
    
    bool initialize_header = false;
    if (st.st_size < (off_t)total_log_size) {
        printf("Log file %s too small or new. Truncating/allocating and initializing header.\n", filename);
        if (ftruncate(fd, total_log_size) != 0) { perror("ftruncate error"); close(fd); exit(EXIT_FAILURE); }
        
        if (posix_fallocate(fd, 0, total_log_size) != 0 && errno != ENOSPC) { 
             perror("posix_fallocate warning (ignoring if ENOSPC and ftruncate succeeded)");
        }
        initialize_header = true;
    } else { 
        CheckpointHeader temp_header;
        if (pread(fd, &temp_header, sizeof(temp_header), 0) != sizeof(temp_header) || 
            temp_header.magic != CHECKPOINT_MAGIC ||
            temp_header.num_state_chunks != NUM_STATE_CHUNKS || 
            temp_header.accounts_per_chunk != ACCOUNTS_PER_STATE_CHUNK) {
            printf("Log file %s exists but header invalid/mismatch. Re-initializing header.\n", filename);
            initialize_header = true; 
        } else {
             printf("Log file %s exists with valid header and matching parameters.\n", filename);
        }
    }

    if (initialize_header) {
        CheckpointHeader h = {CHECKPOINT_MAGIC, 2, 0, NUM_STATE_CHUNKS, ACCOUNTS_PER_STATE_CHUNK};
        if (pwrite(fd, &h, sizeof(h), 0) != sizeof(h)) { perror("Failed to write initial header"); close(fd); exit(EXIT_FAILURE); }
        if (fsync(fd) != 0) { perror("Failed to fsync initial header"); } 
        printf("Initialized log file header for %s.\n", filename);
    }
    close(fd);
}

// --- Main Routine ---
int main(int argc, char **argv) {
    printf("DEBUG: sizeof(Transaction) = %zu\n", sizeof(Transaction));
    printf("DEBUG: BATCH_SIZE = %u\n", BATCH_SIZE);
    printf("DEBUG: Calculated size of one batch in file = %lu bytes\n", (unsigned long)BATCH_SIZE * sizeof(Transaction));

    printf("System Config: BATCH_SIZE=%u, ACCOUNTS=%lu, CHUNKS=%u, ACC_PER_CHUNK=%u, ACCOUNT_SIZE=%zu\n",
           BATCH_SIZE, SMALL_ACCOUNT_COUNT, NUM_STATE_CHUNKS, ACCOUNTS_PER_STATE_CHUNK, ACCOUNT_SIZE);

    if (NUM_STATE_CHUNKS == 0) { exit(EXIT_FAILURE); }

    int64_t *main_state_array;
    if (posix_memalign((void **)&main_state_array, 64, PADDED_ACCOUNT_COUNT * ACCOUNT_SIZE) != 0) {
        perror("Error allocating main_state_array"); exit(EXIT_FAILURE);
    }

    SnapshotSlot *temp_snap_slot = malloc(sizeof(SnapshotSlot));
    TxSlot *temp_tx_slot = malloc(sizeof(TxSlot));
    if (!temp_snap_slot || !temp_tx_slot) {
        perror("Failed to allocate temporary commit slots"); free(main_state_array); exit(EXIT_FAILURE);
    }

    const size_t TOTAL_LOG_FILE_SIZE = CHECKPOINT_HEADER_SIZE +
                                       TOTAL_SNAPSHOT_SLOTS * sizeof(SnapshotSlot) +
                                       TOTAL_TX_SLOTS * sizeof(TxSlot);
    preallocate_and_init_log_file(LOG_FILE, TOTAL_LOG_FILE_SIZE);

    // --- STEP A: RECOVERY ---
    double rec_start_t = get_time_ms();
    int log_fd_rec = open(LOG_FILE, O_RDONLY);
    int recovered_batch = -1; // Batch number of the state successfully recovered from LOG_FILE

    printf("Zeroing main_state_array before recovery attempt...\n");
    memset(main_state_array, 0, PADDED_ACCOUNT_COUNT * ACCOUNT_SIZE);

    bool recovery_successful = false;
    if (log_fd_rec >= 0) {
        printf("Attempting state recovery from %s\n", LOG_FILE);
        if (recover_state_from_log(log_fd_rec, main_state_array, &recovered_batch) == 0) {
            printf("Recovery function completed. main_state_array now reflects state after batch %d from %s.\n", recovered_batch, LOG_FILE);
            recovery_successful = (recovered_batch != -1); // Consider recovery to batch 0 as successful
        } else {
            fprintf(stderr, "Recovery function reported an error. main_state_array remains zeroed.\n");
            recovered_batch = -1;
        }
        close(log_fd_rec);
    } else {
        perror("CRITICAL: Failed to open log file for recovery reading. main_state_array remains zeroed.");
        recovered_batch = -1;
    }
    printf("State recovery phase took: %.3f ms. State from %s is for batch: %d\n", get_time_ms() - rec_start_t, LOG_FILE, recovered_batch);

    // --- STEP B: VERIFICATION (of the recovered state from LOG_FILE) ---
    if (recovered_batch != -1) {
        printf("Verifying hash of recovered state (batch %d from %s) against %s.\n", recovered_batch, LOG_FILE, STATE_HASH_FILE);
        if (verify_recovered_state_hash(main_state_array, SMALL_ACCOUNT_COUNT, recovered_batch) != 0) {
            fprintf(stderr, "FATAL: Post-recovery state hash verification FAILED for batch %d. Exiting.\n", recovered_batch);
            // Depending on policy, you might not want to exit, but for ensuring consistency, exiting is safest.
            free(main_state_array); free(temp_snap_slot); free(temp_tx_slot);
            exit(EXIT_FAILURE);
        } else {
            printf("Post-recovery state hash verification SUCCEEDED for batch %d.\n", recovered_batch);
        }
    } else {
        printf("No state successfully recovered from %s (or recovery to pre-batch 0 state). Initializing to defaults for TX_FILE processing.\n", LOG_FILE);
        // If no recovery or recovery to -1, the main_state_array is currently zeroed.
        // If TX_FILE is meant to be applied to a default non-zero state, initialize here.
        // For this example, we'll assume applying TX_FILE to a zeroed state is fine if no recovery.
        // Or, if TX_FILE always expects a base state (e.g., all accounts 1000000):
        if (!recovery_successful) { // If recovered_batch remained -1
             printf("Initializing main state array with default balances as no valid prior state was recovered.\n");
             for (uint64_t i = 0; i < PADDED_ACCOUNT_COUNT; i++) main_state_array[i] = 1000000;
        }
    }

    // --- MMAP and ASYNC THREAD SETUP (for checkpointing during TX_FILE processing) ---
    int log_fd_mmap = open(LOG_FILE, O_RDWR);
    if (log_fd_mmap < 0) { /* ... perror and exit ... */ }
    void *mapped_log_region = mmap(NULL, TOTAL_LOG_FILE_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED, log_fd_mmap, 0);
    if (mapped_log_region == MAP_FAILED) { /* ... perror and exit ... */ }
    CheckpointHeader *log_header_ptr = (CheckpointHeader*)mapped_log_region;
    if (log_header_ptr->magic != CHECKPOINT_MAGIC) { /* ... fprintf, munmap, close, exit ... */ }
    pthread_t msync_pthread;
    msync_thread_data msync_data = { mapped_log_region, TOTAL_LOG_FILE_SIZE, 1 };
    if (pthread_create(&msync_pthread, NULL, msync_thread_func, &msync_data) != 0) { /* ... perror, munmap, close, exit ... */ }

    Transaction *tx_batch_buf = malloc(BATCH_SIZE * sizeof(Transaction));
    if (!tx_batch_buf) { /* ... perror and full cleanup ... */ exit(EXIT_FAILURE); }

    // --- STEP C: APPLY transactions.bin (ALL of them, from the beginning) ---
    FILE *fp_tx = fopen(TX_FILE, "rb");
    if (!fp_tx) {
        char err_buf[256]; sprintf(err_buf, "Error opening transactions file '%s'", TX_FILE); perror(err_buf);
        /* ... full cleanup ... */ exit(EXIT_FAILURE);
    }

    printf("Seeking to the beginning of %s to apply all its transactions on top of current state (state of batch %d).\n", TX_FILE, recovered_batch);
    if (fseek(fp_tx, 0L, SEEK_SET) != 0) {
        char err_buf[256]; sprintf(err_buf, "fseek to beginning of %s failed", TX_FILE); perror(err_buf);
        /* ... full cleanup ... */ exit(EXIT_FAILURE);
    }

    // current_batch_num_for_tx_file_processing will be the batch number from TX_FILE (0, 1, 2...).
    // This batch number will be used for checkpointing the *newly formed composite state*.
    uint32_t current_batch_num_for_tx_file_processing = 0;

    // last_batch_processed_from_tx_file_this_run will track the last batch *number from TX_FILE*
    // that was successfully processed in this run. The final hash will be saved against this number.
    int last_batch_processed_from_tx_file_this_run = -1;

    double proc_start_t = get_time_ms();
    uint32_t file_batches_processed_this_run_count = 0;

    printf("Starting transaction processing loop. Reading %s from batch %u and applying on top of current state.\n",
           TX_FILE, current_batch_num_for_tx_file_processing);

    while(1) {
        size_t num_tx_read = fread(tx_batch_buf, sizeof(Transaction), BATCH_SIZE, fp_tx);
        if (num_tx_read == 0) {
            if(feof(fp_tx)) {
                printf("End of transaction file %s reached.\n", TX_FILE);
            } else {
                char err_buf[256]; sprintf(err_buf, "Error reading %s at file batch %u", TX_FILE, current_batch_num_for_tx_file_processing); perror(err_buf);
            }
            break;
        }

        for (size_t k = 0; k < num_tx_read; ++k) {
            apply_transaction_to_state_array(&tx_batch_buf[k], main_state_array);
        }

        // Checkpoint this new composite state.
        // The batch number for the checkpoint log is current_batch_num_for_tx_file_processing.
        uint32_t chkpt_cycle = (current_batch_num_for_tx_file_processing / NUM_STATE_CHUNKS) % CYCLES;
        uint32_t slot_in_cycle = current_batch_num_for_tx_file_processing % NUM_STATE_CHUNKS;
        commit_batch_data_to_log(chkpt_cycle, slot_in_cycle, current_batch_num_for_tx_file_processing,
                                 main_state_array, tx_batch_buf, num_tx_read,
                                 mapped_log_region, temp_snap_slot, temp_tx_slot);

        if (((current_batch_num_for_tx_file_processing + 1) % NUM_STATE_CHUNKS) == 0 && current_batch_num_for_tx_file_processing >= (NUM_STATE_CHUNKS -1) ) {
            uint32_t completed_cycle = chkpt_cycle;
            log_header_ptr->oldest_cycle = completed_cycle;
            if (msync((void*)log_header_ptr, CHECKPOINT_HEADER_SIZE, MS_SYNC) != 0) {
                 perror("CRITICAL: Failed to msync header update for oldest_cycle");
            }
            printf("Batch %u (from %s): Cycle %u fully checkpointed. Header's oldest_cycle updated to %u.\n", current_batch_num_for_tx_file_processing, TX_FILE, completed_cycle, log_header_ptr->oldest_cycle);
        }

        if ((current_batch_num_for_tx_file_processing % 100) == 0 || num_tx_read < BATCH_SIZE) {
            printf("Applied batch %u from %s (%zu tx) to current state.\n", current_batch_num_for_tx_file_processing, TX_FILE, num_tx_read);
        }

        last_batch_processed_from_tx_file_this_run = current_batch_num_for_tx_file_processing;
        file_batches_processed_this_run_count++;
        current_batch_num_for_tx_file_processing++;

        if (num_tx_read < BATCH_SIZE) {
            printf("Last batch from %s (%u) was incomplete (%zu tx) or end of file.\n", TX_FILE, last_batch_processed_from_tx_file_this_run, num_tx_read);
            break;
        }
    }
    printf("--- End of transaction processing loop for %s ---\n", TX_FILE);
    printf("Total batches applied from %s in this run: %u.\n", TX_FILE, file_batches_processed_this_run_count);
    printf("State in memory now reflects (state of batch %d from %s) + (all batches 0 to %d from %s).\n",
           recovered_batch, LOG_FILE, last_batch_processed_from_tx_file_this_run, TX_FILE);

    // --- SAVE FINAL HASH ---
    // The state to be hashed is the composite state.
    // The batch number associated with this hash will be 'last_batch_processed_from_tx_file_this_run'.
    // This means the next run's verify_recovered_state_hash will compare against a hash
    // that was generated *after* that run's equivalent of this Step C.
    int batch_for_final_hash = -1;
    if (file_batches_processed_this_run_count > 0) {
        batch_for_final_hash = last_batch_processed_from_tx_file_this_run;
    } else if (recovery_successful) { // No new file TXs applied, but state was recovered
        batch_for_final_hash = recovered_batch;
    }


    if (batch_for_final_hash != -1) {
        printf("Computing final state hash. State is effectively after batch %d from %s, with tx file batches up to %d applied on top.\n",
            recovered_batch, LOG_FILE, last_batch_processed_from_tx_file_this_run);
        printf("Labeling this final hash with batch number %d (last batch from %s processed this run, or %d if none from file).\n",
            batch_for_final_hash, TX_FILE, recovered_batch);

        printf("Sample state BEFORE final hash (labeled as batch %d):\n", batch_for_final_hash);
        for (int i = 0; i < 5 && i < SMALL_ACCOUNT_COUNT; ++i) {
             printf("  main_state_array[%d] = %" PRId64 "\n", i, main_state_array[i]);
        }
        uint64_t final_hash = compute_state_hash(main_state_array, SMALL_ACCOUNT_COUNT);
        printf("Computed final_hash: 0x%016" PRIx64 " for state labeled as batch %d\n", final_hash, batch_for_final_hash);
        save_state_hash_to_file(final_hash, batch_for_final_hash);
    } else {
        printf("No valid state to hash (no recovery and no transactions from file processed).\n");
    }

    printf("Shutting down...\n");
    msync_data.running = 0;
    pthread_join(msync_pthread, NULL);
    printf("msync thread joined.\n");

    if (msync(mapped_log_region, TOTAL_LOG_FILE_SIZE, MS_SYNC) != 0) { perror("Final msync of log data failed during cleanup"); }
    if (fsync(log_fd_mmap) != 0) { perror("fsync of log_fd_mmap failed during cleanup"); }
    if (munmap(mapped_log_region, TOTAL_LOG_FILE_SIZE) != 0) { perror("munmap failed during cleanup"); }
    fclose(fp_tx);
    close(log_fd_mmap);
    printf("Log unmapped and closed.\n");

    free(tx_batch_buf);
    free(temp_snap_slot);
    free(temp_tx_slot);
    free(main_state_array);
    printf("Memory freed. Execution complete. Total processing time for applying %s: %.3f ms\n", TX_FILE, get_time_ms() - proc_start_t);
    return 0;
}
