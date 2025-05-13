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
#include <limits.h> // Required for INT_MAX

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
#define REFERENCE_STATE_FILE    "reference_state.bin"
#define DIFF_OUTPUT_FILE        "state_diff.txt"

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
    uint32_t chunk_idx_in_ring; // Index within the cycle (0 to NUM_STATE_CHUNKS-1)
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

// Structure to hold transaction along with its original batch number for sorting
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
    FILE *diff_fp = fopen(DIFF_OUTPUT_FILE, "w");
    if (!diff_fp) {
        perror("dump_state_diff: Error opening diff output file");
        diff_fp = stderr; // Fallback to stderr
        fprintf(diff_fp, "--- dump_state_diff outputting to stderr due to file open error ---\n");
    }

    fprintf(diff_fp, "--- dump_state_diff starting (comparing %zu accounts, reporting max %zu) ---\n", count, max_report);
    for (size_t i = 0; i < count; ++i) {
        if (a[i] != b[i]) {
                fprintf(diff_fp,
                        "  ◇ MISMATCH @ idx %-8zu  reference = %-12" PRId64
                        "  recovered = %-12" PRId64 "\n",
                        i, a[i], b[i]);
            ++mismatches;
            if (mismatches >= max_report && max_report > 0) { // Check max_report > 0 to allow unlimited reporting if 0
                fprintf(diff_fp, "  ... reporting capped at %zu mismatches ...\n", max_report);
                break;
            }
        }
    }

    if (mismatches == 0) {
        printf("dump_state_diff: States are **identical** (%zu accounts) (Details in %s)\n", count, (diff_fp == stderr ? "stderr" : DIFF_OUTPUT_FILE));
        if (diff_fp != stderr) { // Avoid double printing to console if stderr is the fallback
            fprintf(diff_fp, "dump_state_diff: States are **identical** (%zu accounts)\n", count);
        }
    } else {
        fprintf(diff_fp, // To file
                "dump_state_diff: %zu account(s) differ. Reporting capped at %zu.\n", mismatches, max_report);
        fprintf(stderr, // To console
                "dump_state_diff: %zu account(s) differ. Detailed report in %s\n", mismatches, (diff_fp == stderr ? "stderr" : DIFF_OUTPUT_FILE));

    }
    fprintf(diff_fp, "--- dump_state_diff finished ---\n");

    if (diff_fp != stderr) {
        fclose(diff_fp);
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
        if (state_array[sidx] >= (int64_t)tx->amount) {
            state_array[sidx] -= tx->amount;
            state_array[ridx] += tx->amount;
        }
    } else if (sfunc == 1 && rfunc == 1) { // Range Set
        uint64_t start_idx = sidx;
        uint64_t len       = GET_DATA(tx->receiver);
        if (len == 0) return;
        if (start_idx >= SMALL_ACCOUNT_COUNT || start_idx + len > SMALL_ACCOUNT_COUNT) {
            return;
        }
        for (uint64_t i = 0; i < len; ++i) {
            state_array[start_idx + i] = (int64_t)tx->amount;
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
    const int64_t *source_state_chunk_ptr = full_state_array + (size_t)slot_idx_in_cycle * ACCOUNTS_PER_STATE_CHUNK ;
    nt_memcpy(temp_snap_slot->state_data, source_state_chunk_ptr, ACCOUNTS_PER_STATE_CHUNK * ACCOUNT_SIZE);
    nt_memcpy((char*)mapped_log_region + final_snap_offset, temp_snap_slot, sizeof(SnapshotSlot));

    memset(temp_tx_slot->transactions, 0, BATCH_SIZE * sizeof(Transaction));
    temp_tx_slot->batch_num = current_batch_num;
    temp_tx_slot->tx_count = num_tx_in_current_batch;
    if (num_tx_in_current_batch > 0) {
        nt_memcpy(temp_tx_slot->transactions, current_transaction_batch, num_tx_in_current_batch * sizeof(Transaction));
    }
    nt_memcpy((char*)mapped_log_region + final_tx_offset, temp_tx_slot, sizeof(TxSlot));
}

// --- Recovery Function (User's Specified Algorithm - Refined) ---
int recover_state_from_log(int log_fd, int64_t *restrict state_array_to_recover, int *last_recovered_batch_num) {
    CheckpointHeader header;
    ssize_t bytes = pread(log_fd, &header, sizeof(header), 0);
    if (bytes != (ssize_t)sizeof(header) || header.magic != CHECKPOINT_MAGIC) {
        fprintf(stderr, "Recovery: Invalid or missing checkpoint header. Skipping recovery.\n");
        return -1;
    }
    if (header.num_state_chunks != NUM_STATE_CHUNKS || header.accounts_per_chunk != ACCOUNTS_PER_STATE_CHUNK) {
        fprintf(stderr, "Recovery: Log file parameters mismatch. Cannot recover.\n");
        return -1;
    }

    printf("Recovery: Valid checkpoint header. Oldest cycle: %u. Newest cycle (potential): %u.\n",
           header.oldest_cycle, (header.oldest_cycle + 1) % CYCLES);

    uint32_t oldest_cycle = header.oldest_cycle;
    uint32_t newest_cycle = (oldest_cycle + 1) % CYCLES;
    *last_recovered_batch_num = -1; // Initialize

    SnapshotSlot *temp_snap_slot = malloc(sizeof(SnapshotSlot));
    TxSlot *temp_tx_slot = malloc(sizeof(TxSlot));
    // Array to store the batch number of the snapshot loaded for each chunk
    int *batch_of_snapshot_for_chunk = calloc(NUM_STATE_CHUNKS, sizeof(int));

    if (!temp_snap_slot || !temp_tx_slot || !batch_of_snapshot_for_chunk) {
        perror("Recovery: malloc for slot read/tracking buffers failed");
        free(temp_snap_slot); free(temp_tx_slot); free(batch_of_snapshot_for_chunk);
        return -1;
    }
    for(uint32_t i=0; i<NUM_STATE_CHUNKS; ++i) batch_of_snapshot_for_chunk[i] = -1; // Init to -1 (no snapshot loaded yet)


    // --- Step 1: Determine latest_chunk_saved_in_newest_cycle ---
    int latest_chunk_saved_in_newest_cycle = -1;
    int highest_batch_in_newest_cycle = -1;
    size_t snapshot_slot_array_offset = CHECKPOINT_HEADER_SIZE;

    printf("Recovery Sub-Step 1.1: Scanning newest_cycle (%u) for latest saved chunk...\n", newest_cycle);
    for (uint32_t chunk_k = 0; chunk_k < NUM_STATE_CHUNKS; ++chunk_k) {
        off_t snap_offset_in_file = snapshot_slot_array_offset +
                                   ((off_t)newest_cycle * NUM_STATE_CHUNKS + chunk_k) * sizeof(SnapshotSlot);
        memset(temp_snap_slot, 0, sizeof(SnapshotSlot));
        ssize_t read_bytes = pread(log_fd, temp_snap_slot, sizeof(SnapshotSlot), snap_offset_in_file);

        if (read_bytes == (ssize_t)sizeof(SnapshotSlot)) {
            if ((int)temp_snap_slot->batch_num > highest_batch_in_newest_cycle) {
                highest_batch_in_newest_cycle = (int)temp_snap_slot->batch_num;
                latest_chunk_saved_in_newest_cycle = temp_snap_slot->chunk_idx_in_ring;
            }
        }
    }
    if (latest_chunk_saved_in_newest_cycle != -1) {
         printf("Recovery Sub-Step 1.1: Latest chunk found in newest_cycle (%u) is %d (batch %d).\n",
               newest_cycle, latest_chunk_saved_in_newest_cycle, highest_batch_in_newest_cycle);
    } else {
        printf("Recovery Sub-Step 1.1: No valid chunks found in newest_cycle (%u). Will load all from oldest_cycle.\n", newest_cycle);
    }

    // --- Step 2: Load State Chunks ---
    printf("Recovery Sub-Step 1.2: Loading state chunks...\n");
    memset(state_array_to_recover, 0, PADDED_ACCOUNT_COUNT * ACCOUNT_SIZE);
    int max_loaded_batch_overall = -1;

    if (latest_chunk_saved_in_newest_cycle != -1) {
        printf("  Loading chunks 0 to %d from newest_cycle (%u).\n", latest_chunk_saved_in_newest_cycle, newest_cycle);
        for (uint32_t chunk_k = 0; (int)chunk_k <= latest_chunk_saved_in_newest_cycle; ++chunk_k) {
            off_t snap_offset_in_file = snapshot_slot_array_offset +
                                       ((off_t)newest_cycle * NUM_STATE_CHUNKS + chunk_k) * sizeof(SnapshotSlot);
            memset(temp_snap_slot, 0, sizeof(SnapshotSlot));
            ssize_t read_bytes = pread(log_fd, temp_snap_slot, sizeof(SnapshotSlot), snap_offset_in_file);
            if (read_bytes == (ssize_t)sizeof(SnapshotSlot) && temp_snap_slot->chunk_idx_in_ring == chunk_k) {
                nt_memcpy(state_array_to_recover + (size_t)chunk_k * ACCOUNTS_PER_STATE_CHUNK,
                          temp_snap_slot->state_data, ACCOUNTS_PER_STATE_CHUNK * ACCOUNT_SIZE);
                batch_of_snapshot_for_chunk[chunk_k] = (int)temp_snap_slot->batch_num; // Store batch of loaded snapshot
                if ((int)temp_snap_slot->batch_num > max_loaded_batch_overall) {
                    max_loaded_batch_overall = (int)temp_snap_slot->batch_num;
                }
            } else {
                fprintf(stderr, "Recovery WARNING: Failed to read/validate snapshot for chunk %u from newest_cycle. State zeroed, batch -1.\n", chunk_k);
                // batch_of_snapshot_for_chunk[chunk_k] remains -1
            }
        }
    }

    uint32_t start_chunk_from_oldest = (latest_chunk_saved_in_newest_cycle == -1) ? 0 : (uint32_t)(latest_chunk_saved_in_newest_cycle + 1);
    if (start_chunk_from_oldest < NUM_STATE_CHUNKS) {
        printf("  Loading chunks %u to %u from oldest_cycle (%u).\n", start_chunk_from_oldest, NUM_STATE_CHUNKS - 1, oldest_cycle);
        for (uint32_t chunk_k = start_chunk_from_oldest; chunk_k < NUM_STATE_CHUNKS; ++chunk_k) {
            off_t snap_offset_in_file = snapshot_slot_array_offset +
                                       ((off_t)oldest_cycle * NUM_STATE_CHUNKS + chunk_k) * sizeof(SnapshotSlot);
            memset(temp_snap_slot, 0, sizeof(SnapshotSlot));
            ssize_t read_bytes = pread(log_fd, temp_snap_slot, sizeof(SnapshotSlot), snap_offset_in_file);
            if (read_bytes == (ssize_t)sizeof(SnapshotSlot) && temp_snap_slot->chunk_idx_in_ring == chunk_k) {
                nt_memcpy(state_array_to_recover + (size_t)chunk_k * ACCOUNTS_PER_STATE_CHUNK,
                          temp_snap_slot->state_data, ACCOUNTS_PER_STATE_CHUNK * ACCOUNT_SIZE);
                batch_of_snapshot_for_chunk[chunk_k] = (int)temp_snap_slot->batch_num; // Store batch of loaded snapshot
                if ((int)temp_snap_slot->batch_num > max_loaded_batch_overall) {
                    max_loaded_batch_overall = (int)temp_snap_slot->batch_num;
                }
            } else {
                fprintf(stderr, "Recovery WARNING: Failed to read/validate snapshot for chunk %u from oldest_cycle. State zeroed, batch -1.\n", chunk_k);
                 // batch_of_snapshot_for_chunk[chunk_k] remains -1
            }
        }
    }
    *last_recovered_batch_num = max_loaded_batch_overall;
    printf("Recovery Sub-Step 1.2: Composite snapshot loaded. Effective batch: %d.\n", *last_recovered_batch_num);

    // --- Step 3: Selective Transaction Replay (Refined Temporal Check) ---
    printf("Recovery Step 2: Collecting eligible transactions for selective replay (refined logic)...\n");
    size_t max_possible_txs_to_collect = (size_t)TOTAL_TX_SLOTS * BATCH_SIZE; // Max possible from all slots
    TransactionWithBatchNum *tx_buffer_for_sorting = malloc(max_possible_txs_to_collect * sizeof(TransactionWithBatchNum));
    if (!tx_buffer_for_sorting) {
        perror("Recovery: malloc for tx_buffer_for_sorting failed");
        free(temp_snap_slot); free(temp_tx_slot); free(batch_of_snapshot_for_chunk);
        return -1;
    }
    size_t collected_tx_count = 0;
    size_t tx_slot_array_offset = CHECKPOINT_HEADER_SIZE + TOTAL_SNAPSHOT_SLOTS * sizeof(SnapshotSlot);

    for (uint32_t current_processing_chunk_idx = 0; current_processing_chunk_idx < NUM_STATE_CHUNKS; ++current_processing_chunk_idx) {
        uint32_t source_cycle_for_tx;
        if (latest_chunk_saved_in_newest_cycle != -1 && (int)current_processing_chunk_idx <= latest_chunk_saved_in_newest_cycle) {
            source_cycle_for_tx = newest_cycle;
        } else {
            source_cycle_for_tx = oldest_cycle;
        }

        off_t tx_offset_in_file = tx_slot_array_offset +
                                 ((off_t)source_cycle_for_tx * NUM_STATE_CHUNKS + current_processing_chunk_idx) * sizeof(TxSlot);
        memset(temp_tx_slot, 0, sizeof(TxSlot));
        ssize_t read_bytes = pread(log_fd, temp_tx_slot, sizeof(TxSlot), tx_offset_in_file);

        if (read_bytes == (ssize_t)sizeof(TxSlot)) {
            // This TxSlot is associated with current_processing_chunk_idx.
            // Its batch number is temp_tx_slot->batch_num.

            if (temp_tx_slot->tx_count > BATCH_SIZE) {
                fprintf(stderr, "Recovery WARNING: TxSlot for chunk %u (cycle %u, batch %u) has tx_count %u > BATCH_SIZE. Clamping.\n",
                        current_processing_chunk_idx, source_cycle_for_tx, temp_tx_slot->batch_num, temp_tx_slot->tx_count);
                temp_tx_slot->tx_count = BATCH_SIZE;
            }

            for (uint32_t tx_idx = 0; tx_idx < temp_tx_slot->tx_count; ++tx_idx) {
                const Transaction *tx = &temp_tx_slot->transactions[tx_idx];
                bool apply_this_tx = false;

                uint8_t sfunc = GET_FUNC(tx->sender);
                uint8_t rfunc = GET_FUNC(tx->receiver);
                uint64_t sdata = GET_DATA(tx->sender);
                uint64_t rdata = GET_DATA(tx->receiver);

                // Check sender account
                if (sdata < PADDED_ACCOUNT_COUNT) { // Ensure account index is valid
                    uint32_t s_chunk_affected = sdata / ACCOUNTS_PER_STATE_CHUNK;
                    if (s_chunk_affected < NUM_STATE_CHUNKS && s_chunk_affected <= current_processing_chunk_idx) { // Spatial rule
                        if ((int)temp_tx_slot->batch_num > batch_of_snapshot_for_chunk[s_chunk_affected]) { // Temporal rule
                            apply_this_tx = true;
                        }
                    }
                }

                // Check receiver account (if not already set to apply)
                if (!apply_this_tx && rdata < PADDED_ACCOUNT_COUNT) { // Ensure account index is valid
                     if (sfunc == 0 && rfunc == 0) { // Only for transfers, range set only has one "affected" primary region
                        uint32_t r_chunk_affected = rdata / ACCOUNTS_PER_STATE_CHUNK;
                        if (r_chunk_affected < NUM_STATE_CHUNKS && r_chunk_affected <= current_processing_chunk_idx) { // Spatial rule
                            if ((int)temp_tx_slot->batch_num > batch_of_snapshot_for_chunk[r_chunk_affected]) { // Temporal rule
                                apply_this_tx = true;
                            }
                        }
                     }
                }
                
                // Check range for Range Set
                if (sfunc == 1 && rfunc == 1) {
                    uint64_t start_idx = sdata;
                    uint64_t len = rdata;
                    if (len > 0 && start_idx < PADDED_ACCOUNT_COUNT) {
                        uint64_t end_idx_range = start_idx + len -1;
                        if (end_idx_range >= PADDED_ACCOUNT_COUNT) end_idx_range = PADDED_ACCOUNT_COUNT -1;

                        for (uint64_t acc_idx_in_range = start_idx; acc_idx_in_range <= end_idx_range; ++acc_idx_in_range) {
                            uint32_t current_chunk_affected = acc_idx_in_range / ACCOUNTS_PER_STATE_CHUNK;
                            if (current_chunk_affected < NUM_STATE_CHUNKS && current_chunk_affected <= current_processing_chunk_idx) { // Spatial rule
                                if ((int)temp_tx_slot->batch_num > batch_of_snapshot_for_chunk[current_chunk_affected]) { // Temporal rule
                                    apply_this_tx = true;
                                    break; // Found one affected account in range that needs update
                                }
                            }
                        }
                    }
                }


                if (apply_this_tx) {
                    if (collected_tx_count < max_possible_txs_to_collect) {
                        tx_buffer_for_sorting[collected_tx_count].tx = *tx;
                        tx_buffer_for_sorting[collected_tx_count].original_batch_num = temp_tx_slot->batch_num;
                        collected_tx_count++;
                    } else {
                        fprintf(stderr, "Recovery ERROR: tx_buffer_for_sorting overflowed. Aborting collection.\n");
                        goto end_tx_collection;
                    }
                }
            }
        }
    }
end_tx_collection:;

    if (collected_tx_count > 0) {
        printf("Recovery Step 2: Collected %zu eligible transactions. Sorting and applying...\n", collected_tx_count);
        qsort(tx_buffer_for_sorting, collected_tx_count, sizeof(TransactionWithBatchNum), compare_tx_with_batch_num);

        int highest_replayed_batch = -1;
        for (size_t i = 0; i < collected_tx_count; ++i) {
            apply_transaction_to_state_array(&tx_buffer_for_sorting[i].tx, state_array_to_recover);
            if ((int)tx_buffer_for_sorting[i].original_batch_num > highest_replayed_batch) {
                highest_replayed_batch = (int)tx_buffer_for_sorting[i].original_batch_num;
            }
        }
        if (highest_replayed_batch > *last_recovered_batch_num) {
             *last_recovered_batch_num = highest_replayed_batch;
        }
        printf("Recovery Step 2: Applied %zu transactions. State now reflects batch %d.\n", collected_tx_count, *last_recovered_batch_num);
    } else {
        printf("Recovery Step 2: No eligible transactions found for replay based on refined criteria.\n");
    }

    free(tx_buffer_for_sorting);
    free(temp_snap_slot);
    free(temp_tx_slot);
    free(batch_of_snapshot_for_chunk);

    printf("Recovery finished. Final state reflects batch number: %d\n", *last_recovered_batch_num);
    return 0;
}


// --- State Hashing ---
uint64_t compute_state_hash(const int64_t *state_array, size_t num_accounts_to_hash) {
    uint64_t hash = 0x1234567890ABCDEFULL;
    if (num_accounts_to_hash > PADDED_ACCOUNT_COUNT) {
        num_accounts_to_hash = PADDED_ACCOUNT_COUNT;
    }
    for (size_t i = 0; i < num_accounts_to_hash; i++) {
        hash = (hash << 13) | (hash >> (64 - 13));
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
    if (fflush(file) != 0) { perror("fflush failed for hash file"); }
    int fd = fileno(file);
    if (fd >= 0 && fsync(fd) != 0) { perror("fsync failed for hash file"); }
    fclose(file);
    printf("Saved state hash: 0x%016" PRIx64 " (for state after batch %u) to %s\n", hash_value, batch_num_for_hash, STATE_HASH_FILE);
    return 0;
}

int verify_recovered_state_hash(const int64_t *current_state_array, size_t num_accounts_to_hash, uint32_t batch_num_of_current_state) {
    StateHash hash_data_from_file;
    FILE *file = fopen(STATE_HASH_FILE, "rb");
    if (!file) {
        if (errno == ENOENT) {
            printf("Hash Verify: No previous state hash file '%s' found. Skipping verification.\n", STATE_HASH_FILE);
            return 0;
        }
        perror("Hash Verify: Error opening hash file for reading");
        return -1;
    }
    if (fread(&hash_data_from_file, sizeof(hash_data_from_file), 1, file) != 1) {
        if (feof(file) && ferror(file) == 0) {
             fprintf(stderr, "Hash Verify: State hash file '%s' is empty or too small. Skipping verification.\n", STATE_HASH_FILE);
             fclose(file); return 0;
        } else {
            fprintf(stderr, "Hash Verify: Error reading state hash from file '%s'.\n", STATE_HASH_FILE);
            fclose(file); return -1;
        }
    }
    fclose(file);

    uint64_t computed_hash_of_current_state = compute_state_hash(current_state_array, num_accounts_to_hash);
    printf("Hash Verify: Comparing current state (labeled as batch %u) against saved hash from %s.\n",
           batch_num_of_current_state, STATE_HASH_FILE);
    printf("             Current State Hash: 0x%016" PRIx64 "\n", computed_hash_of_current_state);
    printf("             Saved Hash (batch %u): 0x%016" PRIx64 "\n",
           hash_data_from_file.batch_num_for_hash, hash_data_from_file.hash);

    if (batch_num_of_current_state != hash_data_from_file.batch_num_for_hash) {
        fprintf(stderr, "Hash Verify WARNING: Batch number mismatch! Current state is for batch %u, but saved hash is for batch %u. Cannot verify.\n",
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
    if (st.st_size < (off_t)sizeof(CheckpointHeader)) {
        initialize_header = true;
    } else {
        CheckpointHeader temp_header;
        ssize_t read_bytes = pread(fd, &temp_header, sizeof(temp_header), 0);
        if (read_bytes != sizeof(temp_header) ||
            temp_header.magic != CHECKPOINT_MAGIC ||
            temp_header.num_state_chunks != NUM_STATE_CHUNKS ||
            temp_header.accounts_per_chunk != ACCOUNTS_PER_STATE_CHUNK) {
            initialize_header = true;
        } else {
             printf("Log file %s exists with valid header (Oldest Cycle: %u).\n", filename, temp_header.oldest_cycle);
        }
    }

    if (initialize_header || st.st_size < (off_t)total_log_size) {
        if (st.st_size < (off_t)total_log_size) {
            if (ftruncate(fd, total_log_size) != 0) { perror("ftruncate error"); }
            errno = 0;
            if (posix_fallocate(fd, 0, total_log_size) != 0 && errno != ENOSPC && errno != EINVAL && errno != EOPNOTSUPP) {
                 perror("posix_fallocate error");
            }
        }
        if (initialize_header) {
            printf("Initializing log file header for %s.\n", filename);
            CheckpointHeader h = {CHECKPOINT_MAGIC, 2, 0, NUM_STATE_CHUNKS, ACCOUNTS_PER_STATE_CHUNK};
            if (pwrite(fd, &h, sizeof(h), 0) != sizeof(h)) { perror("Failed to write initial header"); close(fd); exit(EXIT_FAILURE); }
            if (fsync(fd) != 0) { perror("Failed to fsync initial header"); }
            printf("Initialized log file header (Oldest cycle set to 0).\n");
        }
    }
    close(fd);
}

// --- Main Routine ---
int main(int argc, char **argv) {
    bool is_reference_run = false;
    if (argc > 1 && strcmp(argv[1], "saveref") == 0) {
        is_reference_run = true;
        printf("INFO: ***** REFERENCE RUN *****\n");
    } else {
        printf("INFO: ***** DEBUG/RECOVERY RUN *****\n");
    }

    printf("System Config: ACCOUNTS=%lu, ACCOUNT_SIZE=%zu, BATCH_SIZE=%u\n",
            SMALL_ACCOUNT_COUNT, ACCOUNT_SIZE, BATCH_SIZE);
    printf("               CHUNKS=%u, ACC_PER_CHUNK=%u, PADDED_ACCOUNTS=%lu\n",
           NUM_STATE_CHUNKS, ACCOUNTS_PER_STATE_CHUNK, PADDED_ACCOUNT_COUNT);
    
    int64_t *main_state_array = NULL;
    if (posix_memalign((void **)&main_state_array, 64, PADDED_ACCOUNT_COUNT * ACCOUNT_SIZE) != 0) {
        perror("Error allocating main_state_array"); exit(EXIT_FAILURE);
    }

    SnapshotSlot *temp_snap_slot = malloc(sizeof(SnapshotSlot));
    TxSlot *temp_tx_slot = malloc(sizeof(TxSlot));
    if (!temp_snap_slot || !temp_tx_slot) {
        perror("Failed to allocate temporary commit slots");
        free(main_state_array);
        exit(EXIT_FAILURE);
    }

    const size_t TOTAL_LOG_FILE_SIZE = CHECKPOINT_HEADER_SIZE +
                                   TOTAL_SNAPSHOT_SLOTS * sizeof(SnapshotSlot) +
                                   TOTAL_TX_SLOTS * sizeof(TxSlot);
    if (is_reference_run) {
        remove(LOG_FILE); remove(STATE_HASH_FILE); remove(REFERENCE_STATE_FILE);
    }
    preallocate_and_init_log_file(LOG_FILE, TOTAL_LOG_FILE_SIZE);

    double rec_start_t = get_time_ms();
    int log_fd_rec = open(LOG_FILE, O_RDONLY);
    int recovered_batch = -1;
    bool meaningful_state_recovered = false;

    memset(main_state_array, 0, PADDED_ACCOUNT_COUNT * ACCOUNT_SIZE);

    if (log_fd_rec >= 0) {
        if (recover_state_from_log(log_fd_rec, main_state_array, &recovered_batch) == 0) {
            if (recovered_batch >= 0) {
                meaningful_state_recovered = true;
                printf("Recovery successful. State loaded reflects state AFTER batch %d.\n", recovered_batch);
            } else {
                printf("Recovery function finished, but no prior state (batch >= 0) could be established.\n");
                memset(main_state_array, 0, PADDED_ACCOUNT_COUNT * ACCOUNT_SIZE); // Re-zero
            }
        } else {
            fprintf(stderr, "Recovery function reported a critical error.\n");
            memset(main_state_array, 0, PADDED_ACCOUNT_COUNT * ACCOUNT_SIZE); // Re-zero
            recovered_batch = -1;
        }
        close(log_fd_rec);
    } else {
        if (!is_reference_run) { perror("CRITICAL: Failed to open log file for recovery"); exit(EXIT_FAILURE); }
        else { printf("INFO: Log file %s not found (expected for fresh reference run).\n", LOG_FILE); }
    }
    printf("State recovery phase took: %.3f ms. State reflects batch: %d.\n", get_time_ms() - rec_start_t, recovered_batch);

    if (!meaningful_state_recovered) {
        printf("Initializing main state array with default balances as no prior state was recovered.\n");
        for (uint64_t i = 0; i < PADDED_ACCOUNT_COUNT; i++) main_state_array[i] = 1000000;
    } else {
        if (verify_recovered_state_hash(main_state_array, SMALL_ACCOUNT_COUNT, recovered_batch) != 0) {
            if (!is_reference_run) {
                fprintf(stderr, "FATAL: Post-recovery state hash verification FAILED for batch %d. Exiting.\n", recovered_batch);
                 // Load reference state and dump diff if available
                int64_t *ref_state = malloc(PADDED_ACCOUNT_COUNT * ACCOUNT_SIZE);
                if(ref_state) {
                    FILE *f_ref = fopen(REFERENCE_STATE_FILE, "rb");
                    if(f_ref) {
                        if(fread(ref_state, ACCOUNT_SIZE, PADDED_ACCOUNT_COUNT, f_ref) == PADDED_ACCOUNT_COUNT) {
                            dump_state_diff(ref_state, main_state_array, SMALL_ACCOUNT_COUNT, 50);
                        } fclose(f_ref);
                    } free(ref_state);
                }
                exit(EXIT_FAILURE);
            }
        } else {
            printf("Post-recovery state hash verification SUCCEEDED for batch %d.\n", recovered_batch);
        }
         if (!is_reference_run) {
             int64_t *reference_state_array = malloc(PADDED_ACCOUNT_COUNT * ACCOUNT_SIZE);
             if (reference_state_array) {
                 FILE *f_ref = fopen(REFERENCE_STATE_FILE, "rb");
                 if (f_ref) {
                     if (fread(reference_state_array, ACCOUNT_SIZE, PADDED_ACCOUNT_COUNT, f_ref) == PADDED_ACCOUNT_COUNT) {
                         printf("Comparing current main_state_array (post-recovery) with loaded reference state...\n");
                         dump_state_diff(reference_state_array, main_state_array, SMALL_ACCOUNT_COUNT, 50);
                     } fclose(f_ref);
                 } else if (errno != ENOENT) { perror("DEBUG RUN: Error opening reference state file");}
                 free(reference_state_array);
             }
         }
    }

    int log_fd_mmap = open(LOG_FILE, O_RDWR);
    if (log_fd_mmap < 0) { perror("CRITICAL: Failed to re-open log file for mmap"); exit(EXIT_FAILURE); }
    void *mapped_log_region = mmap(NULL, TOTAL_LOG_FILE_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED, log_fd_mmap, 0);
    if (mapped_log_region == MAP_FAILED) { perror("mmap failed"); close(log_fd_mmap); exit(EXIT_FAILURE); }
    CheckpointHeader *log_header_ptr = (CheckpointHeader*)mapped_log_region;
    if (log_header_ptr->magic != CHECKPOINT_MAGIC) {
        fprintf(stderr, "CRITICAL: Log file header invalid after mmap!\n"); exit(EXIT_FAILURE);
    }

    pthread_t msync_pthread;
    msync_thread_data msync_data = { mapped_log_region, TOTAL_LOG_FILE_SIZE, 1 };
    if (pthread_create(&msync_pthread, NULL, msync_thread_func, &msync_data) != 0) {
        perror("Error creating msync thread"); munmap(mapped_log_region, TOTAL_LOG_FILE_SIZE); close(log_fd_mmap); exit(EXIT_FAILURE);
    }

    Transaction *tx_batch_buf = malloc(BATCH_SIZE * sizeof(Transaction));
    if (!tx_batch_buf) { perror("Failed to allocate tx_batch_buf"); /* proper cleanup */ exit(EXIT_FAILURE); }
    FILE *fp_tx = fopen(TX_FILE, "rb");
    if (!fp_tx) { perror("Error opening transactions file"); /* proper cleanup */ exit(EXIT_FAILURE); }

    uint32_t starting_batch_num_from_tx_file = meaningful_state_recovered ? (uint32_t)recovered_batch + 1 : 0;
    if (meaningful_state_recovered && starting_batch_num_from_tx_file > 0) {
        off_t tx_file_offset = (off_t)starting_batch_num_from_tx_file * BATCH_SIZE * sizeof(Transaction);
        if (fseeko(fp_tx, tx_file_offset, SEEK_SET) != 0) {
             perror("fseeko failed on transaction file"); /* proper cleanup */ exit(EXIT_FAILURE);
        }
    }
    
    uint32_t current_batch_num_in_loop = starting_batch_num_from_tx_file;
    uint32_t file_batches_processed_this_run_count = 0;
    int last_batch_processed_this_run = recovered_batch;
    double proc_start_t_loop = get_time_ms();

    printf("Starting transaction processing loop from batch %u...\n", current_batch_num_in_loop);
    while(1) {
        size_t num_tx_read = fread(tx_batch_buf, sizeof(Transaction), BATCH_SIZE, fp_tx);
        if (num_tx_read == 0) {
            if(feof(fp_tx)) printf("End of transaction file %s reached.\n", TX_FILE);
            else perror("Error reading transaction file");
            break;
        }
        for (size_t k = 0; k < num_tx_read; ++k) {
            apply_transaction_to_state_array(&tx_batch_buf[k], main_state_array);
        }
        uint32_t chkpt_cycle = (current_batch_num_in_loop / NUM_STATE_CHUNKS) % CYCLES;
        uint32_t slot_in_cycle = current_batch_num_in_loop % NUM_STATE_CHUNKS;
        commit_batch_data_to_log(chkpt_cycle, slot_in_cycle, current_batch_num_in_loop,
                                 main_state_array, tx_batch_buf, num_tx_read,
                                 mapped_log_region, temp_snap_slot, temp_tx_slot);
        if (slot_in_cycle == (NUM_STATE_CHUNKS - 1)) {
            log_header_ptr->oldest_cycle = chkpt_cycle;
            if (msync((void*)log_header_ptr, CHECKPOINT_HEADER_SIZE, MS_SYNC) != 0) {
                 perror("CRITICAL: Failed to msync header update for oldest_cycle");
            }
            printf("Batch %u: Cycle %u fully checkpointed. Header oldest_cycle updated to %u.\n",
                   current_batch_num_in_loop, chkpt_cycle, log_header_ptr->oldest_cycle);
        }
        if ((current_batch_num_in_loop % 100) == 0 || num_tx_read < BATCH_SIZE) {
            printf("Processed batch %u (%zu tx read).\n", current_batch_num_in_loop, num_tx_read);
        }
        last_batch_processed_this_run = current_batch_num_in_loop;
        file_batches_processed_this_run_count++;
        current_batch_num_in_loop++;
        if (num_tx_read < BATCH_SIZE) break;
    }
    printf("--- Finished processing %s (%u batches) in %.3f ms ---\n", TX_FILE, file_batches_processed_this_run_count, get_time_ms() - proc_start_t_loop);
    printf("Final state in memory reflects batch: %d\n", last_batch_processed_this_run);

    int batch_for_final_hash_label = last_batch_processed_this_run;
    if (batch_for_final_hash_label >= 0) {
        printf("Computing and saving final state hash for batch %d...\n", batch_for_final_hash_label);
        uint64_t final_hash = compute_state_hash(main_state_array, SMALL_ACCOUNT_COUNT);
        save_state_hash_to_file(final_hash, batch_for_final_hash_label);
        if (is_reference_run) {
            printf("REFERENCE RUN: Saving current main state array to %s\n", REFERENCE_STATE_FILE);
            FILE* f_ref_save = fopen(REFERENCE_STATE_FILE, "wb");
            if (f_ref_save) {
                fwrite(main_state_array, ACCOUNT_SIZE, PADDED_ACCOUNT_COUNT, f_ref_save);
                if (fflush(f_ref_save) != 0) { perror("fflush failed for reference state file");}
                int fd_ref = fileno(f_ref_save);
                if (fd_ref >= 0 && fsync(fd_ref) != 0) { perror("fsync failed for reference state file");}
                fclose(f_ref_save);
            } else { perror("REFERENCE RUN: Error opening reference state file for writing"); }
        }
    }

    msync_data.running = 0;
    pthread_join(msync_pthread, NULL);
    printf("Background msync thread joined.\n");
    if (msync(mapped_log_region, TOTAL_LOG_FILE_SIZE, MS_SYNC) != 0) { perror("Final msync of log data failed"); }
    if (fsync(log_fd_mmap) != 0) { perror("Final fsync of log_fd_mmap failed"); }
    munmap(mapped_log_region, TOTAL_LOG_FILE_SIZE);
    if(fp_tx) fclose(fp_tx);
    if(log_fd_mmap >=0) close(log_fd_mmap);
    free(tx_batch_buf); free(temp_snap_slot); free(temp_tx_slot); free(main_state_array);
    printf("Execution complete.\n");
    return 0;
}
