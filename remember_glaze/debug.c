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
        diff_fp = stderr;
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
            if (mismatches >= max_report) {
                fprintf(diff_fp, "  ... reporting capped at %zu mismatches ...\n", max_report);
                break;
            }
        }
    }

    if (mismatches == 0) {
        printf("dump_state_diff: States are **identical** (%zu accounts) (Details in %s)\n", count, DIFF_OUTPUT_FILE);
        if (diff_fp != stderr) {
            fprintf(diff_fp, "dump_state_diff: States are **identical** (%zu accounts)\n", count);
        }
    } else {
        fprintf(diff_fp,
                "dump_state_diff: %zu account(s) differ. Reporting capped at %zu.\n", mismatches, max_report);
        fprintf(stderr, "dump_state_diff: %zu account(s) differ. Detailed report in %s\n", mismatches, DIFF_OUTPUT_FILE);

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

// Non-temporal memcpy might not be beneficial here unless chunks are huge and L1/L2 cache pollution is proven issue.
// Using standard memcpy is safer and clearer.
static inline void nt_memcpy(void *dest, const void *src, size_t n) {
    memcpy(dest, src, n);
}

// Comparison function for qsort, sorting transactions by their original batch number
int compare_tx_with_batch_num(const void *a, const void *b) {
    TransactionWithBatchNum *tx_a = (TransactionWithBatchNum *)a;
    TransactionWithBatchNum *tx_b = (TransactionWithBatchNum *)b;
    if (tx_a->original_batch_num < tx_b->original_batch_num) return -1;
    if (tx_a->original_batch_num > tx_b->original_batch_num) return 1;
    // If batch numbers are the same, maintain original order (stable sort not guaranteed by qsort, but usually okay)
    // If a strict order within a batch is needed, add comparison based on original position/index.
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
        // Asynchronous flush - tells the kernel to start writing, doesn't wait
        msync(data->mapped_region, data->size, MS_ASYNC);
        struct timespec ts = {0, 10 * 1000 * 1000}; // sleep for 10ms
        nanosleep(&ts, NULL);
    }
    // Final synchronous flush before exiting thread might be useful
    // msync(data->mapped_region, data->size, MS_SYNC);
    return NULL;
}

// --- Transaction Application to In-Memory State ---
static inline void apply_transaction_to_state_array(const Transaction *tx, int64_t *restrict state_array) {
    uint8_t sfunc = GET_FUNC(tx->sender);
    uint8_t rfunc = GET_FUNC(tx->receiver);
    uint64_t sidx = GET_DATA(tx->sender);
    uint64_t ridx = GET_DATA(tx->receiver);

    // Ensure indices are within the valid range *before* accessing the array
    if (sfunc == 0 && rfunc == 0) { // Simple Transfer
        if (sidx >= SMALL_ACCOUNT_COUNT || ridx >= SMALL_ACCOUNT_COUNT) {
             // fprintf(stderr, "Warning: Transfer tx skipped, invalid index (s:%lu, r:%lu)\n", sidx, ridx);
            return;
        }
        // Check sender balance *before* modifying receiver to prevent overdraft creation
        if (state_array[sidx] >= (int64_t)tx->amount) {
            state_array[sidx] -= tx->amount;
            state_array[ridx] += tx->amount;
        } else {
            // fprintf(stderr, "Warning: Transfer tx skipped, insufficient funds (s:%lu has %ld, needs %lu)\n", sidx, state_array[sidx], tx->amount);
        }
    } else if (sfunc == 1 && rfunc == 1) { // Range Set
        uint64_t start_idx = sidx;
        uint64_t len       = GET_DATA(tx->receiver);
        if (len == 0) {
            // fprintf(stderr, "Warning: RangeSet tx skipped, len is zero\n");
             return; // Zero length does nothing
        }
        // Check bounds carefully: start index must be valid, end index must not exceed count
        if (start_idx >= SMALL_ACCOUNT_COUNT || start_idx + len > SMALL_ACCOUNT_COUNT) {
            // fprintf(stderr, "Warning: RangeSet tx skipped, invalid range (start:%lu, len:%lu)\n", start_idx, len);
            return;
        }
        for (uint64_t i = 0; i < len; ++i) {
            state_array[start_idx + i] = (int64_t)tx->amount; // Cast amount
        }
    }
    // else: Handle other potential function codes if added later
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
    SnapshotSlot *restrict temp_snap_slot, // Pre-allocated temporary buffer
    TxSlot *restrict temp_tx_slot       // Pre-allocated temporary buffer
) {
    // Calculate offsets within the mapped log file
    size_t snapshot_slot_array_offset = CHECKPOINT_HEADER_SIZE;
    size_t tx_slot_array_offset = snapshot_slot_array_offset + TOTAL_SNAPSHOT_SLOTS * sizeof(SnapshotSlot);

    // Calculate the index for the current slot in the flat array (across both cycles)
    size_t overall_slot_idx = current_cycle * NUM_STATE_CHUNKS + slot_idx_in_cycle;

    // Calculate the final byte offsets in the mapped region
    size_t final_snap_offset = snapshot_slot_array_offset + overall_slot_idx * sizeof(SnapshotSlot);
    size_t final_tx_offset   = tx_slot_array_offset + overall_slot_idx * sizeof(TxSlot);

    // --- Prepare and write SnapshotSlot ---
    temp_snap_slot->batch_num = current_batch_num;
    temp_snap_slot->chunk_idx_in_ring = slot_idx_in_cycle; // Store the index within the cycle

    // Pointer to the start of the relevant chunk data in the main state array
    const int64_t *source_state_chunk_ptr = full_state_array + (size_t)slot_idx_in_cycle * ACCOUNTS_PER_STATE_CHUNK ;

    // Copy the state chunk data into the temporary buffer
    nt_memcpy(temp_snap_slot->state_data, source_state_chunk_ptr, ACCOUNTS_PER_STATE_CHUNK * ACCOUNT_SIZE);

    // Copy the temporary buffer to the mapped log file region
    nt_memcpy((char*)mapped_log_region + final_snap_offset, temp_snap_slot, sizeof(SnapshotSlot));

    // --- Prepare and write TxSlot ---
    // Zero out the transaction part of the temp buffer first (important if num_tx < BATCH_SIZE)
    memset(temp_tx_slot->transactions, 0, BATCH_SIZE * sizeof(Transaction));

    temp_tx_slot->batch_num = current_batch_num;
    temp_tx_slot->tx_count = num_tx_in_current_batch;

    // Copy the actual transactions for this batch into the temporary buffer
    if (num_tx_in_current_batch > 0) {
        nt_memcpy(temp_tx_slot->transactions, current_transaction_batch, num_tx_in_current_batch * sizeof(Transaction));
    }

    // Copy the temporary buffer to the mapped log file region
    nt_memcpy((char*)mapped_log_region + final_tx_offset, temp_tx_slot, sizeof(TxSlot));

    // Note: The actual persistence relies on the msync thread or explicit msync calls later.
}

// --- Recovery Function (Revised) ---
int recover_state_from_log(int log_fd, int64_t *restrict state_array_to_recover, int *last_recovered_batch_num) {
    CheckpointHeader header;
    ssize_t bytes = pread(log_fd, &header, sizeof(header), 0);
    if (bytes != (ssize_t)sizeof(header) || header.magic != CHECKPOINT_MAGIC) {
        fprintf(stderr, "Recovery: Invalid or missing checkpoint header (magic: 0x%x, expected: 0x%x, bytes: %zd). Skipping recovery.\n",
                (bytes > 0 ? header.magic : 0), CHECKPOINT_MAGIC, bytes);
        return -1; // Cannot proceed without a valid header
    }
    if (header.num_state_chunks != NUM_STATE_CHUNKS || header.accounts_per_chunk != ACCOUNTS_PER_STATE_CHUNK) {
        fprintf(stderr, "Recovery: Log file parameters mismatch (Log: %u chunks, %u acc/chunk; Expected: %u chunks, %u acc/chunk). Cannot recover.\n",
                header.num_state_chunks, header.accounts_per_chunk,
                NUM_STATE_CHUNKS, ACCOUNTS_PER_STATE_CHUNK);
        return -1; // Configuration mismatch
    }

    printf("Recovery: Valid checkpoint header found. Oldest cycle: %u. Log params: %u chunks, %u acc/chunk.\n",
           header.oldest_cycle, header.num_state_chunks, header.accounts_per_chunk);

    uint32_t oldest_cycle = header.oldest_cycle;
    *last_recovered_batch_num = -1; // Initialize to "no state recovered yet"

    // Allocate temporary buffers for reading slots
    SnapshotSlot *current_snap_slot = malloc(sizeof(SnapshotSlot));
    TxSlot *current_tx_slot = malloc(sizeof(TxSlot));
    if (!current_snap_slot || !current_tx_slot) {
        perror("Recovery: malloc for slot read buffers failed");
        free(current_snap_slot); // free whichever one succeeded, if any
        free(current_tx_slot);
        return -1;
    }

    // --- Step 1: Load base state from oldest_cycle snapshots ---
    printf("Recovery Step 1: Loading base state from oldest_cycle (%u) snapshots.\n", oldest_cycle);
    size_t snapshot_slot_array_offset = CHECKPOINT_HEADER_SIZE;
    int min_batch_in_oldest = INT_MAX; // Use INT_MAX for minimum tracking
    int max_batch_in_oldest = -1;      // Use -1 for maximum tracking
    bool oldest_cycle_valid = false;   // Flag to check if we found any valid data

    for (uint32_t chunk_k = 0; chunk_k < NUM_STATE_CHUNKS; ++chunk_k) {
        // Calculate offset for the snapshot slot in the oldest cycle
        off_t snap_offset_in_file = snapshot_slot_array_offset +
                                   ((off_t)oldest_cycle * NUM_STATE_CHUNKS + chunk_k) * sizeof(SnapshotSlot);

        // Read the snapshot slot directly from the file descriptor
        memset(current_snap_slot, 0, sizeof(SnapshotSlot)); // Clear buffer before read
        ssize_t read_bytes = pread(log_fd, current_snap_slot, sizeof(SnapshotSlot), snap_offset_in_file);

        if (read_bytes != (ssize_t)sizeof(SnapshotSlot)) {
            fprintf(stderr, "Recovery WARNING: Failed or short read for snapshot slot %u (cycle %u, offset %ld, read %zd/%zu bytes). Chunk data may be stale/zeroed.\n",
                    chunk_k, oldest_cycle, (long)snap_offset_in_file, read_bytes, sizeof(SnapshotSlot));
            // Do NOT update min/max batch based on this potentially corrupt slot
            // The state array was already zeroed, so this chunk remains zeroed if read fails.
            continue; // Skip to the next chunk
        }

        // Check if the chunk index in the slot matches the expected index (sanity check)
        if (current_snap_slot->chunk_idx_in_ring != chunk_k) {
             fprintf(stderr, "Recovery WARNING: Snapshot slot %u (cycle %u) has unexpected chunk_idx_in_ring %u. Using data anyway.\n",
                     chunk_k, oldest_cycle, current_snap_slot->chunk_idx_in_ring);
             // Decide whether to trust the data or skip; here we trust it but warn.
        }


        // Copy data into the main recovery array
        size_t dest_offset_in_state_array = (size_t)chunk_k * ACCOUNTS_PER_STATE_CHUNK;
        // Check bounds before memcpy
        if (dest_offset_in_state_array + ACCOUNTS_PER_STATE_CHUNK <= PADDED_ACCOUNT_COUNT) {
             nt_memcpy(state_array_to_recover + dest_offset_in_state_array,
                       current_snap_slot->state_data,
                       ACCOUNTS_PER_STATE_CHUNK * ACCOUNT_SIZE);

             // Update min and max batch numbers encountered in this cycle's snapshots
             // Only consider valid (non-zero?) batch numbers? Let's assume batch 0 is valid.
             if ((int)current_snap_slot->batch_num < min_batch_in_oldest) {
                 min_batch_in_oldest = (int)current_snap_slot->batch_num;
             }
             if ((int)current_snap_slot->batch_num > max_batch_in_oldest) {
                 max_batch_in_oldest = (int)current_snap_slot->batch_num;
             }
             oldest_cycle_valid = true; // Mark that we found at least one valid snapshot
        } else {
            fprintf(stderr, "Recovery ERROR: Calculated destination offset for snapshot chunk %u is out of bounds (%zu). Skipping chunk.\n",
                    chunk_k, dest_offset_in_state_array);
            // This indicates a fundamental config mismatch, should ideally not happen if header check passed.
        }
    }

    if (!oldest_cycle_valid) {
        fprintf(stderr, "Recovery WARNING: No valid snapshot data found in the oldest cycle (%u). State remains zeroed. Cannot determine replay start point.\n", oldest_cycle);
        // If no snapshots, min/max remain at initial values. Recovery effectively fails.
        free(current_snap_slot);
        free(current_tx_slot);
        *last_recovered_batch_num = -1;
        return 0; // Return 0, indicating function finished, but recovery didn't yield a state.
    }

    // Handle case where only batch 0 snapshots were found
    if (min_batch_in_oldest == INT_MAX) min_batch_in_oldest = -1; // If loop finished but min never updated

    printf("Recovery Step 1: Base state loaded from oldest cycle (%u). Min batch: %d, Max batch: %d.\n",
           oldest_cycle, min_batch_in_oldest, max_batch_in_oldest);


    // --- Step 2: Collect all transactions after the *minimum* batch number from the base state ---
    int replay_start_batch_num = min_batch_in_oldest; // Replay transactions strictly AFTER this batch

    printf("Recovery Step 2: Collecting transactions from ALL TxSlots with batch_num > %d.\n", replay_start_batch_num);

    // Calculate the theoretical maximum number of transactions we might need to store
    size_t max_possible_txs = (size_t)TOTAL_TX_SLOTS * BATCH_SIZE;
    TransactionWithBatchNum *tx_buffer_for_sorting = malloc(max_possible_txs * sizeof(TransactionWithBatchNum));
    if (!tx_buffer_for_sorting) {
        perror("Recovery: malloc for large tx_buffer_for_sorting failed");
        free(current_snap_slot); free(current_tx_slot);
        return -1; // Critical error
    }

    size_t total_tx_to_apply_count = 0;
    int max_batch_overall_collected = replay_start_batch_num; // Track the highest batch number we actually collect for replay

    size_t tx_slot_array_offset = CHECKPOINT_HEADER_SIZE + TOTAL_SNAPSHOT_SLOTS * sizeof(SnapshotSlot);

    // Iterate through ALL transaction slots in the log file (both cycles)
    for (uint32_t cycle = 0; cycle < CYCLES; ++cycle) {
        for (uint32_t chunk_k = 0; chunk_k < NUM_STATE_CHUNKS; ++chunk_k) {
            // Calculate offset for the transaction slot
            off_t tx_offset_in_file = tx_slot_array_offset +
                                     ((off_t)cycle * NUM_STATE_CHUNKS + chunk_k) * sizeof(TxSlot);

            // Read the transaction slot
            memset(current_tx_slot, 0, sizeof(TxSlot)); // Clear buffer
            ssize_t read_bytes = pread(log_fd, current_tx_slot, sizeof(TxSlot), tx_offset_in_file);

            if (read_bytes != (ssize_t)sizeof(TxSlot)) {
                 // This might be expected if the log isn't full, but log a warning if non-zero bytes were read
                if (read_bytes > 0) {
                    fprintf(stderr, "Recovery WARNING: Failed or short read for TxSlot %u (cycle %u, offset %ld, read %zd/%zu bytes). Slot skipped.\n",
                            chunk_k, cycle, (long)tx_offset_in_file, read_bytes, sizeof(TxSlot));
                } else if (read_bytes < 0) {
                    perror("Recovery Error reading TxSlot");
                }
                continue; // Skip this slot
            }

            // Check if this transaction batch needs to be replayed
            if ((int)current_tx_slot->batch_num > replay_start_batch_num) {
                // Validate the transaction count in the slot
                if (current_tx_slot->tx_count > BATCH_SIZE) {
                     fprintf(stderr, "Recovery WARNING: Invalid tx_count %u in TxSlot %u (cycle %u, batch %u). Clamping to BATCH_SIZE (%d).\n",
                             current_tx_slot->tx_count, chunk_k, cycle, current_tx_slot->batch_num, BATCH_SIZE);
                     current_tx_slot->tx_count = BATCH_SIZE; // Clamp to avoid buffer issues
                }

                // Copy transactions from this slot to the main buffer if count > 0
                if (current_tx_slot->tx_count > 0) {
                    for (uint32_t tx_idx = 0; tx_idx < current_tx_slot->tx_count; ++tx_idx) {
                        // Check if we have space in the buffer
                        if (total_tx_to_apply_count < max_possible_txs) {
                            tx_buffer_for_sorting[total_tx_to_apply_count].tx = current_tx_slot->transactions[tx_idx];
                            tx_buffer_for_sorting[total_tx_to_apply_count].original_batch_num = current_tx_slot->batch_num;
                            total_tx_to_apply_count++;
                        } else {
                            fprintf(stderr, "Recovery ERROR: tx_buffer_for_sorting overflowed! Max size %zu reached. Aborting collection.\n", max_possible_txs);
                            // Free buffers and return error
                            free(tx_buffer_for_sorting);
                            free(current_snap_slot); free(current_tx_slot);
                            return -1; // Critical error
                        }
                    }

                    // Update the maximum batch number seen among collected transactions
                    if ((int)current_tx_slot->batch_num > max_batch_overall_collected) {
                        max_batch_overall_collected = (int)current_tx_slot->batch_num;
                    }
                }
            } // end if batch_num > replay_start_batch_num
        } // end loop chunk_k
    } // end loop cycle

    // --- Step 3: Sort and Apply Collected Transactions ---
    if (total_tx_to_apply_count > 0) {
        printf("Recovery Step 3: Collected %zu transactions (from batches > %d). Sorting & Applying...\n",
               total_tx_to_apply_count, replay_start_batch_num);

        // Sort the collected transactions by their original batch number
        qsort(tx_buffer_for_sorting, total_tx_to_apply_count, sizeof(TransactionWithBatchNum), compare_tx_with_batch_num);

        // Apply the sorted transactions to the state array
        double apply_start_t = get_time_ms();
        for (size_t i = 0; i < total_tx_to_apply_count; ++i) {
            apply_transaction_to_state_array(&tx_buffer_for_sorting[i].tx, state_array_to_recover);
            // Optional: Log progress every N transactions if it takes long
            // if ((i + 1) % 100000 == 0) {
            //     printf("  ... applied %zu / %zu transactions ...\n", i + 1, total_tx_to_apply_count);
            // }
        }
        double apply_end_t = get_time_ms();
        printf("Recovery Step 3: Applied %zu transactions in %.3f ms.\n", total_tx_to_apply_count, apply_end_t - apply_start_t);

        // The final state corresponds to the latest batch number applied
        *last_recovered_batch_num = max_batch_overall_collected;

    } else {
        printf("Recovery Step 3: No transactions found with batch_num > %d. No replay needed.\n", replay_start_batch_num);
        // If no replay happened, the state is consistent up to the maximum batch found in the oldest cycle snapshots
        *last_recovered_batch_num = max_batch_in_oldest;
    }

    printf("Recovery finished. Final state reflects batch number: %d\n", *last_recovered_batch_num);

    // Cleanup
    free(tx_buffer_for_sorting);
    free(current_snap_slot);
    free(current_tx_slot);

    // Check if we actually managed to recover a meaningful state
    if (*last_recovered_batch_num < 0 && oldest_cycle_valid) {
        // This could happen if oldest cycle only contained batch 0, and no newer tx were found.
        // Set recovered batch to 0 in this specific case.
        if (max_batch_in_oldest == 0) {
             *last_recovered_batch_num = 0;
             printf("Adjusted recovered batch to 0 as only batch 0 snapshots were found and no replay occurred.\n");
        } else {
             printf("Recovery completed, but final recovered batch number is negative (%d) despite valid oldest cycle snapshots found (max_batch %d). State might be inconsistent if snapshots were very old.\n", *last_recovered_batch_num, max_batch_in_oldest);
             // Keep it negative to signal potential issues downstream? Or set to max_batch_in_oldest?
             // Let's keep it negative to signal uncertainty.
        }
    } else if (*last_recovered_batch_num < 0 && !oldest_cycle_valid) {
        // No valid snapshots found, state is zeroed, recovered_batch is -1. Correct.
         printf("Recovery determined no valid prior state could be loaded.\n");
    }


    return 0; // Indicate recovery function completed execution
}


// --- State Hashing ---
uint64_t compute_state_hash(const int64_t *state_array, size_t num_accounts_to_hash) {
    // Simple XOR-based rolling hash (example) - replace with a better one if needed (e.g., MurmurHash, SHA)
    uint64_t hash = 0x1234567890ABCDEFULL; // Initial seed
    // Clamp num_accounts_to_hash to prevent reading beyond buffer in case PADDED > SMALL
    if (num_accounts_to_hash > PADDED_ACCOUNT_COUNT) {
         fprintf(stderr,"compute_state_hash WARNING: num_accounts_to_hash (%zu) > PADDED_ACCOUNT_COUNT (%lu). Clamping.\n", num_accounts_to_hash, PADDED_ACCOUNT_COUNT);
        num_accounts_to_hash = PADDED_ACCOUNT_COUNT;
    }

    for (size_t i = 0; i < num_accounts_to_hash; i++) {
        // Combine state value with hash - using a simple rotate and XOR
        hash = (hash << 13) | (hash >> (64 - 13)); // Rotate left 13 bits
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
    // Ensure data is written to disk
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
            return 0; // Not an error if file doesn't exist (e.g., first run)
        }
        perror("Hash Verify: Error opening hash file for reading");
        return -1; // Error opening existing file
    }
    if (fread(&hash_data_from_file, sizeof(hash_data_from_file), 1, file) != 1) {
        if (feof(file) && ferror(file) == 0) { // Check if EOF was reached cleanly
             fprintf(stderr, "Hash Verify: State hash file '%s' is empty or too small (read 0 bytes). Skipping verification.\n", STATE_HASH_FILE);
             fclose(file);
             return 0; // Treat as no hash available
        } else { // Actual read error occurred
            fprintf(stderr, "Hash Verify: Error reading state hash from file '%s'.\n", STATE_HASH_FILE);
            fclose(file);
            return -1; // Read error
        }
    }
    fclose(file);

    // Compute hash of the current in-memory state
    uint64_t computed_hash_of_current_state = compute_state_hash(current_state_array, num_accounts_to_hash);

    printf("Hash Verify: Comparing current state (labeled as batch %u) against saved hash from %s.\n",
           batch_num_of_current_state, STATE_HASH_FILE);
    printf("             Current State Hash: 0x%016" PRIx64 "\n", computed_hash_of_current_state);
    printf("             Saved Hash (batch %u): 0x%016" PRIx64 "\n",
           hash_data_from_file.batch_num_for_hash, hash_data_from_file.hash);

    // Check if the batch numbers match - essential for valid comparison
    if (batch_num_of_current_state != hash_data_from_file.batch_num_for_hash) {
        fprintf(stderr, "Hash Verify WARNING: Batch number mismatch! Current state is for batch %u, but saved hash in %s is for batch %u. Cannot verify.\n",
                batch_num_of_current_state, STATE_HASH_FILE, hash_data_from_file.batch_num_for_hash);
        return 0; // Cannot verify if batch numbers differ, but not necessarily an error in recovery itself.
    }

    // Compare the hashes
    if (computed_hash_of_current_state != hash_data_from_file.hash) {
        fprintf(stderr, "Hash Verify ERROR: State hash MISMATCH for batch %u! Recovered state does not match saved hash.\n", batch_num_of_current_state);
        return -1; // Hashes differ - indicates a problem
    }

    printf("Hash Verify: State hash for batch %u VERIFIED successfully!\n", batch_num_of_current_state);
    return 0; // Verification successful
}


// --- File Pre-allocation ---
void preallocate_and_init_log_file(const char *filename, size_t total_log_size) {
    int fd = open(filename, O_RDWR | O_CREAT, 0666);
    if (fd < 0) { perror("Error opening log file"); exit(EXIT_FAILURE); }
    struct stat st;
    if (fstat(fd, &st) != 0) { perror("fstat error on log file"); close(fd); exit(EXIT_FAILURE); }

    bool initialize_header = false;
    // Check if file is too small OR header is invalid/mismatched
    if (st.st_size < (off_t)sizeof(CheckpointHeader)) {
        printf("Log file %s too small for header or new. Will initialize/truncate.\n", filename);
        initialize_header = true;
    } else {
        CheckpointHeader temp_header;
        ssize_t read_bytes = pread(fd, &temp_header, sizeof(temp_header), 0);
        if (read_bytes != sizeof(temp_header) ||
            temp_header.magic != CHECKPOINT_MAGIC ||
            temp_header.num_state_chunks != NUM_STATE_CHUNKS ||
            temp_header.accounts_per_chunk != ACCOUNTS_PER_STATE_CHUNK) {
            printf("Log file %s exists but header invalid or parameters mismatch (Read %zdB, Magic 0x%x, Chunks %u, Acc/Chunk %u). Re-initializing header.\n",
                   filename, read_bytes, (read_bytes > 0 ? temp_header.magic : 0),
                   (read_bytes > 0 ? temp_header.num_state_chunks : 0), (read_bytes > 0 ? temp_header.accounts_per_chunk : 0) );
            initialize_header = true;
        } else {
             printf("Log file %s exists with valid header and matching parameters (Oldest Cycle: %u).\n", filename, temp_header.oldest_cycle);
        }
    }

    // Ensure file has the correct size and write initial header if needed
    if (initialize_header || st.st_size < (off_t)total_log_size) {
        // Ensure size first
        if (st.st_size < (off_t)total_log_size) {
             printf("Log file %s current size %lld, target size %zu. Allocating space...\n", filename, (long long)st.st_size, total_log_size);
            // Use posix_fallocate for efficient space reservation
             // ftruncate first sets the logical size, fallocate reserves blocks
            if (ftruncate(fd, total_log_size) != 0) {
                 perror("ftruncate error during preallocation");
                 // Consider fallback or exit
             }
            // posix_fallocate might fail on some filesystems (e.g. tmpfs) or if out of space
            // EINVAL might mean not supported, ENOSPC out of space. Warn but continue.
            errno = 0; // Clear errno before call
            if (posix_fallocate(fd, 0, total_log_size) != 0) {
                if (errno == ENOSPC) {
                     perror("posix_fallocate warning (out of space)");
                } else if (errno == EINVAL || errno == EOPNOTSUPP) {
                    // Not supported, ftruncate should still work but might be slow/fragmented
                    perror("posix_fallocate warning (not supported?)");
                } else {
                    // Other error
                     perror("posix_fallocate error during preallocation");
                     // Maybe exit here depending on requirements
                }
            } else {
                 printf("Log file space allocated successfully.\n");
            }
        }

        // Write/overwrite header if needed
        if (initialize_header) {
            printf("Initializing log file header for %s.\n", filename);
            CheckpointHeader h = {CHECKPOINT_MAGIC, 2 /* Version */, 0 /* Initial oldest */, NUM_STATE_CHUNKS, ACCOUNTS_PER_STATE_CHUNK};
            // Use pwrite for atomicity (at least for the write itself)
            ssize_t written_bytes = pwrite(fd, &h, sizeof(h), 0);
            if (written_bytes != sizeof(h)) {
                perror("Failed to write initial header");
                close(fd);
                exit(EXIT_FAILURE);
            }
            // Sync header write to disk
            if (fsync(fd) != 0) {
                perror("Failed to fsync initial header");
                // Non-fatal? Depends on requirements.
            }
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
        printf("      Log (%s), hash (%s), and reference (%s) files will be overwritten.\n",
               LOG_FILE, STATE_HASH_FILE, REFERENCE_STATE_FILE);
    } else {
        printf("INFO: ***** DEBUG/RECOVERY RUN *****\n");
        printf("      Attempting recovery from %s. Will compare against %s if available.\n",
               LOG_FILE, REFERENCE_STATE_FILE);
    }

    printf("System Config: ACCOUNTS=%lu, ACCOUNT_SIZE=%zu, BATCH_SIZE=%u\n",
            SMALL_ACCOUNT_COUNT, ACCOUNT_SIZE, BATCH_SIZE);
    printf("               CHUNKS=%u, ACC_PER_CHUNK=%u, PADDED_ACCOUNTS=%lu\n",
           NUM_STATE_CHUNKS, ACCOUNTS_PER_STATE_CHUNK, PADDED_ACCOUNT_COUNT);
    printf("               TOTAL_LOG_FILE_SIZE = %zu bytes\n",
           CHECKPOINT_HEADER_SIZE + TOTAL_SNAPSHOT_SLOTS * sizeof(SnapshotSlot) + TOTAL_TX_SLOTS * sizeof(TxSlot));


    if (NUM_STATE_CHUNKS == 0) { // Should be caught by preprocessor check, but belts and braces
        fprintf(stderr,"ERROR: NUM_STATE_CHUNKS is zero. Check config.\n");
        exit(EXIT_FAILURE);
     }

    // Allocate main state memory, aligned for potential performance benefits
    int64_t *main_state_array = NULL;
    if (posix_memalign((void **)&main_state_array, 64, PADDED_ACCOUNT_COUNT * ACCOUNT_SIZE) != 0) {
        perror("Error allocating aligned main_state_array"); exit(EXIT_FAILURE);
    }
    printf("Allocated main state array: %zu bytes (for %lu padded accounts).\n",
           PADDED_ACCOUNT_COUNT * ACCOUNT_SIZE, PADDED_ACCOUNT_COUNT);

    // Allocate temporary buffers used in commit_batch_data_to_log
    SnapshotSlot *temp_snap_slot = malloc(sizeof(SnapshotSlot));
    TxSlot *temp_tx_slot = malloc(sizeof(TxSlot));
    if (!temp_snap_slot || !temp_tx_slot) {
        perror("Failed to allocate temporary commit slots");
        free(main_state_array); // Clean up previous allocation
        exit(EXIT_FAILURE);
    }

    // --- Prepare Log File ---
    const size_t TOTAL_LOG_FILE_SIZE = CHECKPOINT_HEADER_SIZE +
                                   TOTAL_SNAPSHOT_SLOTS * sizeof(SnapshotSlot) +
                                   TOTAL_TX_SLOTS * sizeof(TxSlot);
    if (is_reference_run) {
        printf("INFO: Reference run, deleting old log, hash, and reference files...\n");
        remove(LOG_FILE); // Delete log to ensure clean state for reference run
        remove(STATE_HASH_FILE);
        remove(REFERENCE_STATE_FILE);
    }
    // Ensure log file exists, has correct size, and has a valid header
    preallocate_and_init_log_file(LOG_FILE, TOTAL_LOG_FILE_SIZE);


    // --- Attempt Recovery ---
    double rec_start_t = get_time_ms();
    int log_fd_rec = open(LOG_FILE, O_RDONLY); // Open read-only for recovery phase
    int recovered_batch = -1; // Batch number the state reflects after recovery

    printf("Zeroing main_state_array before recovery attempt...\n");
    memset(main_state_array, 0, PADDED_ACCOUNT_COUNT * ACCOUNT_SIZE);

    bool recovery_attempted = false;
    bool recovery_succeeded = false; // Did recover_state_from_log return 0?
    bool meaningful_state_recovered = false; // Did recovery result in batch >= 0?

    if (log_fd_rec >= 0) {
        printf("Attempting state recovery from %s...\n", LOG_FILE);
        recovery_attempted = true;
        if (recover_state_from_log(log_fd_rec, main_state_array, &recovered_batch) == 0) {
            // Recovery function finished without critical internal errors
            recovery_succeeded = true;
            if (recovered_batch >= 0) {
                meaningful_state_recovered = true;
                printf("Recovery successful. State loaded reflects state AFTER batch %d.\n", recovered_batch);
            } else {
                printf("Recovery function finished, but no prior state (batch >= 0) could be established. State remains zeroed or partially loaded but inconsistent.\n");
                // Reset state to known zero state if recovery didn't yield a batch number >= 0 ?
                // Or trust the potentially partially loaded state? Let's re-zero for safety.
                printf("Re-zeroing main_state_array as recovery yielded state for batch %d.\n", recovered_batch);
                memset(main_state_array, 0, PADDED_ACCOUNT_COUNT * ACCOUNT_SIZE);
            }
        } else {
            fprintf(stderr, "Recovery function reported a critical error. State is likely invalid.\n");
            // Ensure state is zeroed after a failed recovery attempt
             memset(main_state_array, 0, PADDED_ACCOUNT_COUNT * ACCOUNT_SIZE);
             recovered_batch = -1; // Ensure batch number reflects failure
        }
        close(log_fd_rec); // Close read-only FD
    } else {
        // Could not open log file for reading
        if (!is_reference_run) {
             perror("CRITICAL: Failed to open log file for recovery reading");
             // Decide if we can continue - maybe initialize state? For now, exit.
             free(main_state_array); free(temp_snap_slot); free(temp_tx_slot);
             exit(EXIT_FAILURE);
        } else {
            printf("INFO: Log file %s not found (expected for a fresh reference run). Starting with zeroed state.\n", LOG_FILE);
            recovered_batch = -1; // No state recovered
        }
    }
    printf("State recovery phase took: %.3f ms. State reflects batch: %d.\n", get_time_ms() - rec_start_t, recovered_batch);

    // --- Post-Recovery / Pre-Processing Initialization ---
    if (!meaningful_state_recovered) {
        // If recovery didn't yield a state (batch -1), initialize state here
        // Only do this if we didn't successfully recover something.
        printf("Initializing main state array with default balances (1,000,000) as no prior state was recovered.\n");
        for (uint64_t i = 0; i < PADDED_ACCOUNT_COUNT; i++) {
            main_state_array[i] = 1000000; // Default initial balance
        }
        // Hash verification is not possible if we just initialized
    } else {
        // --- Verify Recovered State (if possible) ---
        printf("Verifying hash of recovered state (batch %d) against %s...\n", recovered_batch, STATE_HASH_FILE);
        if (verify_recovered_state_hash(main_state_array, SMALL_ACCOUNT_COUNT, recovered_batch) != 0) {
            // Hash mismatch or other verification error
            if (!is_reference_run) {
                fprintf(stderr, "FATAL: Post-recovery state hash verification FAILED for batch %d. Exiting.\n", recovered_batch);
                // Optional: Dump state diff here if reference state is available?
                free(main_state_array); free(temp_snap_slot); free(temp_tx_slot);
                exit(EXIT_FAILURE); // Exit on mismatch for debug runs
            } else {
                 printf("INFO: Reference run - Hash verification failed or skipped (e.g., no hash file yet). Continuing reference run.\n");
            }
        } else {
            printf("Post-recovery state hash verification SUCCEEDED for batch %d.\n", recovered_batch);
        }

        // --- (Optional) Compare with Reference State File ---
         if (!is_reference_run) {
             printf("DEBUG RUN: Loading reference state from %s for comparison with recovered state (batch %d)...\n", REFERENCE_STATE_FILE, recovered_batch);
             int64_t *reference_state_array = malloc(PADDED_ACCOUNT_COUNT * ACCOUNT_SIZE);
             if (reference_state_array) {
                 FILE *f_ref = fopen(REFERENCE_STATE_FILE, "rb");
                 if (f_ref) {
                     size_t items_read = fread(reference_state_array, ACCOUNT_SIZE, PADDED_ACCOUNT_COUNT, f_ref);
                     fclose(f_ref);
                     if (items_read == PADDED_ACCOUNT_COUNT) {
                         printf("Comparing current main_state_array (post-recovery) with loaded reference state...\n");
                         dump_state_diff(reference_state_array, main_state_array, SMALL_ACCOUNT_COUNT, 50); // Compare relevant accounts
                     } else {
                         fprintf(stderr, "DEBUG RUN: Error reading full reference state from %s (read %zu/%lu items). Skipping diff.\n", REFERENCE_STATE_FILE, items_read, PADDED_ACCOUNT_COUNT);
                     }
                 } else {
                      if (errno == ENOENT) {
                         printf("DEBUG RUN: Reference state file %s not found. Run with 'saveref' first to create it. Skipping diff.\n", REFERENCE_STATE_FILE);
                      } else {
                         perror("DEBUG RUN: Error opening reference state file for reading");
                         fprintf(stderr, "Skipping diff.\n");
                      }
                 }
                 free(reference_state_array);
             } else {
                 perror("DEBUG RUN: Failed to allocate memory for reference_state_array");
             }
         } // end if (!is_reference_run)
    } // end else (meaningful_state_recovered)


    // --- Setup for Transaction Processing & Checkpointing ---
    // Open log file again, this time R/W for mmap
    int log_fd_mmap = open(LOG_FILE, O_RDWR);
    if (log_fd_mmap < 0) {
        perror("CRITICAL: Failed to re-open log file for mmap R/W");
        free(main_state_array); free(temp_snap_slot); free(temp_tx_slot);
        exit(EXIT_FAILURE);
    }

    // Map the log file into memory
    void *mapped_log_region = mmap(NULL, TOTAL_LOG_FILE_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED, log_fd_mmap, 0);
    if (mapped_log_region == MAP_FAILED) {
        perror("mmap failed for log file");
        close(log_fd_mmap);
        free(main_state_array); free(temp_snap_slot); free(temp_tx_slot);
        exit(EXIT_FAILURE);
    }
    printf("Log file %s mapped into memory (R/W).\n", LOG_FILE);

    // Get a pointer to the header within the mapped region
    CheckpointHeader *log_header_ptr = (CheckpointHeader*)mapped_log_region;

    // Sanity check the header again after mmap
    if (log_header_ptr->magic != CHECKPOINT_MAGIC || log_header_ptr->num_state_chunks != NUM_STATE_CHUNKS || log_header_ptr->accounts_per_chunk != ACCOUNTS_PER_STATE_CHUNK) {
        fprintf(stderr, "CRITICAL: Log file header invalid/mismatched after mmap! (Magic: 0x%x, Chunks: %u, Acc/Chunk: %u)\n",
                log_header_ptr->magic, log_header_ptr->num_state_chunks, log_header_ptr->accounts_per_chunk);
        munmap(mapped_log_region, TOTAL_LOG_FILE_SIZE); close(log_fd_mmap);
        free(main_state_array); free(temp_snap_slot); free(temp_tx_slot);
        exit(EXIT_FAILURE);
    }

    // Start the asynchronous msync thread
    pthread_t msync_pthread;
    msync_thread_data msync_data = { mapped_log_region, TOTAL_LOG_FILE_SIZE, 1 /* running flag */ };
    if (pthread_create(&msync_pthread, NULL, msync_thread_func, &msync_data) != 0) {
        perror("Error creating msync thread");
        munmap(mapped_log_region, TOTAL_LOG_FILE_SIZE); close(log_fd_mmap);
        free(main_state_array); free(temp_snap_slot); free(temp_tx_slot);
        exit(EXIT_FAILURE);
    }
    printf("Background msync thread started.\n");


    // --- Process Transactions from File ---
    Transaction *tx_batch_buf = malloc(BATCH_SIZE * sizeof(Transaction));
    if (!tx_batch_buf) {
        perror("Failed to allocate tx_batch_buf");
        // Clean up mmap and thread
        msync_data.running = 0; pthread_join(msync_pthread, NULL);
        munmap(mapped_log_region, TOTAL_LOG_FILE_SIZE); close(log_fd_mmap);
        free(main_state_array); free(temp_snap_slot); free(temp_tx_slot);
        exit(EXIT_FAILURE);
    }

    FILE *fp_tx = fopen(TX_FILE, "rb");
    if (!fp_tx) {
        perror("Error opening transactions file");
         // Clean up mmap and thread
        msync_data.running = 0; pthread_join(msync_pthread, NULL);
        munmap(mapped_log_region, TOTAL_LOG_FILE_SIZE); close(log_fd_mmap);
        free(main_state_array); free(temp_snap_slot); free(temp_tx_slot); free(tx_batch_buf);
        exit(EXIT_FAILURE);
    }
    printf("Opened transaction file: %s\n", TX_FILE);


    // Determine starting batch number and file offset based on recovery result
    uint32_t starting_batch_num_from_tx_file = 0;
    if (meaningful_state_recovered) {
        // We recovered state up to recovered_batch. Start processing the *next* batch.
        starting_batch_num_from_tx_file = (uint32_t)recovered_batch + 1;

        // Calculate byte offset in the transaction file
        off_t tx_file_offset = (off_t)starting_batch_num_from_tx_file * BATCH_SIZE * sizeof(Transaction);
        printf("Attempting to seek %s to offset %ld (to start processing batch %u)\n",
               TX_FILE, (long)tx_file_offset, starting_batch_num_from_tx_file);

        if (fseeko(fp_tx, tx_file_offset, SEEK_SET) != 0) { // Use fseeko for large file offsets
            // Could happen if transactions.bin is shorter than expected based on recovered_batch
             perror("fseeko failed on transaction file");
             fprintf(stderr, "Maybe %s is shorter than expected state from log file (%d batches)?\n", TX_FILE, recovered_batch + 1);
             // Decide how to handle: error out, or process from beginning? Let's error out.
             fclose(fp_tx);
             msync_data.running = 0; pthread_join(msync_pthread, NULL);
             munmap(mapped_log_region, TOTAL_LOG_FILE_SIZE); close(log_fd_mmap);
             free(main_state_array); free(temp_snap_slot); free(temp_tx_slot); free(tx_batch_buf);
             exit(EXIT_FAILURE);
        }
         printf("Successfully seeked %s. Ready to process starting from batch %u.\n", TX_FILE, starting_batch_num_from_tx_file);
    } else {
        // No meaningful recovery, start processing transactions from batch 0
        printf("No prior state recovered, processing %s from the beginning (batch 0).\n", TX_FILE);
        starting_batch_num_from_tx_file = 0;
        // No seek needed, already at the beginning.
    }


    // Main transaction processing loop
    uint32_t current_batch_num_in_loop = starting_batch_num_from_tx_file;
    uint32_t file_batches_processed_this_run_count = 0;
    int last_batch_processed_this_run = recovered_batch; // Initialize with recovered batch
    double proc_start_t_loop = get_time_ms();

    printf("Starting transaction processing loop from batch %u...\n", current_batch_num_in_loop);

    while(1) {
        // Read the next batch of transactions
        size_t num_tx_read = fread(tx_batch_buf, sizeof(Transaction), BATCH_SIZE, fp_tx);

        if (num_tx_read == 0) {
            if(feof(fp_tx)) {
                printf("End of transaction file %s reached.\n", TX_FILE);
            } else {
                // Read error occurred
                char err_buf[256]; sprintf(err_buf, "Error reading %s at batch %u", TX_FILE, current_batch_num_in_loop); perror(err_buf);
                // Decide how to handle: stop processing? For now, break loop.
            }
            break; // Exit loop on EOF or read error
        }

        // Apply transactions in the current batch to the main state array
        for (size_t k = 0; k < num_tx_read; ++k) {
            apply_transaction_to_state_array(&tx_batch_buf[k], main_state_array);
        }

        // --- Checkpoint the state chunk and transaction batch to the log ---
        // Determine the cycle and slot index for checkpointing this batch
        uint32_t chkpt_cycle = (current_batch_num_in_loop / NUM_STATE_CHUNKS) % CYCLES;
        uint32_t slot_in_cycle = current_batch_num_in_loop % NUM_STATE_CHUNKS;

        // Commit snapshot chunk and transaction batch to the mmap'd log
        commit_batch_data_to_log(chkpt_cycle, slot_in_cycle, current_batch_num_in_loop,
                                 main_state_array, tx_batch_buf, num_tx_read,
                                 mapped_log_region, temp_snap_slot, temp_tx_slot);

        // --- Update Oldest Cycle in Header ---
        // If this batch completes a cycle (i.e., it's the last slot index of a cycle)
        if (slot_in_cycle == (NUM_STATE_CHUNKS - 1)) {
            // The cycle just completed is `chkpt_cycle`.
            // The *new* oldest cycle should be the one we just finished writing.
            // (Because the *other* cycle is now the one we will start overwriting)
            uint32_t just_completed_cycle = chkpt_cycle;

            // Update the header in the mapped region
            // Ensure atomicity if needed, though a single uint32 write is often atomic
            // Use volatile pointer access if strict guarantees needed, or locking if multi-threaded writers existed.
            log_header_ptr->oldest_cycle = just_completed_cycle;

            // Explicitly flush the header update to disk (important!)
            // MS_SYNC ensures it waits for disk write.
            if (msync((void*)log_header_ptr, CHECKPOINT_HEADER_SIZE, MS_SYNC) != 0) {
                 perror("CRITICAL: Failed to msync header update for oldest_cycle");
                 // Consider this a fatal error? Or log and continue?
            }
            printf("Batch %u: Cycle %u fully checkpointed. Header oldest_cycle updated to %u and synced.\n",
                   current_batch_num_in_loop, just_completed_cycle, log_header_ptr->oldest_cycle);
        }

        // --- Logging and Loop Update ---
        // Log progress periodically
        if ((current_batch_num_in_loop % 100) == 0 || num_tx_read < BATCH_SIZE) {
            printf("Processed batch %u (%zu tx read).\n", current_batch_num_in_loop, num_tx_read);
        }

        last_batch_processed_this_run = current_batch_num_in_loop; // Update last processed batch number
        file_batches_processed_this_run_count++;
        current_batch_num_in_loop++; // Move to the next batch number

        // If we read less than a full batch, it must be the end of the file
        if (num_tx_read < BATCH_SIZE) {
            printf("Last batch read (%u) was incomplete (%zu tx). Assuming end of %s.\n",
                   last_batch_processed_this_run, num_tx_read, TX_FILE);
            break; // Exit loop
        }
    } // End transaction processing while loop

    double proc_end_t_loop = get_time_ms();
    printf("--- Finished processing %s (%u batches processed in this run) ---\n", TX_FILE, file_batches_processed_this_run_count);
    printf("Processing loop took: %.3f ms\n", proc_end_t_loop - proc_start_t_loop);
    printf("Final state in memory reflects batch: %d\n", last_batch_processed_this_run);


    // --- Final Hash Calculation and Saving ---
    int batch_for_final_hash_label = last_batch_processed_this_run; // State reflects the last batch processed *or* recovered

    if (batch_for_final_hash_label >= 0) {
        printf("Computing and saving final state hash for batch %d...\n", batch_for_final_hash_label);
        // Optional: Print sample state before hashing for debugging
        printf("Sample state BEFORE final hash (batch %d):\n", batch_for_final_hash_label);
        for (int i = 0; i < 5 && (unsigned long)i < SMALL_ACCOUNT_COUNT; ++i) {
             printf("  main_state_array[%d] = %" PRId64 "\n", i, main_state_array[i]);
        }
        if (SMALL_ACCOUNT_COUNT > 5 && PADDED_ACCOUNT_COUNT > 5) {
            printf("  ...\n");
             printf("  main_state_array[%lu] = %" PRId64 "\n", SMALL_ACCOUNT_COUNT -1, main_state_array[SMALL_ACCOUNT_COUNT -1]);
        }


        uint64_t final_hash = compute_state_hash(main_state_array, SMALL_ACCOUNT_COUNT); // Hash only the used accounts
        printf("Computed final hash: 0x%016" PRIx64 " for state after batch %d\n", final_hash, batch_for_final_hash_label);

        // Save the computed hash and its corresponding batch number
        save_state_hash_to_file(final_hash, batch_for_final_hash_label);

        // --- Save Reference State (if reference run) ---
        if (is_reference_run) {
            printf("REFERENCE RUN: Saving current main state array to %s (state after batch %d)\n",
                   REFERENCE_STATE_FILE, last_batch_processed_this_run);
            FILE* f_ref_save = fopen(REFERENCE_STATE_FILE, "wb");
            if (f_ref_save) {
                // Write the entire padded array for simplicity, even if only SMALL_ACCOUNT_COUNT is used
                size_t items_written = fwrite(main_state_array, ACCOUNT_SIZE, PADDED_ACCOUNT_COUNT, f_ref_save);
                if (fflush(f_ref_save) != 0) { perror("fflush failed for reference state file");}
                int fd_ref = fileno(f_ref_save);
                if (fd_ref >= 0 && fsync(fd_ref) != 0) { perror("fsync failed for reference state file");}
                fclose(f_ref_save); // Close after fsync

                if (items_written == PADDED_ACCOUNT_COUNT) {
                    printf("REFERENCE RUN: Successfully saved state to %s\n", REFERENCE_STATE_FILE);
                } else {
                    fprintf(stderr, "REFERENCE RUN: Error writing full state to %s (wrote %zu/%lu items)\n", REFERENCE_STATE_FILE, items_written, PADDED_ACCOUNT_COUNT);
                }
            } else {
                perror("REFERENCE RUN: Error opening reference state file for writing");
            }
        }
    } else {
        printf("No meaningful state (batch >= 0) to hash or save for this run.\n");
    }

    // --- Cleanup ---
    printf("Finalizing operations...\n");

    // Stop the msync thread and wait for it to join
    msync_data.running = 0;
    pthread_join(msync_pthread, NULL);
    printf("Background msync thread joined.\n");

    // Final synchronous flush of the entire log file before unmapping
    printf("Performing final sync flush of log file...\n");
    double final_sync_start = get_time_ms();
    if (msync(mapped_log_region, TOTAL_LOG_FILE_SIZE, MS_SYNC) != 0) {
        perror("Final msync of log data failed during cleanup");
    }
    if (fsync(log_fd_mmap) != 0) { // Also fsync the file descriptor
        perror("Final fsync of log_fd_mmap failed during cleanup");
    }
     printf("Final sync flush completed (%.3f ms).\n", get_time_ms() - final_sync_start);


    // Unmap the log file
    if (munmap(mapped_log_region, TOTAL_LOG_FILE_SIZE) != 0) {
        perror("munmap failed during cleanup");
    } else {
        printf("Log file unmapped.\n");
    }

    // Close files
    if(fp_tx) fclose(fp_tx);
    if(log_fd_mmap >=0) close(log_fd_mmap);
    printf("File descriptors closed.\n");

    // Free allocated memory
    free(tx_batch_buf);
    free(temp_snap_slot);
    free(temp_tx_slot);
    free(main_state_array);
    printf("Memory freed.\n");

    printf("Execution complete.\n");

    return 0;
}