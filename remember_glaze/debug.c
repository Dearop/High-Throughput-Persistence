#define _POSIX_C_SOURCE 200809L
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <time.h>
#include <math.h>
#include <stdbool.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <errno.h>
#include <pthread.h>
#include <inttypes.h>

// --- Definitions and Constants ---

#define BATCH_SIZE              (1ULL << 16)      // 65,536 transactions per batch
#define SMALL_ACCOUNT_COUNT     2000000UL       // Target number of accounts
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
    uint32_t current_cycle_ptr; // Modified: Pointer to the current cycle (always 0 in single cycle)
                                // Alternatively, this field could be repurposed or removed if strictly single-cycle.
                                // For now, let's assume it indicates the cycle being written to, which is always 0.
    uint32_t num_state_chunks;  // For verification: NUM_STATE_CHUNKS
    uint32_t accounts_per_chunk;// For verification: ACCOUNTS_PER_STATE_CHUNK
} CheckpointHeader;

#define CHECKPOINT_HEADER_SIZE (sizeof(CheckpointHeader))

// --- Ring Log Layout ---
#define CYCLES 1 // MODIFIED: We maintain only one complete copy (cycle)
#define TOTAL_SNAPSHOT_SLOTS (NUM_STATE_CHUNKS * CYCLES) // Will simplify to NUM_STATE_CHUNKS
#define TOTAL_TX_SLOTS       (NUM_STATE_CHUNKS * CYCLES) // Will simplify to NUM_STATE_CHUNKS

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

typedef struct ChunkSlot {
    uint32_t batch_num;
    uint32_t chunk_idx_in_ring; // Index within the cycle (0 to NUM_STATE_CHUNKS-1)
    int64_t state_data[ACCOUNTS_PER_STATE_CHUNK];
} ChunkSlot;

typedef struct BatchSlot {
    uint32_t batch_num;
    uint32_t tx_count;
    Transaction transactions[BATCH_SIZE];
} BatchSlot;

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
        double percentage_diff = 0.0;
        if (count > 0) {
            percentage_diff = ((double)mismatches / count) * 100.0;
        }
        fprintf(diff_fp, // To file
                "dump_state_diff: %zu account(s) differ (%.2f%%). Reporting capped at %zu.\n", mismatches, percentage_diff, max_report);
        fprintf(stderr, // To console
                "dump_state_diff: %zu account(s) differ (%.2f%%). Detailed report in %s\n", mismatches, percentage_diff, (diff_fp == stderr ? "stderr" : DIFF_OUTPUT_FILE));

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

static inline uint32_t positive_modulo(int32_t value, uint32_t modulus) {
    int32_t result = value % (int32_t)modulus;
    return (uint32_t)(result >= 0 ? result : result + modulus);
}

int compare_tx_with_batch_num(const void *a, const void *b) {
    TransactionWithBatchNum *tx_a = (TransactionWithBatchNum *)a;
    TransactionWithBatchNum *tx_b = (TransactionWithBatchNum *)b;
    // Reversed comparison to sort in descending order (newest first)
    if (tx_a->original_batch_num > tx_b->original_batch_num) return -1;
    if (tx_a->original_batch_num < tx_b->original_batch_num) return 1;
    return 0;
}

// --- Transaction Application to In-Memory State ---
static inline bool apply_transaction_to_state_array(const Transaction *tx, int64_t *restrict state_array) {
    uint8_t sfunc = GET_FUNC(tx->sender);
    uint8_t rfunc = GET_FUNC(tx->receiver);
    uint64_t sidx = GET_DATA(tx->sender);
    uint64_t ridx = GET_DATA(tx->receiver);

    if (sidx >= PADDED_ACCOUNT_COUNT || (rfunc == 0 && ridx >= PADDED_ACCOUNT_COUNT)) { // Check bounds carefully
        // For range set, ridx is length, not an account index directly for bounds check here
        if (sfunc == 0 && rfunc == 0) return false; // Simple transfer out of bounds
        if (sfunc == 1 && rfunc == 1) return false; // Range set starts out of bounds
    }


    if (sfunc == 0 && rfunc == 0) { // Simple Transfer
         if (sidx >= SMALL_ACCOUNT_COUNT || ridx >= SMALL_ACCOUNT_COUNT) { // Apply business logic bounds
            return false;
        }
        if (state_array[sidx] >= (int64_t)tx->amount) {
            state_array[sidx] -= tx->amount;
            state_array[ridx] += tx->amount;
            return true;
        }
    } else if (sfunc == 1 && rfunc == 1) { // Range Set
        uint64_t start_idx = sidx;
        uint64_t len       = GET_DATA(tx->receiver); // ridx is len
        if (len == 0) return false;
        // Apply business logic bounds for range set
        if (start_idx >= SMALL_ACCOUNT_COUNT) return false; // Entire range is outside interesting area

        for (uint64_t i = 0; i < len; ++i) {
            if (start_idx + i >= SMALL_ACCOUNT_COUNT) return true; // Stop if exceeding logical account limit
            state_array[start_idx + i] = (int64_t)tx->amount;
        }
        return true;
    }
    return false;
}

// --- Checkpoint Commit Function ---
static void commit_batch_data_to_log(
    uint32_t slot_idx_in_cycle, // This is current_batch_num % NUM_STATE_CHUNKS
    uint32_t current_batch_num,
    const int64_t *restrict full_state_array,
    const Transaction *restrict current_transaction_batch,
    uint32_t num_tx_in_current_batch,
    void *restrict mapped_log_region,
    ChunkSlot *restrict temp_snap_slot,
    BatchSlot *restrict temp_tx_slot,
    long pagesize // Added pagesize parameter
) {
    size_t snapshot_slot_header_offset = CHECKPOINT_HEADER_SIZE;
    size_t tx_slot_array_offset = snapshot_slot_header_offset + NUM_STATE_CHUNKS * sizeof(ChunkSlot);

    size_t final_snap_offset = snapshot_slot_header_offset + slot_idx_in_cycle * sizeof(ChunkSlot);
    size_t final_tx_offset   = tx_slot_array_offset + slot_idx_in_cycle * sizeof(BatchSlot);

    // Prepare and write snapshot data
    temp_snap_slot->batch_num = current_batch_num;
    temp_snap_slot->chunk_idx_in_ring = slot_idx_in_cycle;
    const int64_t *source_state_chunk_ptr = full_state_array + (size_t)slot_idx_in_cycle * ACCOUNTS_PER_STATE_CHUNK ;
    nt_memcpy(temp_snap_slot->state_data, source_state_chunk_ptr, ACCOUNTS_PER_STATE_CHUNK * ACCOUNT_SIZE);
    void *snap_write_target_addr = (char*)mapped_log_region + final_snap_offset;
    nt_memcpy(snap_write_target_addr, temp_snap_slot, sizeof(ChunkSlot));

    // Prepare and write transaction data
    memset(temp_tx_slot->transactions, 0, BATCH_SIZE * sizeof(Transaction));
    temp_tx_slot->batch_num = current_batch_num;
    temp_tx_slot->tx_count = num_tx_in_current_batch;
    if (num_tx_in_current_batch > 0) {
        nt_memcpy(temp_tx_slot->transactions, current_transaction_batch, num_tx_in_current_batch * sizeof(Transaction));
    }
    void *tx_write_target_addr = (char*)mapped_log_region + final_tx_offset;
    nt_memcpy(tx_write_target_addr, temp_tx_slot, sizeof(BatchSlot));

    // Perform a single msync for both snapshot and transaction data
    if (pagesize == -1) { // Should have been checked earlier, but as a safeguard
        perror("commit_batch_data_to_log: invalid pagesize");
        return;
    }

    // The snapshot data for a slot is always before the transaction data for any slot in the log file structure.
    // Thus, snap_write_target_addr is the start of our modified region for this commit operation (or part of it).
    void *sync_start_page_addr = (void*)((uintptr_t)snap_write_target_addr & ~(pagesize - 1));
    
    // The last byte written is at the end of the transaction slot data.
    uintptr_t last_byte_written_addr = (uintptr_t)tx_write_target_addr + sizeof(BatchSlot) - 1;
    
    // Calculate length for msync to cover from sync_start_page_addr up to the end of the page containing last_byte_written_addr.
    size_t sync_len = ((last_byte_written_addr / pagesize) * pagesize + pagesize) - (uintptr_t)sync_start_page_addr;

    if (msync(sync_start_page_addr, sync_len, MS_SYNC) != 0) {
        char err_buf[256];
        snprintf(err_buf, sizeof(err_buf), "CRITICAL: Combined msync failed (snap_addr: %p, tx_addr: %p, sync_start: %p, len: %zu)",
                 snap_write_target_addr, tx_write_target_addr, sync_start_page_addr, sync_len);
        perror(err_buf);
        // Decide on error handling, e.g., exit(EXIT_FAILURE);
    }
}

// --- Recovery Function (User's Specified Algorithm - Refined) ---
int recover_state_from_log(int log_fd, int64_t *restrict state_array_to_recover, int *last_recovered_batch_num) {
    //printf("[DEBUG] Entered recover_state_from_log\n");
    CheckpointHeader header;
    ssize_t bytes = pread(log_fd, &header, sizeof(header), 0);
    //printf("[DEBUG] Read %zd bytes for header. Magic: 0x%08x\n", bytes, header.magic);
    if (bytes != (ssize_t)sizeof(header) || header.magic != CHECKPOINT_MAGIC) {
        fprintf(stderr, "Recovery: Invalid or missing checkpoint header. Skipping recovery.\n");
        return -1;
    }
    // MODIFIED: header.oldest_cycle is now header.current_cycle_ptr, should be 0
    //printf("[DEBUG] Header: version=%u, current_cycle_ptr=%u, num_state_chunks=%u, accounts_per_chunk=%u\n",
           //header.version, header.current_cycle_ptr, header.num_state_chunks, header.accounts_per_chunk);

    if (header.num_state_chunks != NUM_STATE_CHUNKS || header.accounts_per_chunk != ACCOUNTS_PER_STATE_CHUNK) {
        fprintf(stderr, "Recovery: Log file parameters mismatch. Cannot recover.\n");
        return -1;
    }
    if (header.current_cycle_ptr != 0 && CYCLES == 1) { // Sanity check for single cycle
         fprintf(stderr, "Recovery: Header current_cycle_ptr is not 0 in a single-cycle configuration. Log may be from a multi-cycle system.\n");
        // Depending on strictness, might return -1 or proceed assuming 0.
        // For now, let's be strict for a clean single-cycle implementation.
        // return -1; // Or attempt to use 0 anyway if deemed safe.
    }

    printf("Recovery: Valid checkpoint header. Using cycle 0.\n");

    ChunkSlot *temp_snap_slot = malloc(sizeof(ChunkSlot));
    BatchSlot *temp_tx_slot = malloc(sizeof(BatchSlot));
    int *batch_number_of_chunk = calloc(NUM_STATE_CHUNKS, sizeof(int));
    //printf("[DEBUG] Allocated temp_snap_slot=%p, temp_tx_slot=%p, batch_number_of_chunk=%p\n", (void*)temp_snap_slot, (void*)temp_tx_slot, (void*)batch_number_of_chunk);

    if (!temp_snap_slot || !temp_tx_slot || !batch_number_of_chunk) {
        perror("Recovery: malloc for slot read/tracking buffers failed");
        free(temp_snap_slot); free(temp_tx_slot); free(batch_number_of_chunk);
        return -1;
    }
    for(uint32_t i=0; i < NUM_STATE_CHUNKS; ++i)
      batch_number_of_chunk[i] = -1;

    // --- Step 1: Determine latest_chunk_saved_in_cycle ---
    int latest_chunk_idx_overall = -1; // Index of the chunk with the highest batch number
    int highest_batch_in_cycle = -1;
    size_t snapshot_slot_header_offset = CHECKPOINT_HEADER_SIZE;

    printf("Recovery Sub-Step 1.1: Scanning cycle for latest saved chunk snapshot info...\n");
    for (uint32_t chunk_k = 0; chunk_k < NUM_STATE_CHUNKS; ++chunk_k) {
        // Offset calculation for single cycle
        off_t snap_offset_in_file = snapshot_slot_header_offset + (off_t)chunk_k * sizeof(ChunkSlot);
        //printf("[DEBUG] Reading snapshot chunk_k=%u from cycle %u at offset=%ld\n", chunk_k, recovery_cycle, (long)snap_offset_in_file);
        memset(temp_snap_slot, 0, sizeof(ChunkSlot));
        ssize_t read_bytes = pread(log_fd, temp_snap_slot, sizeof(ChunkSlot), snap_offset_in_file);
        //printf("[DEBUG] Read %zd bytes for chunk_k=%u (cycle %u), batch_num=%u, chunk_idx_in_ring=%u\n",
               //read_bytes, chunk_k, recovery_cycle, temp_snap_slot->batch_num, temp_snap_slot->chunk_idx_in_ring);

        if (read_bytes == (ssize_t)sizeof(ChunkSlot) && temp_snap_slot->chunk_idx_in_ring == chunk_k) { // Validate chunk_idx_in_ring
            if ((int)temp_snap_slot->batch_num > highest_batch_in_cycle) {
                highest_batch_in_cycle = (int)temp_snap_slot->batch_num;
                latest_chunk_idx_overall = temp_snap_slot->chunk_idx_in_ring; // This should be chunk_k
                //printf("[DEBUG] New highest_batch_in_cycle=%d, latest_chunk_idx_overall=%d (for chunk_k %u)\n",
                       //highest_batch_in_cycle, latest_chunk_idx_overall, chunk_k);
            }
        }
    }
    if (latest_chunk_idx_overall != -1) {
         printf("Recovery Sub-Step 1.1: Latest state information found in cycle corresponds to chunk %d (batch %d).\n",
               latest_chunk_idx_overall, highest_batch_in_cycle);
    } else {
        printf("Recovery Sub-Step 1.1: No valid snapshot chunks found in cycle. Cannot determine baseline state.\n");
        // This is a critical failure if no snapshots are found.
        free(temp_snap_slot); 
        free(temp_tx_slot); 
        free(batch_number_of_chunk);
        return -1;
    }

    // --- Step 2: Load State Chunks---
    printf("Recovery Sub-Step 1.2: Loading state chunks from cycle...\n");
    memset(state_array_to_recover, 0, PADDED_ACCOUNT_COUNT * ACCOUNT_SIZE);
    int max_loaded_batch_overall = -1; // Will be same as highest_batch_in_cycle if all chunks are read

    for (uint32_t chunk_k = 0; chunk_k < NUM_STATE_CHUNKS; ++chunk_k) {
        off_t snap_offset_in_file = snapshot_slot_header_offset + (off_t)chunk_k * sizeof(ChunkSlot);
        //printf("[DEBUG] Loading chunk_k=%u from cycle %u at offset=%ld\n", chunk_k, recovery_cycle, (long)snap_offset_in_file);
        memset(temp_snap_slot, 0, sizeof(ChunkSlot));
        ssize_t read_bytes = pread(log_fd, temp_snap_slot, sizeof(ChunkSlot), snap_offset_in_file);
        //printf("[DEBUG] Read %zd bytes for chunk_k=%u (cycle %u), batch_num=%u, chunk_idx_in_ring=%u\n",
               //read_bytes, chunk_k, recovery_cycle, temp_snap_slot->batch_num, temp_snap_slot->chunk_idx_in_ring);

        if (read_bytes == (ssize_t)sizeof(ChunkSlot) && temp_snap_slot->chunk_idx_in_ring == chunk_k) {
            nt_memcpy(state_array_to_recover + (size_t)chunk_k * ACCOUNTS_PER_STATE_CHUNK,
                      temp_snap_slot->state_data, ACCOUNTS_PER_STATE_CHUNK * ACCOUNT_SIZE);
            batch_number_of_chunk[chunk_k] = (int)temp_snap_slot->batch_num;
            if ((int)temp_snap_slot->batch_num > max_loaded_batch_overall) {
                max_loaded_batch_overall = (int)temp_snap_slot->batch_num;
            }
            //printf("[DEBUG] Loaded chunk_k=%u from cycle %u, batch_num=%d\n", chunk_k, recovery_cycle, temp_snap_slot->batch_num);
        } else {
            fprintf(stderr, "Recovery WARNING: Failed to read/validate snapshot for chunk %u from cycle. State zeroed, batch -1.\n", chunk_k);
            // batch_number_of_chunk[chunk_k] remains -1, state for this chunk is 0.
            // This is problematic, recovery might be impossible or state will be inconsistent.
            // For a single cycle system, all chunks should ideally be present and valid.
        }
    }
    // After loading, max_loaded_batch_overall should be equal to highest_batch_in_cycle if all chunks are from a consistent checkpoint pass.
    // If they differ significantly, it implies an inconsistent write or corruption.

    *last_recovered_batch_num = max_loaded_batch_overall;
    printf("Recovery Sub-Step 1.2: Composite snapshot loaded from cycle. Effective batch: %d.\n", *last_recovered_batch_num);

    if (*last_recovered_batch_num == -1) {
        fprintf(stderr, "Recovery: Failed to load any valid snapshot data. Aborting recovery.\n");
        free(temp_snap_slot); free(temp_tx_slot); free(batch_number_of_chunk);
        return -1;
    }
   
    // --- Step 3: Replay Transactions ---
    // The goal is to bring all chunks from their snapshot batch (batch_number_of_chunk[k])
    // forward to the overall latest batch found in snapshots (max_loaded_batch_overall).
    size_t tx_slot_array_offset = CHECKPOINT_HEADER_SIZE + NUM_STATE_CHUNKS * sizeof(ChunkSlot);
    uint32_t chnk_processed_counter = 0;
    uint32_t collected_tx_count = 0;
    for (uint32_t currently_processing_chnk = positive_modulo(latest_chunk_idx_overall - 1, NUM_STATE_CHUNKS); 
         chnk_processed_counter < NUM_STATE_CHUNKS - 1; 
         currently_processing_chnk = positive_modulo((int32_t)currently_processing_chnk - 1, NUM_STATE_CHUNKS)) {
        printf("[DEBUG] Processing chunk %u (chnk_processed_counter=%u)\n", currently_processing_chnk, chnk_processed_counter);
        ++ chnk_processed_counter;
        // Iterate through the batches that are newer than the batch of the currently processing chunk
        for (uint32_t curr_batch = batch_number_of_chunk[currently_processing_chnk] + 1; 
            curr_batch <= batch_number_of_chunk[latest_chunk_idx_overall]; 
            ++curr_batch) {

            printf("[DEBUG] Processing batch %u for chunk %u\n", curr_batch, currently_processing_chnk);
            uint32_t tx_log_slot_idx = (uint32_t)curr_batch % NUM_STATE_CHUNKS;
            // load current batch
            off_t tx_offset_in_file = tx_slot_array_offset + (off_t)tx_log_slot_idx * sizeof(BatchSlot);
            //printf("[DEBUG] Reading tx batch at offset=%ld\n", (long)tx_offset_in_file);
            memset(temp_tx_slot, 0, sizeof(BatchSlot));
            ssize_t read_bytes = pread(log_fd, temp_tx_slot, sizeof(BatchSlot), tx_offset_in_file);
            //printf("[DEBUG] Read %zd bytes for tx batch, batch_num=%u, tx_count=%u\n", read_bytes, temp_tx_slot->batch_num, temp_tx_slot->tx_count);
            if (read_bytes == (ssize_t)sizeof(BatchSlot)) {
                if (temp_tx_slot->tx_count > BATCH_SIZE) {
                    fprintf(stderr, "Recovery WARNING: BatchSlot for chunk %u (batch %u) has tx_count %u > BATCH_SIZE. Clamping.\n",
                            currently_processing_chnk, temp_tx_slot->batch_num, temp_tx_slot->tx_count);
                    temp_tx_slot->tx_count = BATCH_SIZE;
                }
            }
            // Iterate through the transactions in the current batch and filter
            for (uint32_t tx_idx = 0; tx_idx < temp_tx_slot->tx_count; ++tx_idx) {
                const Transaction *tx = &temp_tx_slot->transactions[tx_idx];
                uint8_t sfunc = GET_FUNC(tx->sender);
                uint8_t rfunc = GET_FUNC(tx->receiver);
                uint64_t sdata = GET_DATA(tx->sender);
                uint64_t rdata = GET_DATA(tx->receiver);

                // Print transaction details
                //printf("[REPLAY TX] Batch: %u, TX_idx: %u, Sender: 0x%016lx (Func: %u, Data: %lu), Receiver: 0x%016lx (Func: %u, Data: %lu), Amount: %lu\n",
                //       temp_tx_slot->batch_num, // or curr_batch
                //       tx_idx,
                //       tx->sender, sfunc, sdata,
                //       tx->receiver, rfunc, rdata,
                //       tx->amount);

                //printf("[DEBUG] Processing tx_idx=%u, sfunc=%u, rfunc=%u, sdata=%lu, rdata=%lu\n", tx_idx, sfunc, rfunc, sdata, rdata);
                // Transfer transaction
                
                if (sfunc == 0 && rfunc == 0) {
                    bool sender_in_this_chunk_scope = (sdata < PADDED_ACCOUNT_COUNT && (sdata / ACCOUNTS_PER_STATE_CHUNK) == currently_processing_chnk);
                    bool receiver_in_this_chunk_scope = (rdata < PADDED_ACCOUNT_COUNT && (rdata / ACCOUNTS_PER_STATE_CHUNK) == currently_processing_chnk);
                    bool can_transfer_based_on_current_sdata_in_array = (sdata < PADDED_ACCOUNT_COUNT);
                    bool counted = false;
                    if (sender_in_this_chunk_scope) {
                        if (can_transfer_based_on_current_sdata_in_array) {
                            state_array_to_recover[sdata] -= (int64_t)tx->amount;
                            ++collected_tx_count;
                            counted = true;
                        }
                    }
                    
                    if (receiver_in_this_chunk_scope) {
                        // CRUCIAL: Use the SAME can_transfer decision. Do NOT re-evaluate based on potentially changed state_array_to_recover[sdata]
                        if (can_transfer_based_on_current_sdata_in_array) { 
                            state_array_to_recover[rdata] += (int64_t)tx->amount; 
                             if (counted == false) {
                                ++collected_tx_count;
                                counted = true;
                            }
                        }
                    }
                }
                // Range set transaction
                if (sfunc == 1 && rfunc == 1) {
                    ++ collected_tx_count;
                    uint64_t start_idx = sdata;
                    uint64_t len = rdata;
                    if (start_idx >= 0 && start_idx < PADDED_ACCOUNT_COUNT) {
                        for (uint64_t i = 0; i < len; ++ i) {
                            uint32_t current_chunk_affected = (start_idx + i) / ACCOUNTS_PER_STATE_CHUNK;
                            if (start_idx + i >= SMALL_ACCOUNT_COUNT) break;
                            if (current_chunk_affected == currently_processing_chnk) {
                                state_array_to_recover[start_idx + i] = (int64_t)tx->amount;
                            }
                        }
                    }
                }
            }
        } 
    }
    printf("Recovery Step 2: Applied %zu transactions. State now reflects batch %d.\n", collected_tx_count, *last_recovered_batch_num);

    free(temp_snap_slot);
    free(temp_tx_slot);
    free(batch_number_of_chunk);

    printf("[DEBUG] Exiting recover_state_from_log. Final state reflects batch number: %d\n", *last_recovered_batch_num);
    printf("Recovery finished. Final state reflects batch number: %d\n", *last_recovered_batch_num);
    return 0;
}


// --- State Hashing ---
uint64_t compute_state_hash(const int64_t *state_array, size_t num_accounts_to_hash) {
    uint64_t new_hash = 0;
    if (num_accounts_to_hash == 0) {
        return 0;
    }
    for (int i = 0; i < 64; ++i) {
        uint8_t segment_parity = 0;
        size_t start_index = (i * num_accounts_to_hash) / 64;
        size_t end_index = ((i + 1) * num_accounts_to_hash) / 64;
        for (size_t k = start_index; k < end_index; ++k) {
            segment_parity ^= (state_array[k] & 1);
        }
        if (segment_parity != 0) {
            new_hash |= (1ULL << i);
        }
    }
    return new_hash;
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
        return 0; // Not an error, just can't verify.
    }
    if (computed_hash_of_current_state != hash_data_from_file.hash) {
        fprintf(stderr, "Hash Verify ERROR: State hash MISMATCH for batch %u!\n", batch_num_of_current_state);
        return -1; // This is an error.
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
            temp_header.accounts_per_chunk != ACCOUNTS_PER_STATE_CHUNK ||
            (CYCLES == 1 && temp_header.current_cycle_ptr != 0) ) { // Check current_cycle_ptr for single cycle
            initialize_header = true;
        } else {
             printf("Log file %s exists with valid header (Current Cycle Ptr: %u).\n", filename, temp_header.current_cycle_ptr);
        }
    }

    if (initialize_header || st.st_size < (off_t)total_log_size) {
        if (st.st_size < (off_t)total_log_size) {
             printf("Log file %s size %ld is less than required %zu. Resizing.\n", filename, (long)st.st_size, total_log_size);
            if (ftruncate(fd, total_log_size) != 0) {
                perror("ftruncate error");
                // Continue to try fallocate if ftruncate fails for some reason but size is still small
            }
            errno = 0; // Reset errno before posix_fallocate
            // Try fallocate, but don't make it fatal if not supported or disk full and ftruncate worked.
            if (posix_fallocate(fd, 0, total_log_size) != 0 && errno != EOPNOTSUPP && errno != EINVAL && errno != ENOSPC) {
                 perror("posix_fallocate warning");
            }
        }
        if (initialize_header) {
            printf("Initializing log file header for %s (single cycle).\n", filename);
            // current_cycle_ptr is always 0 for single cycle
            CheckpointHeader h = {CHECKPOINT_MAGIC, 2, 0, NUM_STATE_CHUNKS, ACCOUNTS_PER_STATE_CHUNK};
            if (pwrite(fd, &h, sizeof(h), 0) != sizeof(h)) {
                perror("Failed to write initial header"); close(fd); exit(EXIT_FAILURE);
            }
            if (fsync(fd) != 0) { perror("Failed to fsync initial header"); }
            printf("Initialized log file header (Current cycle ptr set to 0).\n");
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

    printf("System Config: ACCOUNTS=%lu, ACCOUNT_SIZE=%zu, BATCH_SIZE=%llu\n",
            SMALL_ACCOUNT_COUNT, ACCOUNT_SIZE, BATCH_SIZE);
    printf("               CHUNKS=%u, ACC_PER_CHUNK=%u, PADDED_ACCOUNTS=%lu\n",
           NUM_STATE_CHUNKS, ACCOUNTS_PER_STATE_CHUNK, PADDED_ACCOUNT_COUNT);
    printf("               LOGGING CYCLES: %d\n", CYCLES);

    // Get pagesize once
    long pagesize = sysconf(_SC_PAGESIZE);
    if (pagesize == -1) {
        perror("sysconf _SC_PAGESIZE failed in main");
        exit(EXIT_FAILURE); // Critical for mmap operations
    }
    printf("System page size: %ld bytes\n", pagesize);

    int64_t *main_state_array = NULL;
    if (posix_memalign((void **)&main_state_array, 64, PADDED_ACCOUNT_COUNT * ACCOUNT_SIZE) != 0) {
        perror("Error allocating main_state_array"); exit(EXIT_FAILURE);
    }

    ChunkSlot *temp_snap_slot = malloc(sizeof(ChunkSlot));
    BatchSlot *temp_tx_slot = malloc(sizeof(BatchSlot));
    if (!temp_snap_slot || !temp_tx_slot) {
        perror("Failed to allocate temporary commit slots");
        free(main_state_array);
        exit(EXIT_FAILURE);
    }
    // TOTAL_SNAPSHOT_SLOTS and TOTAL_TX_SLOTS are simplified by CYCLES=1
    const size_t SINGLE_CYCLE_SNAPSHOT_BYTES = NUM_STATE_CHUNKS * sizeof(ChunkSlot);
    const size_t SINGLE_CYCLE_TX_BYTES = NUM_STATE_CHUNKS * sizeof(BatchSlot);
    const size_t TOTAL_LOG_FILE_SIZE = CHECKPOINT_HEADER_SIZE +
                                   SINGLE_CYCLE_SNAPSHOT_BYTES +
                                   SINGLE_CYCLE_TX_BYTES;

    printf("Total log file size will be: %zu bytes (Header: %zu, Snapshots: %zu, TXlogs: %zu)\n",
           TOTAL_LOG_FILE_SIZE, CHECKPOINT_HEADER_SIZE, SINGLE_CYCLE_SNAPSHOT_BYTES, SINGLE_CYCLE_TX_BYTES);


    if (is_reference_run) {
        printf("Reference run: Removing old log/state files...\n");
        remove(LOG_FILE);
        remove(STATE_HASH_FILE);
        remove(REFERENCE_STATE_FILE);
    }

    preallocate_and_init_log_file(LOG_FILE, TOTAL_LOG_FILE_SIZE); // Initialize/Verify log file first

    double rec_start_t = get_time_ms();
    int log_fd_rec = -1;

    int recovered_batch = -1;
    bool meaningful_state_recovered = false;

    if (access(LOG_FILE, F_OK) != 0 && errno == ENOENT) { // File does not exist
        printf("Recovery: %s not found. Skipping recovery phase, will start fresh.\n", LOG_FILE);
        // preallocate_and_init_log_file already called, ensures header if it creates the file
        memset(main_state_array, 0, PADDED_ACCOUNT_COUNT * ACCOUNT_SIZE);
    } else { // File exists or other access error
        log_fd_rec = open(LOG_FILE, O_RDONLY);
        if (log_fd_rec < 0) {
            perror("Recovery: Error opening existing log file for reading. Skipping recovery.");
            // preallocate_and_init_log_file might have issues if file is problematic.
            // For safety, initialize state array if we can't recover.
            memset(main_state_array, 0, PADDED_ACCOUNT_COUNT * ACCOUNT_SIZE);
        } else {
            printf("Attempting to recover state from %s\n", LOG_FILE);
            double rec_phase_start = get_time_ms();
            if (recover_state_from_log(log_fd_rec, main_state_array, &recovered_batch) == 0) {
                meaningful_state_recovered = true;
                printf("Recovery successful. Last recovered batch: %d\n", recovered_batch);
            } else {
                printf("Recovery from log failed or produced no meaningful state. Initializing state to zero.\n");
                memset(main_state_array, 0, PADDED_ACCOUNT_COUNT * ACCOUNT_SIZE);
                recovered_batch = -1;
            }
            double rec_phase_end = get_time_ms();
            printf("[TIMING] Recovery phase took %.3f ms.\n", rec_phase_end - rec_phase_start);
            close(log_fd_rec);
        }
    }
    double rec_end_t = get_time_ms();
    printf("Recovery phase took %.3f ms. Last recovered batch: %d. Meaningful state recovered: %s \n",
           rec_end_t - rec_start_t, recovered_batch, meaningful_state_recovered ? "yes" : "no");

    // Print state after recovery or initial default initialization
    //printf("--- State After Recovery/Initialisation (Reflects Batch: %d) ---\n", recovered_batch);
    //for (uint64_t i = 0; i < SMALL_ACCOUNT_COUNT; ++i) {
    //    printf("Account[%02lu]: %-12ld ", i, main_state_array[i]);
    //    if ((i + 1) % 4 == 0 || i == SMALL_ACCOUNT_COUNT - 1) printf("\n");
    //}
    printf("--------------------------------------------------------------------\n");

    if (!meaningful_state_recovered) {
        printf("Initializing main state array with default balances as no prior state was recovered.\n");
        for (uint64_t i = 0; i < SMALL_ACCOUNT_COUNT; i++) main_state_array[i] = 1000000; // Init only logical accounts
        for (uint64_t i = SMALL_ACCOUNT_COUNT; i < PADDED_ACCOUNT_COUNT; i++) main_state_array[i] = 0; // Zero out padding
    } else {
        if (verify_recovered_state_hash(main_state_array, SMALL_ACCOUNT_COUNT, recovered_batch) != 0) {
            if (!is_reference_run) {
                fprintf(stderr, "FATAL: Post-recovery state hash verification FAILED for batch %d.\n", recovered_batch);
                // dump_state_diff if reference exists
                int64_t *ref_state = malloc(PADDED_ACCOUNT_COUNT * ACCOUNT_SIZE);
                if(ref_state) {
                    FILE *f_ref = fopen(REFERENCE_STATE_FILE, "rb");
                    if(f_ref) {
                        fclose(f_ref);
                    } else if (errno != ENOENT) {
                        perror("Error opening reference state file for diff");
                    }
                    free(ref_state);
                }
                // Consider exiting: exit(EXIT_FAILURE);
            }
        } else {
            printf("Post-recovery state hash verification SUCCEEDED for batch %d.\n", recovered_batch);
        }
         if (!is_reference_run) {
             int64_t *reference_state_array_debug = malloc(PADDED_ACCOUNT_COUNT * ACCOUNT_SIZE);
             if (reference_state_array_debug) {
                 FILE *f_ref_debug = fopen(REFERENCE_STATE_FILE, "rb");
                 if (f_ref_debug) {
                     if (fread(reference_state_array_debug, ACCOUNT_SIZE, PADDED_ACCOUNT_COUNT, f_ref_debug) == PADDED_ACCOUNT_COUNT) {
                         printf("DEBUG RUN: Comparing current main_state_array (post-recovery) with loaded reference state...\n");
                         dump_state_diff(reference_state_array_debug, main_state_array, SMALL_ACCOUNT_COUNT, SMALL_ACCOUNT_COUNT); // Report 100 diffs
                     }
                     fclose(f_ref_debug);
                 } else if (errno != ENOENT) { perror("DEBUG RUN: Error opening reference state file for comparison");}
                 free(reference_state_array_debug);
             }
         }
    }

    int log_fd_mmap = open(LOG_FILE, O_RDWR);
    if (log_fd_mmap < 0) {
        perror("CRITICAL: Failed to re-open log file for mmap");
        // Perform necessary cleanup before exiting
        free(main_state_array); free(temp_snap_slot); free(temp_tx_slot);
        exit(EXIT_FAILURE);
    }
    void *mapped_log_region = mmap(NULL, TOTAL_LOG_FILE_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED, log_fd_mmap, 0);
    if (mapped_log_region == MAP_FAILED) {
        perror("mmap failed");
        close(log_fd_mmap);
        free(main_state_array); free(temp_snap_slot); free(temp_tx_slot);
        exit(EXIT_FAILURE);
    }
    CheckpointHeader *log_header_ptr = (CheckpointHeader*)mapped_log_region;
    if (log_header_ptr->magic != CHECKPOINT_MAGIC) {
        fprintf(stderr, "CRITICAL: Log file header invalid after mmap!\n");
        munmap(mapped_log_region, TOTAL_LOG_FILE_SIZE); close(log_fd_mmap);
        free(main_state_array); free(temp_snap_slot); free(temp_tx_slot);
        exit(EXIT_FAILURE);
    }
     // For single cycle, current_cycle_ptr in header should always be 0.
    log_header_ptr->current_cycle_ptr = 0;


    Transaction *tx_batch_buf = malloc(BATCH_SIZE * sizeof(Transaction));
    if (!tx_batch_buf) {
        perror("Failed to allocate tx_batch_buf");
        munmap(mapped_log_region, TOTAL_LOG_FILE_SIZE); close(log_fd_mmap);
        free(main_state_array); free(temp_snap_slot); free(temp_tx_slot);
        exit(EXIT_FAILURE);
    }
    FILE *fp_tx = fopen(TX_FILE, "rb");
    if (!fp_tx) {
        perror("Error opening transactions file");
        free(tx_batch_buf);
        munmap(mapped_log_region, TOTAL_LOG_FILE_SIZE); close(log_fd_mmap);
        free(main_state_array); free(temp_snap_slot); free(temp_tx_slot);
        exit(EXIT_FAILURE);
    }

    uint32_t current_batch_num_in_loop;

    uint32_t file_batches_processed_this_run_count = 0;
    double proc_start_t_loop = get_time_ms();

    // Allocate array to store total times for each batch processed in this run
    size_t max_batches_this_run = 100000; // Arbitrary large enough value
    double *batch_total_times = malloc(max_batches_this_run * sizeof(double));
    if (!batch_total_times) {
        perror("Failed to allocate batch_total_times array");
        munmap(mapped_log_region, TOTAL_LOG_FILE_SIZE); close(log_fd_mmap);
        free(main_state_array); free(temp_snap_slot); free(temp_tx_slot); free(tx_batch_buf);
        exit(EXIT_FAILURE);
    }
    size_t batch_time_count = 0;
    printf("Starting transaction processing loop from batch %u...\n", current_batch_num_in_loop);
    while(1) {
        size_t num_tx_read = fread(tx_batch_buf, sizeof(Transaction), BATCH_SIZE, fp_tx);
        if (num_tx_read == 0) {
            if(feof(fp_tx)) printf("End of transaction file %s reached.\n", TX_FILE);
            else perror("Error reading transaction file");
            break;
        }

        double tx_apply_start_t = get_time_ms();
        for (size_t k = 0; k < num_tx_read; ++k) {
            bool curr_tx_applied = apply_transaction_to_state_array(&tx_batch_buf[k], main_state_array);
            if (!curr_tx_applied) {
                tx_batch_buf[k].amount = 0;
            }
        }
        double tx_apply_end_t = get_time_ms();

        uint32_t slot_in_cycle = current_batch_num_in_loop % NUM_STATE_CHUNKS;

        double persist_start_t = get_time_ms();
        commit_batch_data_to_log(slot_in_cycle, current_batch_num_in_loop,
                                 main_state_array, tx_batch_buf, num_tx_read,
                                 mapped_log_region, temp_snap_slot, temp_tx_slot,
                                 pagesize); // Pass pagesize
        double persist_end_t = get_time_ms();

        // Collect total time for this batch (application + persistence)
        if (batch_time_count < max_batches_this_run) {
            batch_total_times[batch_time_count++] = (tx_apply_end_t - tx_apply_start_t) + (persist_end_t - persist_start_t);
        }

        if (slot_in_cycle == (NUM_STATE_CHUNKS - 1)) {
            log_header_ptr->current_cycle_ptr = 0;
            if (msync((void*)log_header_ptr, CHECKPOINT_HEADER_SIZE, MS_SYNC) != 0) {
                 perror("CRITICAL: Failed to msync header update");
            }
            if (current_batch_num_in_loop != 2021) {
                printf("Batch %u: Full pass over %u log slots completed. Header (current_cycle_ptr=%u) synced.\n",
                       current_batch_num_in_loop, NUM_STATE_CHUNKS, log_header_ptr->current_cycle_ptr);
            }
        }
        if ((current_batch_num_in_loop % 10) == 0 || num_tx_read < BATCH_SIZE) {
            if (current_batch_num_in_loop != 2021) {
                printf("Processed batch %u (%zu tx read) in %.3f ms. Slot in cycle: %u\n",
                       current_batch_num_in_loop, num_tx_read, tx_apply_end_t - tx_apply_start_t, slot_in_cycle);
            }
        }
        file_batches_processed_this_run_count++;
        current_batch_num_in_loop++;
        if (num_tx_read < BATCH_SIZE && feof(fp_tx)) break;
    }
    current_batch_num_in_loop--;
    // Compute median and 99th percentile of batch_total_times
    if (batch_time_count > 0) {
        // Sort the array
        double *sorted_times = malloc(batch_time_count * sizeof(double));
        if (!sorted_times) {
            perror("Failed to allocate sorted_times array");
        } else {
            memcpy(sorted_times, batch_total_times, batch_time_count * sizeof(double));
            // Simple insertion sort (since batch_time_count is not huge)
            for (size_t i = 1; i < batch_time_count; ++i) {
                double key = sorted_times[i];
                size_t j = i;
                while (j > 0 && sorted_times[j-1] > key) {
                    sorted_times[j] = sorted_times[j-1];
                    --j;
                }
                sorted_times[j] = key;
            }
            // Median
            double median = (batch_time_count % 2 == 0) ?
                (sorted_times[batch_time_count/2 - 1] + sorted_times[batch_time_count/2]) / 2.0 :
                sorted_times[batch_time_count/2];
            // 99th percentile (nearest rank method)
            size_t idx_99 = (size_t)(0.99 * batch_time_count);
            if (idx_99 >= batch_time_count) idx_99 = batch_time_count - 1;
            double p99 = sorted_times[idx_99];
            printf("[STATS] Median batch total time (apply+persist): %.3f ms\n", median);
            printf("[STATS] 99th percentile batch total time (apply+persist): %.3f ms\n", p99);
            free(sorted_times);
        }
    }
    free(batch_total_times);

    printf("--- Finished processing %s (%u batches this run) in %.3f ms ---\n",
           TX_FILE, file_batches_processed_this_run_count, get_time_ms() - proc_start_t_loop);
    //printf("Final state in memory reflects batch: %d\n", current_batch_num_in_loop);
    printf("[SUMMARY] Number of batches processed from %s in this run: %u\n", TX_FILE, file_batches_processed_this_run_count);

    // Print state after all transactions for this run are processed
    //printf("--- State After Processing All Transactions in This Run (Reflects Batch: %d) ---\n", current_batch_num_in_loop);
    //for (uint64_t i = 0; i < SMALL_ACCOUNT_COUNT; ++i) {
    //    printf("Account[%02lu]: %-12ld ", i, main_state_array[i]);
    //    if ((i + 1) % 4 == 0 || i == SMALL_ACCOUNT_COUNT - 1) printf("\n");
    //}
    //printf("------------------------------------------------------------------------------------\n");

    int batch_for_final_hash_label = current_batch_num_in_loop;
    if (batch_for_final_hash_label >= 0) {
        printf("Computing and saving final state hash for batch %d...\n", batch_for_final_hash_label);
        uint64_t final_hash = compute_state_hash(main_state_array, SMALL_ACCOUNT_COUNT);
        save_state_hash_to_file(final_hash, batch_for_final_hash_label);
        if (is_reference_run) {
            printf("REFERENCE RUN: Saving current main state array to %s\n", REFERENCE_STATE_FILE);
            FILE* f_ref_save = fopen(REFERENCE_STATE_FILE, "wb");
            if (f_ref_save) {
                if(fwrite(main_state_array, ACCOUNT_SIZE, PADDED_ACCOUNT_COUNT, f_ref_save) != PADDED_ACCOUNT_COUNT) {
                    perror("REFERENCE RUN: Error writing full reference state file");
                }
                if (fflush(f_ref_save) != 0) { perror("fflush failed for reference state file");}
                int fd_ref = fileno(f_ref_save);
                if (fd_ref >= 0 && fsync(fd_ref) != 0) { perror("fsync failed for reference state file");}
                fclose(f_ref_save);
            } else { perror("REFERENCE RUN: Error opening reference state file for writing"); }
        }
    }

    printf("Finalizing log data...\n");
    if (msync(mapped_log_region, TOTAL_LOG_FILE_SIZE, MS_SYNC) != 0) { perror("Final msync of log data failed"); }
    // fsync the file descriptor for mmap region for good measure, though MS_SYNC should cover it for file-backed maps.
    if (fsync(log_fd_mmap) != 0) { perror("Final fsync of log_fd_mmap failed"); }

    munmap(mapped_log_region, TOTAL_LOG_FILE_SIZE);
    if(fp_tx) fclose(fp_tx);
    if(log_fd_mmap >=0) close(log_fd_mmap);
    free(tx_batch_buf); free(temp_snap_slot); free(temp_tx_slot); free(main_state_array);
    printf("Execution complete.\n");
    return 0;
}