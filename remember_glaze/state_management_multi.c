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

// Global stream for all program output
static FILE *g_output_stream = NULL;

// --- Definitions and Constants ---

#define BATCH_SIZE              (1ULL << 16)     // 65,536 transactions per batch
#define SMALL_ACCOUNT_COUNT     500000000UL       // Target number of accounts
#define ACCOUNT_SIZE            8              

// State Chunk configuration
#define TARGET_CHUNK_DATA_BYTES (512 * 1024)   // Needs tuning => trade-off between transaction processing and state recovery speed
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

// --- Compiler-specific macros ---
#if defined(__GNUC__) || defined(__clang__)
#  define LIKELY(x)   __builtin_expect(!!(x), 1)
#  define UNLIKELY(x) __builtin_expect(!!(x), 0)
#  define FORCE_INLINE __attribute__((always_inline)) inline
#  define PREFETCH(addr,rw,locality) __builtin_prefetch((addr),(rw),(locality))
#else
#  define LIKELY(x)   (x)
#  define UNLIKELY(x) (x)
#  define FORCE_INLINE inline
#  define PREFETCH(addr,rw,locality)
#endif

#if !defined(HAVE_AVX512)
#  if defined(__AVX512F__) && defined(__GNUC__)
#    define HAVE_AVX512 1
#  else
#    define HAVE_AVX512 0
#  endif
#endif

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

// --- Threading Data Structures ---
typedef struct {
    uint32_t slot_idx_in_cycle;
    uint32_t current_batch_num;
    void *mapped_log_region;
    long pagesize;
    uint32_t num_tx_in_batch;
    int64_t state_chunk_data[ACCOUNTS_PER_STATE_CHUNK]; // Copy of the relevant state chunk
    Transaction transactions_copy[BATCH_SIZE];           // Copy of the transactions for the batch
} CommitThreadArgs;

// --- Threading Globals ---
static pthread_t commit_thread_id;
static pthread_mutex_t commit_mutex;
static pthread_cond_t commit_cond_data_ready; // Main -> Worker: new data is ready for commit
static pthread_cond_t commit_cond_commit_done;  // Worker -> Main: commit operation is finished
static bool commit_data_is_ready_for_worker = false; // Protected by commit_mutex
static bool commit_by_worker_is_in_progress = false; // Protected by commit_mutex, true if worker is busy
static bool worker_thread_should_exit = false;   // Protected by commit_mutex

// Arguments for the commit worker thread, shared between main and worker
static CommitThreadArgs global_commit_args_for_worker;

// --Debug Functions --
/* static void dump_state_diff(const int64_t *a,
                const int64_t *b,
                size_t          count,
                size_t          max_report)
{
    size_t mismatches = 0;
    FILE *diff_fp = fopen(DIFF_OUTPUT_FILE, "w");
    if (!diff_fp) {
        //perror("dump_state_diff: Error opening diff output file");
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
        // double percentage_diff = 0.0; // Removed as unused
        // if (count > 0) {
        //     percentage_diff = ((double)mismatches / count) * 100.0;
        // }
        //fprintf(diff_fp, "dump_state_diff: %zu account(s) differ (%.2f%%). Reporting capped at %zu.\n", mismatches, percentage_diff, max_report);
        //fprintf(stderr, "dump_state_diff: %zu account(s) differ (%.2f%%). Detailed report in %s\n", mismatches, percentage_diff, (diff_fp == stderr ? "stderr" : DIFF_OUTPUT_FILE));

    }
    fprintf(diff_fp, "--- dump_state_diff finished ---\n");

    if (diff_fp != stderr) {
        fclose(diff_fp);
    }
} */

// --- Utility Functions ---
// Hash function for state verification -- tries to express distance between two states
static inline uint64_t mix64(uint64_t k) {
    k ^= k >> 33;
    k *= UINT64_C(0xff51afd7ed558ccd);
    k ^= k >> 33;
    k *= UINT64_C(0xc4ceb9fe1a85ec53);
    k ^= k >> 33;
    return k;
}

double get_time_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec * 1000.0 + ts.tv_nsec / 1e6;
}

// Placeholder for memcpy
static inline void nt_memcpy(void *dest, const void *src, size_t n) {
    memcpy(dest, src, n);
}

// Modulo operation with positive result
static inline uint32_t positive_modulo(int32_t value, uint32_t modulus) {
    int32_t result = value % (int32_t)modulus;
    return (uint32_t)(result >= 0 ? result : result + modulus);
}

// Comparison function for sorting transactions by batch number, oldest first
int compare_tx_with_batch_num(const void *a, const void *b) {
    TransactionWithBatchNum *tx_a = (TransactionWithBatchNum *)a;
    TransactionWithBatchNum *tx_b = (TransactionWithBatchNum *)b;
    // Reversed comparison to sort in descending order (newest first)
    if (tx_a->original_batch_num > tx_b->original_batch_num) return -1;
    if (tx_a->original_batch_num < tx_b->original_batch_num) return 1;
    return 0;
}

/* Fast hot-path transaction application
 * – Early bound checks
 * – Branch-prediction hints
 * – Memory prefetch on the simple-transfer path
 * – AVX2 vectorised range-set
 */
static inline bool apply_transaction_to_state_array(const Transaction *__restrict tx,
                                 int64_t *__restrict      state_array){
    /* Decode the packed sender/receiver words */
    const uint8_t  sfunc = GET_FUNC(tx->sender);
    const uint8_t  rfunc = GET_FUNC(tx->receiver);
    const uint64_t sidx  = GET_DATA(tx->sender);
    const uint64_t ridx  = GET_DATA(tx->receiver);

    /* -------- early rejection of out-of-bounds indices -------- */
    if (UNLIKELY(sidx >= PADDED_ACCOUNT_COUNT || (rfunc == 0 && ridx >= PADDED_ACCOUNT_COUNT)))
        return false;

    /* ==========================================================
     *  Hot path: simple balance transfer (func == 0 → account id)
     * ========================================================== */
    if (LIKELY(sfunc == 0 && rfunc == 0)) {
        /* Narrower array for hot small-account tier */
        if (UNLIKELY(sidx >= SMALL_ACCOUNT_COUNT ||
                     ridx >= SMALL_ACCOUNT_COUNT))
            return false;

        int64_t *__restrict from = &state_array[sidx];
        int64_t *__restrict to   = &state_array[ridx];

        PREFETCH(from, 0, 1); /* read-only prefetch */
        PREFETCH(to,   1, 1); /* write-intent prefetch */

        const int64_t amt = (int64_t)tx->amount;
        if (LIKELY(*from >= amt)) {
            *from -= amt;
            *to   += amt;
            return true;
        }
        return false; /* insufficient funds */
    }

    /* ==========================================================
     *  Cold path: range-set (func == 1 → [start,len])  
     *  Writes `amount` into accounts [start, start+len)
     * ========================================================== */
    if (LIKELY(sfunc == 1 && rfunc == 1)) {
        uint64_t start = sidx;
        uint64_t len   = ridx;          /* receiver "data" holds length */

        if (UNLIKELY(!len || start >= SMALL_ACCOUNT_COUNT))
            return false;

        uint64_t end = start + len;
        if (end > SMALL_ACCOUNT_COUNT) end = SMALL_ACCOUNT_COUNT;

        const int64_t val = (int64_t)tx->amount;
        int64_t *dst = &state_array[start];

        for (uint64_t i = 0; i < (end - start); ++i) { // Iterate using span relative to dst
            dst[i] = val;
        }
        // #endif
        return true;
    }

    /* Any other function codes -> unsupported */
    return false;
}

static FORCE_INLINE void commit_batch_data_to_log(
     uint32_t slot_idx_in_cycle,          /* current_batch_num % NUM_STATE_CHUNKS */
     uint32_t current_batch_num,
     const int64_t *__restrict full_state_array,
     const Transaction *__restrict current_transaction_batch,
     uint32_t num_tx_in_current_batch,
     void *__restrict mapped_log_region,
     ChunkSlot *__restrict temp_snap_slot,
     BatchSlot *__restrict temp_tx_slot,
     long pagesize)
 {
     /* ---- Layout helpers -------------------------------------------------- */
     const size_t snapshot_slot_header_offset = CHECKPOINT_HEADER_SIZE;
     const size_t tx_slot_array_offset        = snapshot_slot_header_offset +
                                                NUM_STATE_CHUNKS * sizeof(ChunkSlot);

     const size_t final_snap_offset = snapshot_slot_header_offset +
                                      (size_t)slot_idx_in_cycle * sizeof(ChunkSlot);
     const size_t final_tx_offset   = tx_slot_array_offset +
                                      (size_t)slot_idx_in_cycle * sizeof(BatchSlot);

     /* ---- Prepare snapshot chunk ------------------------------------------ */
     temp_snap_slot->batch_num         = current_batch_num;
     temp_snap_slot->chunk_idx_in_ring = slot_idx_in_cycle;

     const int64_t *__restrict src_state_chunk =
         full_state_array + (size_t)slot_idx_in_cycle * ACCOUNTS_PER_STATE_CHUNK;

     nt_memcpy(temp_snap_slot->state_data,
               src_state_chunk,
               ACCOUNTS_PER_STATE_CHUNK * ACCOUNT_SIZE);

     void *__restrict snap_dst = (char *)mapped_log_region + final_snap_offset;
     nt_memcpy(snap_dst, temp_snap_slot, sizeof(ChunkSlot));

     /* ---- Prepare transaction batch --------------------------------------- */
     temp_tx_slot->batch_num = current_batch_num;
     temp_tx_slot->tx_count  = num_tx_in_current_batch;

     if (num_tx_in_current_batch)
         nt_memcpy(temp_tx_slot->transactions,
                   current_transaction_batch,
                   (size_t)num_tx_in_current_batch * sizeof(Transaction));
     else
         memset(temp_tx_slot->transactions, 0, BATCH_SIZE * sizeof(Transaction));

     void *__restrict tx_dst = (char *)mapped_log_region + final_tx_offset;
     nt_memcpy(tx_dst, temp_tx_slot, sizeof(BatchSlot));

     /* ---- Persist to storage with a single msync --------------------------- */
     if (UNLIKELY(pagesize <= 0)) {
         perror("commit_batch_data_to_log: invalid pagesize");
         return;
     }

     void *sync_start = (void *)((uintptr_t)snap_dst & ~( (uintptr_t)pagesize - 1U ));
     const uintptr_t last_byte = (uintptr_t)tx_dst + sizeof(BatchSlot) - 1U;
     const size_t sync_len = ((last_byte / (uintptr_t)pagesize) * (uintptr_t)pagesize +
                              (uintptr_t)pagesize) - (uintptr_t)sync_start;

     if (UNLIKELY(msync(sync_start, sync_len, MS_SYNC))) {
         char err[256];
         snprintf(err, sizeof(err),
                  "CRITICAL: Combined msync failed (snap=%p, tx=%p, start=%p, len=%zu)",
                  snap_dst, tx_dst, sync_start, sync_len);
         perror(err);
         /* Optionally: abort(); */
     }
 }

// --- Commit Worker Thread Function ---
static void* commit_worker_func(void* arg) {
    (void)arg; // Argument not used as we use global_commit_args_for_worker

    CommitThreadArgs local_args_for_current_task; // Worker's local copy of args for safety
    ChunkSlot current_task_snap_slot; // Worker's own temp slot for snapshot
    BatchSlot current_task_tx_slot;   // Worker's own temp slot for transactions

    while (true) {
        pthread_mutex_lock(&commit_mutex);
        while (!commit_data_is_ready_for_worker && !worker_thread_should_exit) {
            pthread_cond_wait(&commit_cond_data_ready, &commit_mutex);
        }

        if (worker_thread_should_exit) {
            pthread_mutex_unlock(&commit_mutex);
            break;
        }

        // At this point, commit_data_is_ready_for_worker is true.
        // Main thread has set commit_by_worker_is_in_progress = true.
        // Copy the shared global_commit_args_for_worker to a local copy.
        local_args_for_current_task = global_commit_args_for_worker;
        commit_data_is_ready_for_worker = false; // Reset for next cycle. Main thread will set it again.
        // commit_by_worker_is_in_progress remains true until worker is done with this task.
        pthread_mutex_unlock(&commit_mutex);

        // --- Perform the commit operation using local_args_for_current_task ---
        const size_t snapshot_slot_header_offset = CHECKPOINT_HEADER_SIZE;
        const size_t tx_slot_array_offset = snapshot_slot_header_offset + NUM_STATE_CHUNKS * sizeof(ChunkSlot);

        const size_t final_snap_offset = snapshot_slot_header_offset +
                                         (size_t)local_args_for_current_task.slot_idx_in_cycle * sizeof(ChunkSlot);
        const size_t final_tx_offset = tx_slot_array_offset +
                                       (size_t)local_args_for_current_task.slot_idx_in_cycle * sizeof(BatchSlot);

        // Prepare snapshot chunk using current_task_snap_slot
        current_task_snap_slot.batch_num = local_args_for_current_task.current_batch_num;
        current_task_snap_slot.chunk_idx_in_ring = local_args_for_current_task.slot_idx_in_cycle;
        nt_memcpy(current_task_snap_slot.state_data,
                  local_args_for_current_task.state_chunk_data, // Use the copied state chunk
                  ACCOUNTS_PER_STATE_CHUNK * ACCOUNT_SIZE);

        void *__restrict snap_dst = (char *)local_args_for_current_task.mapped_log_region + final_snap_offset;
        nt_memcpy(snap_dst, &current_task_snap_slot, sizeof(ChunkSlot));

        // Prepare transaction batch using current_task_tx_slot
        current_task_tx_slot.batch_num = local_args_for_current_task.current_batch_num;
        current_task_tx_slot.tx_count  = local_args_for_current_task.num_tx_in_batch;

        if (local_args_for_current_task.num_tx_in_batch > 0) {
            nt_memcpy(current_task_tx_slot.transactions,
                      local_args_for_current_task.transactions_copy, // Use the copied transactions
                      (size_t)local_args_for_current_task.num_tx_in_batch * sizeof(Transaction));
        }
        // If num_tx_read is 0, worker will handle empty transactions_copy.

        void *__restrict tx_dst = (char *)local_args_for_current_task.mapped_log_region + final_tx_offset;
        nt_memcpy(tx_dst, &current_task_tx_slot, sizeof(BatchSlot));

        // Persist
        if (UNLIKELY(local_args_for_current_task.pagesize <= 0)) {
            perror("commit_worker_func: invalid pagesize");
        } else {
            void *sync_start = (void *)((uintptr_t)snap_dst & ~((uintptr_t)local_args_for_current_task.pagesize - 1U));
            const uintptr_t last_byte = (uintptr_t)tx_dst + sizeof(BatchSlot) - 1U;
            const size_t sync_len = ((last_byte / (uintptr_t)local_args_for_current_task.pagesize) * (uintptr_t)local_args_for_current_task.pagesize +
                                     (uintptr_t)local_args_for_current_task.pagesize) - (uintptr_t)sync_start;

            if (UNLIKELY(msync(sync_start, sync_len, MS_SYNC))) {
                char err_buf[256];
                snprintf(err_buf, sizeof(err_buf),
                         "CRITICAL (WORKER): Combined msync failed (snap=%p, tx=%p, start=%p, len=%zu, batch %u)",
                         snap_dst, tx_dst, sync_start, sync_len, local_args_for_current_task.current_batch_num);
                perror(err_buf);
                // Potentially add more robust error handling / signaling to main thread
            }
        }
        // --- End of commit operation ---

        pthread_mutex_lock(&commit_mutex);
        commit_by_worker_is_in_progress = false; // Worker is done with this task
        pthread_cond_signal(&commit_cond_commit_done); // Signal main
        pthread_mutex_unlock(&commit_mutex);
    }
    return NULL;
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
    //printf("[DEBUG] Header: version=%u, current_cycle_ptr=%u, num_state_chunks=%u, accounts_per_chunk=%u\n",
           //header.version, header.current_cycle_ptr, header.num_state_chunks, header.accounts_per_chunk);

    if (header.num_state_chunks != NUM_STATE_CHUNKS || header.accounts_per_chunk != ACCOUNTS_PER_STATE_CHUNK) {
        fprintf(stderr, "Recovery: Log file parameters mismatch. Cannot recover.\n");
        return -1;
    }
    if (header.current_cycle_ptr != 0 && CYCLES == 1) { // Sanity check for single cycle
         fprintf(stderr, "Recovery: Header current_cycle_ptr is not 0 in a single-cycle configuration. Log may be from a multi-cycle system.\n");
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
        //printf("[DEBUG] Processing chunk %u (chnk_processed_counter=%u)\n", currently_processing_chnk, chnk_processed_counter);
        ++ chnk_processed_counter;
        // Iterate through the batches that are newer than the batch of the currently processing chunk
        for (uint32_t curr_batch = batch_number_of_chunk[currently_processing_chnk] + 1; 
            curr_batch <= batch_number_of_chunk[latest_chunk_idx_overall]; 
            ++curr_batch) {

            //printf("[DEBUG] Processing batch %u for chunk %u\n", curr_batch, currently_processing_chnk);
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
    printf("Recovery Step 2: Applied %u transactions. State now reflects batch %d.\n", collected_tx_count, *last_recovered_batch_num);

    free(temp_snap_slot);
    free(temp_tx_slot);
    free(batch_number_of_chunk);

    //printf("[DEBUG] Exiting recover_state_from_log. Final state reflects batch number: %d\n", *last_recovered_batch_num);
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
    // Initialize the global output stream
    g_output_stream = fopen("program_output.log", "w");
    if (!g_output_stream) {
        // If file opening fails, print to stderr and fallback to stdout for g_output_stream
        fprintf(stderr, "CRITICAL: Failed to open program_output.log for writing: %s. All output will go to console.\n", strerror(errno));
        g_output_stream = stdout;
    }

    bool is_reference_run = false;
    if (argc > 1 && strcmp(argv[1], "saveref") == 0) {
        is_reference_run = true;
        fprintf(g_output_stream, "INFO: ***** REFERENCE RUN *****\n");
    } else {
        fprintf(g_output_stream, "INFO: ***** DEBUG/RECOVERY RUN *****\n");
    }

    fprintf(g_output_stream, "System Config: ACCOUNTS=%lu, ACCOUNT_SIZE=%d, BATCH_SIZE=%llu\n",
            SMALL_ACCOUNT_COUNT, ACCOUNT_SIZE, BATCH_SIZE);
    fprintf(g_output_stream, "               CHUNKS=%lu, ACC_PER_CHUNK=%u, PADDED_ACCOUNTS=%lu\n",
           NUM_STATE_CHUNKS, ACCOUNTS_PER_STATE_CHUNK, PADDED_ACCOUNT_COUNT);
    fprintf(g_output_stream, "               LOGGING CYCLES: %d\n", CYCLES);

    long pagesize = sysconf(_SC_PAGESIZE);
    if (pagesize == -1) {
        fprintf(g_output_stream, "CRITICAL: sysconf _SC_PAGESIZE failed: %s\n", strerror(errno));
        if (g_output_stream != stdout && g_output_stream != stderr) fclose(g_output_stream);
        exit(EXIT_FAILURE);
    }
    fprintf(g_output_stream, "System page size: %ld bytes\n", pagesize);

    int64_t *main_state_array = NULL;
    if (posix_memalign((void **)&main_state_array, 64, PADDED_ACCOUNT_COUNT * ACCOUNT_SIZE) != 0) {
        fprintf(g_output_stream, "CRITICAL: Error allocating main_state_array: %s\n", strerror(errno));
        if (g_output_stream != stdout && g_output_stream != stderr) fclose(g_output_stream);
        exit(EXIT_FAILURE);
    }

    const size_t SINGLE_CYCLE_SNAPSHOT_BYTES = NUM_STATE_CHUNKS * sizeof(ChunkSlot);
    const size_t SINGLE_CYCLE_TX_BYTES = NUM_STATE_CHUNKS * sizeof(BatchSlot);
    const size_t TOTAL_LOG_FILE_SIZE = CHECKPOINT_HEADER_SIZE +
                                   SINGLE_CYCLE_SNAPSHOT_BYTES +
                                   SINGLE_CYCLE_TX_BYTES;
    fprintf(g_output_stream, "Total log file size will be: %zu bytes (Header: %zu, Snapshots: %zu, TXlogs: %zu)\n",
           TOTAL_LOG_FILE_SIZE, CHECKPOINT_HEADER_SIZE, SINGLE_CYCLE_SNAPSHOT_BYTES, SINGLE_CYCLE_TX_BYTES);

    if (is_reference_run) {
        fprintf(g_output_stream, "Reference run: Removing old log/state files...\n");
        remove(LOG_FILE); // Errors handled by observing file presence later
        remove(STATE_HASH_FILE);
        remove(REFERENCE_STATE_FILE);
    }

    preallocate_and_init_log_file(LOG_FILE, TOTAL_LOG_FILE_SIZE); // Uses g_output_stream internally

    double rec_start_t = get_time_ms();
    int log_fd_rec = -1;
    int recovered_batch = -1;
    bool meaningful_state_recovered = false;

    if (access(LOG_FILE, F_OK) != 0 && errno == ENOENT) {
        fprintf(g_output_stream, "Recovery: %s not found. Skipping recovery phase, will start fresh.\n", LOG_FILE);
        memset(main_state_array, 0, PADDED_ACCOUNT_COUNT * ACCOUNT_SIZE);
    } else {
        log_fd_rec = open(LOG_FILE, O_RDONLY);
        if (log_fd_rec < 0) {
            fprintf(g_output_stream, "Recovery: Error opening existing log file '%s' for reading: %s. Skipping recovery.\n", LOG_FILE, strerror(errno));
            memset(main_state_array, 0, PADDED_ACCOUNT_COUNT * ACCOUNT_SIZE);
        } else {
            fprintf(g_output_stream, "Attempting to recover state from %s\n", LOG_FILE);
            if (recover_state_from_log(log_fd_rec, main_state_array, &recovered_batch) == 0) { // Uses g_output_stream
                meaningful_state_recovered = true;
                fprintf(g_output_stream, "Recovery successful. Last recovered batch: %d\n", recovered_batch);
            } else {
                fprintf(g_output_stream, "Recovery from log failed or produced no meaningful state. Initializing state to zero.\n");
                memset(main_state_array, 0, PADDED_ACCOUNT_COUNT * ACCOUNT_SIZE);
                recovered_batch = -1;
            }
            close(log_fd_rec);
        }
    }
    double rec_end_t = get_time_ms();
    fprintf(g_output_stream, "Recovery phase took %.3f ms. Last recovered batch: %d. Meaningful state recovered: %s \n",
           rec_end_t - rec_start_t, recovered_batch, meaningful_state_recovered ? "yes" : "no");
    fprintf(g_output_stream, "---------------------------------------------------------------------------------------------------\n");

    if (!meaningful_state_recovered) {
        fprintf(g_output_stream, "Initializing main state array with default balances as no prior state was recovered.\n");
        for (uint64_t i = 0; i < SMALL_ACCOUNT_COUNT; i++) main_state_array[i] = 1000000;
        for (uint64_t i = SMALL_ACCOUNT_COUNT; i < PADDED_ACCOUNT_COUNT; i++) main_state_array[i] = 0;
    } else {
        if (verify_recovered_state_hash(main_state_array, SMALL_ACCOUNT_COUNT, recovered_batch) != 0) { // Uses g_output_stream
            if (!is_reference_run) {
                fprintf(g_output_stream, "FATAL: Post-recovery state hash verification FAILED for batch %d.\n", recovered_batch);
                // Diff logic can be re-enabled here if needed, using g_output_stream
            }
        } else {
            fprintf(g_output_stream, "Post-recovery state hash verification SUCCEEDED for batch %d.\n", recovered_batch);
        }
    }

    int log_fd_mmap = open(LOG_FILE, O_RDWR);
    if (log_fd_mmap < 0) {
        fprintf(g_output_stream, "CRITICAL: Failed to re-open log file '%s' for mmap: %s\n", LOG_FILE, strerror(errno));
        free(main_state_array);
        if (g_output_stream != stdout && g_output_stream != stderr) fclose(g_output_stream);
        exit(EXIT_FAILURE);
    }
    void *mapped_log_region = mmap(NULL, TOTAL_LOG_FILE_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED, log_fd_mmap, 0);
    if (mapped_log_region == MAP_FAILED) {
        fprintf(g_output_stream, "CRITICAL: mmap failed for '%s': %s\n", LOG_FILE, strerror(errno));
        close(log_fd_mmap);
        free(main_state_array);
        if (g_output_stream != stdout && g_output_stream != stderr) fclose(g_output_stream);
        exit(EXIT_FAILURE);
    }
    CheckpointHeader *log_header_ptr = (CheckpointHeader*)mapped_log_region;
    if (log_header_ptr->magic != CHECKPOINT_MAGIC) {
        fprintf(g_output_stream, "CRITICAL: Log file '%s' header invalid after mmap!\n", LOG_FILE);
        munmap(mapped_log_region, TOTAL_LOG_FILE_SIZE); close(log_fd_mmap);
        free(main_state_array);
        if (g_output_stream != stdout && g_output_stream != stderr) fclose(g_output_stream);
        exit(EXIT_FAILURE);
    }
    log_header_ptr->current_cycle_ptr = 0;

    if (pthread_mutex_init(&commit_mutex, NULL) != 0) {
        fprintf(g_output_stream, "CRITICAL: Mutex init failed: %s\n", strerror(errno));
        munmap(mapped_log_region, TOTAL_LOG_FILE_SIZE); close(log_fd_mmap); free(main_state_array);
        if (g_output_stream != stdout && g_output_stream != stderr) fclose(g_output_stream);
        exit(EXIT_FAILURE);
    }
    if (pthread_cond_init(&commit_cond_data_ready, NULL) != 0) {
        fprintf(g_output_stream, "CRITICAL: Cond var (data_ready) init failed: %s\n", strerror(errno));
        pthread_mutex_destroy(&commit_mutex);
        munmap(mapped_log_region, TOTAL_LOG_FILE_SIZE); close(log_fd_mmap); free(main_state_array);
        if (g_output_stream != stdout && g_output_stream != stderr) fclose(g_output_stream);
        exit(EXIT_FAILURE);
    }
    if (pthread_cond_init(&commit_cond_commit_done, NULL) != 0) {
        fprintf(g_output_stream, "CRITICAL: Cond var (commit_done) init failed: %s\n", strerror(errno));
        pthread_cond_destroy(&commit_cond_data_ready); pthread_mutex_destroy(&commit_mutex);
        munmap(mapped_log_region, TOTAL_LOG_FILE_SIZE); close(log_fd_mmap); free(main_state_array);
        if (g_output_stream != stdout && g_output_stream != stderr) fclose(g_output_stream);
        exit(EXIT_FAILURE);
    }
    if (pthread_create(&commit_thread_id, NULL, commit_worker_func, NULL) != 0) { // commit_worker_func will use g_output_stream
        fprintf(g_output_stream, "CRITICAL: Thread creation failed: %s\n", strerror(errno));
        pthread_cond_destroy(&commit_cond_commit_done); pthread_cond_destroy(&commit_cond_data_ready); pthread_mutex_destroy(&commit_mutex);
        munmap(mapped_log_region, TOTAL_LOG_FILE_SIZE); close(log_fd_mmap); free(main_state_array);
        if (g_output_stream != stdout && g_output_stream != stderr) fclose(g_output_stream);
        exit(EXIT_FAILURE);
    }

    Transaction *tx_batch_buf = malloc(BATCH_SIZE * sizeof(Transaction));
    if (!tx_batch_buf) {
        fprintf(g_output_stream, "CRITICAL: Failed to allocate tx_batch_buf: %s\n", strerror(errno));
        // Signal worker to exit before cleaning up other resources
        pthread_mutex_lock(&commit_mutex);
        worker_thread_should_exit = true;
        commit_data_is_ready_for_worker = true;
        pthread_cond_signal(&commit_cond_data_ready);
        pthread_mutex_unlock(&commit_mutex);
        pthread_join(commit_thread_id, NULL); // Wait for worker to acknowledge exit
        pthread_cond_destroy(&commit_cond_commit_done); pthread_cond_destroy(&commit_cond_data_ready); pthread_mutex_destroy(&commit_mutex);
        munmap(mapped_log_region, TOTAL_LOG_FILE_SIZE); close(log_fd_mmap); free(main_state_array);
        if (g_output_stream != stdout && g_output_stream != stderr) fclose(g_output_stream);
        exit(EXIT_FAILURE);
    }
    FILE *fp_tx = fopen(TX_FILE, "rb");
    if (!fp_tx) {
        fprintf(g_output_stream, "CRITICAL: Error opening transactions file '%s': %s\n", TX_FILE, strerror(errno));
        free(tx_batch_buf);
        // Signal worker to exit (similar cleanup as above)
        pthread_mutex_lock(&commit_mutex);
        worker_thread_should_exit = true;
        commit_data_is_ready_for_worker = true;
        pthread_cond_signal(&commit_cond_data_ready);
        pthread_mutex_unlock(&commit_mutex);
        pthread_join(commit_thread_id, NULL);
        pthread_cond_destroy(&commit_cond_commit_done); pthread_cond_destroy(&commit_cond_data_ready); pthread_mutex_destroy(&commit_mutex);
        munmap(mapped_log_region, TOTAL_LOG_FILE_SIZE); close(log_fd_mmap); free(main_state_array);
        if (g_output_stream != stdout && g_output_stream != stderr) fclose(g_output_stream);
        exit(EXIT_FAILURE);
    }

    uint32_t current_batch_num_in_loop = 0;
    rewind(fp_tx);
    fprintf(g_output_stream, "MAIN: Always processing %s from batch 0 (top of file).\n", TX_FILE);
    uint32_t file_batches_processed_this_run_count = 0;
    double proc_start_t_loop = get_time_ms();
    size_t max_batches_this_run = 10000000;
    double *batch_total_times = malloc(max_batches_this_run * sizeof(double));
    if (!batch_total_times) {
        fprintf(g_output_stream, "CRITICAL: Failed to allocate batch_total_times array: %s\n", strerror(errno));
        // Signal worker to exit (similar cleanup as above)
        pthread_mutex_lock(&commit_mutex);
        worker_thread_should_exit = true;
        commit_data_is_ready_for_worker = true;
        pthread_cond_signal(&commit_cond_data_ready);
        pthread_mutex_unlock(&commit_mutex);
        pthread_join(commit_thread_id, NULL);
        pthread_cond_destroy(&commit_cond_commit_done); pthread_cond_destroy(&commit_cond_data_ready); pthread_mutex_destroy(&commit_mutex);
        fclose(fp_tx); munmap(mapped_log_region, TOTAL_LOG_FILE_SIZE); close(log_fd_mmap); free(main_state_array); free(tx_batch_buf);
        if (g_output_stream != stdout && g_output_stream != stderr) fclose(g_output_stream);
        exit(EXIT_FAILURE);
    }
    size_t batch_time_count = 0;
    fprintf(g_output_stream, "Starting transaction processing loop from batch %u...\n", current_batch_num_in_loop);

    while(1) {
        size_t num_tx_read = fread(tx_batch_buf, sizeof(Transaction), BATCH_SIZE, fp_tx);
        if (num_tx_read == 0) {
            if(feof(fp_tx)) fprintf(g_output_stream, "End of transaction file %s reached.\n", TX_FILE);
            else fprintf(g_output_stream, "Error reading transaction file %s: %s\n", TX_FILE, strerror(errno));
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

        if (file_batches_processed_this_run_count > 0) {
            pthread_mutex_lock(&commit_mutex);
            while (commit_by_worker_is_in_progress) {
                pthread_cond_wait(&commit_cond_commit_done, &commit_mutex);
            }
            pthread_mutex_unlock(&commit_mutex);
        }

        pthread_mutex_lock(&commit_mutex);
        global_commit_args_for_worker.slot_idx_in_cycle = slot_in_cycle;
        global_commit_args_for_worker.current_batch_num = current_batch_num_in_loop;
        global_commit_args_for_worker.mapped_log_region = mapped_log_region;
        global_commit_args_for_worker.pagesize = pagesize;
        global_commit_args_for_worker.num_tx_in_batch = num_tx_read;
        const int64_t *__restrict src_state_chunk_for_commit =
            main_state_array + (size_t)slot_in_cycle * ACCOUNTS_PER_STATE_CHUNK;
        nt_memcpy(global_commit_args_for_worker.state_chunk_data, src_state_chunk_for_commit, ACCOUNTS_PER_STATE_CHUNK * ACCOUNT_SIZE);
        if (num_tx_read > 0) {
            nt_memcpy(global_commit_args_for_worker.transactions_copy, tx_batch_buf, (size_t)num_tx_read * sizeof(Transaction));
        }
        commit_data_is_ready_for_worker = true;
        commit_by_worker_is_in_progress = true;
        pthread_cond_signal(&commit_cond_data_ready);
        pthread_mutex_unlock(&commit_mutex);

        if (batch_time_count < max_batches_this_run) {
            batch_total_times[batch_time_count++] = (tx_apply_end_t - tx_apply_start_t);
        }

        if (slot_in_cycle == (NUM_STATE_CHUNKS - 1)) {
            log_header_ptr->current_cycle_ptr = 0;
            if (msync((void*)log_header_ptr, CHECKPOINT_HEADER_SIZE, MS_SYNC) != 0) {
                 fprintf(g_output_stream, "CRITICAL: Failed to msync header update for batch %u: %s\n", current_batch_num_in_loop, strerror(errno));
            }
            if (current_batch_num_in_loop != 2021) {
                fprintf(g_output_stream, "Batch %u: Full pass over %lu log slots completed. Header (current_cycle_ptr=%u) synced.\n",
                       current_batch_num_in_loop, NUM_STATE_CHUNKS, log_header_ptr->current_cycle_ptr);
            }
        }
        if ((current_batch_num_in_loop % 10) == 0 || num_tx_read < BATCH_SIZE) {
            if (current_batch_num_in_loop != 2021) {
                fprintf(g_output_stream, "Processed batch %u (%zu tx read) in %.3f ms. Slot in cycle: %u\n",
                       current_batch_num_in_loop, num_tx_read, tx_apply_end_t - tx_apply_start_t, slot_in_cycle);
            }
        }
        file_batches_processed_this_run_count++;
        current_batch_num_in_loop++;
        if (num_tx_read < BATCH_SIZE && feof(fp_tx)) break;
    }
    current_batch_num_in_loop--;

    if (batch_time_count > 0) {
        double *sorted_times = malloc(batch_time_count * sizeof(double));
        if (!sorted_times) {
            fprintf(g_output_stream, "Warning: Failed to allocate sorted_times array for stats: %s\n", strerror(errno));
        } else {
            memcpy(sorted_times, batch_total_times, batch_time_count * sizeof(double));
            for (size_t i = 1; i < batch_time_count; ++i) { /* Simple insertion sort */
                double key = sorted_times[i];
                size_t j = i;
                while (j > 0 && sorted_times[j-1] > key) {
                    sorted_times[j] = sorted_times[j-1];
                    --j;
                }
                sorted_times[j] = key;
            }
            double median = (batch_time_count % 2 == 0) ?
                (sorted_times[batch_time_count/2 - 1] + sorted_times[batch_time_count/2]) / 2.0 :
                sorted_times[batch_time_count/2];
            size_t idx_99 = (size_t)(0.99 * batch_time_count);
            if (idx_99 >= batch_time_count) idx_99 = batch_time_count > 0 ? batch_time_count - 1 : 0;
            double p99 = batch_time_count > 0 ? sorted_times[idx_99] : 0.0;
            double sum_of_times = 0;
            for (size_t i = 0; i < batch_time_count; ++i) sum_of_times += batch_total_times[i];
            double average_time = (batch_time_count > 0) ? (sum_of_times / batch_time_count) : 0.0;
            fprintf(g_output_stream, "[STATS] Average batch application time: %.3f ms\n", average_time);
            fprintf(g_output_stream, "[STATS] Median batch application time: %.3f ms\n", median);
            fprintf(g_output_stream, "[STATS] 99th percentile batch application time: %.3f ms\n", p99);
            free(sorted_times);
        }
    }
    free(batch_total_times);

    fprintf(g_output_stream, "--- Finished processing %s (%u batches this run) in %.3f ms ---\n",
           TX_FILE, file_batches_processed_this_run_count, get_time_ms() - proc_start_t_loop);
    fprintf(g_output_stream, "[SUMMARY] Number of batches processed from %s in this run: %u\n", TX_FILE, file_batches_processed_this_run_count);

    int batch_for_final_hash_label = current_batch_num_in_loop;
    if (batch_for_final_hash_label >= 0) {
        fprintf(g_output_stream, "Computing and saving final state hash for batch %d...\n", batch_for_final_hash_label);
        uint64_t final_hash = compute_state_hash(main_state_array, SMALL_ACCOUNT_COUNT);
        save_state_hash_to_file(final_hash, batch_for_final_hash_label); // Uses g_output_stream
        if (is_reference_run) {
            fprintf(g_output_stream, "REFERENCE RUN: Saving current main state array to %s\n", REFERENCE_STATE_FILE);
            FILE* f_ref_save = fopen(REFERENCE_STATE_FILE, "wb");
            if (f_ref_save) {
                if(fwrite(main_state_array, ACCOUNT_SIZE, PADDED_ACCOUNT_COUNT, f_ref_save) != PADDED_ACCOUNT_COUNT) {
                    fprintf(g_output_stream, "REFERENCE RUN: Error writing full reference state file: %s\n", strerror(errno));
                }
                if (fflush(f_ref_save) != 0) { fprintf(g_output_stream, "REFERENCE RUN: fflush failed for reference state file: %s\n", strerror(errno));}
                int fd_ref = fileno(f_ref_save);
                if (fd_ref >= 0 && fsync(fd_ref) != 0) { fprintf(g_output_stream, "REFERENCE RUN: fsync failed for reference state file: %s\n", strerror(errno));}
                fclose(f_ref_save);
            } else { 
                fprintf(g_output_stream, "REFERENCE RUN: Error opening reference state file '%s' for writing: %s\n", REFERENCE_STATE_FILE, strerror(errno)); 
            }
        }
    }

    fprintf(g_output_stream, "Finalizing log data...\n");
    if (msync(mapped_log_region, TOTAL_LOG_FILE_SIZE, MS_SYNC) != 0) { 
        fprintf(g_output_stream, "Final msync of log data failed: %s\n", strerror(errno)); 
    }
    if (fsync(log_fd_mmap) != 0) { 
        fprintf(g_output_stream, "Final fsync of log_fd_mmap failed: %s\n", strerror(errno)); 
    }

    pthread_mutex_lock(&commit_mutex);
    while (commit_by_worker_is_in_progress) {
        pthread_cond_wait(&commit_cond_commit_done, &commit_mutex);
    }
    worker_thread_should_exit = true;
    commit_data_is_ready_for_worker = true;
    pthread_cond_signal(&commit_cond_data_ready);
    pthread_mutex_unlock(&commit_mutex);
    pthread_join(commit_thread_id, NULL);

    pthread_mutex_destroy(&commit_mutex);
    pthread_cond_destroy(&commit_cond_data_ready);
    pthread_cond_destroy(&commit_cond_commit_done);

    munmap(mapped_log_region, TOTAL_LOG_FILE_SIZE);
    if(fp_tx) fclose(fp_tx);
    if(log_fd_mmap >=0) close(log_fd_mmap);
    free(tx_batch_buf);
    free(main_state_array);
    fprintf(g_output_stream, "Execution complete.\n");

    // Close the global output stream if it's a file
    if (g_output_stream != stdout && g_output_stream != stderr) {
        fflush(g_output_stream); // Ensure all buffered output is written
        fclose(g_output_stream);
    }
    return 0;
}
