#define _POSIX_C_SOURCE 200809L
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <errno.h>
#include <omp.h>

// --- Global Constants ---
#define BATCH_SIZE          (1 << 16)      // 65,536 transactions per batch
#define TEST_BATCHES        10             // Number of batches per candidate ring size test
#define SMALL_ACCOUNT_COUNT 2000000UL      // Total number of accounts
#define MAX_TX_COUNT        (BATCH_SIZE)   // Maximum transactions per chunk per batch
#define CYCLES              2              // Number of cycles (double buffering for checkpointing)

// --- Operation Encoding ---
#define FUNC_MASK   0xF000000000000000UL
#define DATA_MASK   0x0FFFFFFFFFFFFFFFUL
#define GET_FUNC(x) ((uint8_t)((x) >> 60))
#define GET_DATA(x) ((x) & DATA_MASK)

// Use branch prediction hints.
#define likely(x)       __builtin_expect((x),1)
#define unlikely(x)     __builtin_expect((x),0)

// --- Data Structures ---

// Transaction structure.
typedef struct {
    uint64_t sender;    // Also encodes function in high 4 bits and index in low 60 bits.
    uint64_t receiver;  // Same encoding.
    uint32_t amount;
} Transaction;

// SnapshotSlot stores one chunk of state.
typedef struct SnapshotSlot {
    uint32_t batch_num;               // Batch when this snapshot was taken.
    uint32_t chunk_offset;            // Starting index in the full state.
    int64_t *state;                   // Pointer to an array of account balances.
} SnapshotSlot;

// TxSlot stores the transactions for a chunk.
typedef struct TxSlot {
    uint32_t base_snapshot_slot;      // Which chunk this log is attached to.
    uint32_t batch_num;               // Batch when recorded.
    uint32_t tx_count;                // How many transactions stored.
    Transaction *transactions;        // Pointer to an array of MAX_TX_COUNT transactions.
} TxSlot;

// --- Utility: current time in milliseconds ---
double get_time_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec * 1000.0 + ts.tv_nsec / 1e6;
}

// --- The transaction application function ---
// This function applies a single transaction in an ordered fashion.
static inline void apply_tx(const Transaction *tx, int64_t *state,
                             Transaction **tx_accum, int *tx_count,
                             uint32_t state_chunk_count) {
    // In our simulation we assume type 0 (debit/credit).
    uint8_t sfunc = GET_FUNC(tx->sender);
    uint8_t rfunc = GET_FUNC(tx->receiver);
    uint64_t sidx = GET_DATA(tx->sender);
    uint64_t ridx = GET_DATA(tx->receiver);

    // We assume both are type 0.
    if(likely(sfunc == 0 && rfunc == 0)) {
        // Check bounds.
        if(unlikely(sidx >= SMALL_ACCOUNT_COUNT || ridx >= SMALL_ACCOUNT_COUNT)) {
            fprintf(stderr, "Out-of-bounds transaction index.\n");
            return;
        }
        // Apply debits/credits sequentially.
        if(state[sidx] > tx->amount)
            state[sidx] -= tx->amount;
        if(state[ridx] > tx->amount)
            state[ridx] += tx->amount;
        // Determine which chunk each index belongs to.
        uint32_t chunk_s = sidx / state_chunk_count;
        uint32_t chunk_r = ridx / state_chunk_count;
        if(tx_count[chunk_s] < MAX_TX_COUNT) {
            tx_accum[chunk_s][tx_count[chunk_s]] = *tx;
            tx_count[chunk_s]++;
        }
        if(chunk_r != chunk_s && tx_count[chunk_r] < MAX_TX_COUNT) {
            tx_accum[chunk_r][tx_count[chunk_r]] = *tx;
            tx_count[chunk_r]++;
        }
    }
    // (For simplicity we do not simulate "set" transactions in this benchmark.)
}

// --- Commit Function ---
// For a given chunk (determined by candidate ring size), commits the in-memory state
// and the accumulated transactions into a simulated checkpoint region.
static void commit_chunk_v2(uint32_t cycle, uint32_t chunk_index, uint32_t batch_num,
                            int64_t *state, Transaction **tx_accum, int *tx_count,
                            void *mapped_region, TxSlot *prealloc_tx_slot,
                            uint32_t state_chunk_count) {
    // Compute which slot index to use.
    uint32_t slot_index = cycle *  /* ring_size */ 0 /* dummy */ + chunk_index; // will set ring_size below
    // We compute offsets in the checkpoint "region" (which is a malloc-ed buffer)
    // In our simulation, mapped_region is simply an allocated block.
    // The layout is: [CheckpointHeader][SnapshotSlots][TxSlots]
    // We'll compute offsets at runtime.

    // (We assume that preallocated SnapshotSlot and TxSlot arrays were allocated properly.)
    // Here we copy the current chunk of state and the accumulated transactions into these buffers.
    // In our simulation, we simply update the prealloc_tx_slot structure.
    
    // For the snapshot slot, we store the batch number and the pointer to state chunk.
    SnapshotSlot *snap_slot = (SnapshotSlot *)((char*)mapped_region + /* header size */ sizeof(uint32_t)*3 +
                            chunk_index * sizeof(SnapshotSlot));
    snap_slot->batch_num = batch_num;
    snap_slot->chunk_offset = chunk_index * state_chunk_count;
    // Instead of copying entire state (which might be large), in an optimizer you might want to
    // simulate the cost of a memory copy. Here we do it with memcpy.
    memcpy(snap_slot->state, state + chunk_index * state_chunk_count,
           state_chunk_count * sizeof(int64_t));

    // For the transaction slot, store transactions from the accumulator.
    TxSlot *tx_slot = (TxSlot *)((char*)mapped_region + sizeof(uint32_t)*3 +
                         (/* total_snapshot_slots */ 0 /* dummy */ * sizeof(SnapshotSlot)) +
                         chunk_index * sizeof(TxSlot));
    tx_slot->base_snapshot_slot = chunk_index;
    tx_slot->batch_num = batch_num;
    tx_slot->tx_count = tx_count[chunk_index];
    size_t tx_bytes = tx_slot->tx_count * sizeof(Transaction);
    memcpy(tx_slot->transactions, tx_accum[chunk_index], tx_bytes);

    // Clear the accumulator for this chunk.
    tx_count[chunk_index] = 0;
}

// --- Helper: Generate a random transaction of type 0 ---
// This function generates two random account indices and an amount in a fixed range.
Transaction generate_random_transaction(void) {
    Transaction tx;
    // Encode as type 0: high 4 bits zero, low 60 bits is account index.
    uint64_t sender_idx = rand() % SMALL_ACCOUNT_COUNT;
    uint64_t receiver_idx = rand() % SMALL_ACCOUNT_COUNT;
    tx.sender = sender_idx;    // sfunc==0 because top bits are 0
    tx.receiver = receiver_idx;
    tx.amount = (rand() % 100) + 1; // amount between 1 and 100
    return tx;
}

// --- run_test_for_ring_size ---
// This function runs TEST_BATCHES batches with the given candidate ring size,
// measures the average batch processing time, and returns it.
// All state and checkpoint buffers are allocated on the heap.
double run_test_for_ring_size(uint32_t ring_size) {
    // Compute dynamic parameters.
    uint32_t state_chunk_count = SMALL_ACCOUNT_COUNT / ring_size;
    size_t state_chunk_size = state_chunk_count * sizeof(int64_t);
    uint32_t total_snapshot_slots = ring_size * CYCLES;
    uint32_t total_tx_slots = ring_size * CYCLES;
    size_t total_checkpoint_size = sizeof(uint32_t)*3 + /* simulated header size */
                                   total_snapshot_slots * sizeof(SnapshotSlot) +
                                   total_tx_slots * sizeof(TxSlot);
    // Allocate full state.
    int64_t *state = malloc(SMALL_ACCOUNT_COUNT * sizeof(int64_t));
    if (!state) { perror("malloc state"); exit(EXIT_FAILURE); }
    for (uint64_t i = 0; i < SMALL_ACCOUNT_COUNT; i++) {
        state[i] = 1000000;
    }
    // Allocate per-chunk transaction accumulators.
    Transaction **tx_accum = malloc(ring_size * sizeof(Transaction *));
    int *tx_count = malloc(ring_size * sizeof(int));
    if(!tx_accum || !tx_count) { perror("malloc tx_accum/tx_count"); exit(EXIT_FAILURE); }
    for (uint32_t i = 0; i < ring_size; i++) {
        tx_accum[i] = malloc(MAX_TX_COUNT * sizeof(Transaction));
        tx_count[i] = 0;
        if(!tx_accum[i]) { perror("malloc tx_accum[i]"); exit(EXIT_FAILURE); }
    }
    // Preallocate TxSlot buffers per chunk.
    TxSlot **prealloc_tx_slots = malloc(ring_size * sizeof(TxSlot *));
    if(!prealloc_tx_slots) { perror("malloc prealloc_tx_slots"); exit(EXIT_FAILURE); }
    for (uint32_t i = 0; i < ring_size; i++) {
        prealloc_tx_slots[i] = malloc(sizeof(TxSlot));
        if(!prealloc_tx_slots[i]) { perror("malloc prealloc_tx_slots[i]"); exit(EXIT_FAILURE); }
        // Also allocate the transaction array for TxSlot.
        prealloc_tx_slots[i]->transactions = malloc(MAX_TX_COUNT * sizeof(Transaction));
        if(!prealloc_tx_slots[i]->transactions) { perror("malloc txslot transactions"); exit(EXIT_FAILURE); }
    }
    // Allocate a simulated checkpoint region (in lieu of mmapped file).
    void *mapped_region = malloc(total_checkpoint_size);
    if(!mapped_region) { perror("malloc mapped_region"); exit(EXIT_FAILURE); }
    memset(mapped_region, 0, total_checkpoint_size);

    double total_time = 0.0;
    for (uint32_t batch_num = 0; batch_num < TEST_BATCHES; batch_num++) {
        double batch_start = get_time_ms();
        // Process one batch.
        for (uint32_t i = 0; i < BATCH_SIZE; i++) {
            Transaction tx = generate_random_transaction();
            apply_tx(&tx, state, tx_accum, tx_count, state_chunk_count);
        }
        // Determine which chunk to commit.
        uint32_t chunk = batch_num % ring_size;
        uint32_t cycle = (batch_num / ring_size) % CYCLES;
        // (For simplicity, we do not recompute offsets in commit_chunk_v2; assume it uses ring_size.)
        commit_chunk_v2(cycle, chunk, batch_num, state, tx_accum, tx_count,
                        mapped_region, prealloc_tx_slots[chunk], state_chunk_count);
        double batch_end = get_time_ms();
        total_time += (batch_end - batch_start);
    }
    double avg_time = total_time / TEST_BATCHES;

    // Cleanup.
    free(state);
    for (uint32_t i = 0; i < ring_size; i++) {
        free(tx_accum[i]);
        free(prealloc_tx_slots[i]->transactions);
        free(prealloc_tx_slots[i]);
    }
    free(tx_accum);
    free(tx_count);
    free(prealloc_tx_slots);
    free(mapped_region);
    return avg_time;
}

// --- Main: Optimizer for Ring Size ---
int main(void) {
    srand(time(NULL));

    // Let the optimizer try candidate ring sizes from min to max.
    uint32_t min_ring = 2, max_ring = 20;
    uint32_t best_ring = min_ring;
    double best_time = 1e9;

    printf("Optimizing ring size over candidate range [%u, %u]\n", min_ring, max_ring);
    for (uint32_t candidate = min_ring; candidate <= max_ring; candidate++) {
        double avg_time = run_test_for_ring_size(candidate);
        printf("Ring size %u : Average batch time = %.3f ms\n", candidate, avg_time);
        if (avg_time < best_time) {
            best_time = avg_time;
            best_ring = candidate;
        }
    }
    printf("\nOptimal ring size determined: %u (average batch time = %.3f ms)\n", best_ring, best_time);
    return 0;
}