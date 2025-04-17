#define _POSIX_C_SOURCE 200809L
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <errno.h>
#include <omp.h>

// -----------------------------------------------------------------------------
//  Global configuration
// -----------------------------------------------------------------------------
#define BATCH_SIZE          (1u << 16)      // 65,536 transactions per batch
#define TEST_BATCHES        10              // Number of batches per candidate ring­‑size test
#define SMALL_ACCOUNT_COUNT 2000000UL       // Total number of accounts in the toy ledger
#define MAX_TX_COUNT        BATCH_SIZE      // Maximum transactions stored per chunk per batch
#define CYCLES              2               // Double‑buffered ring‑buffer (snapshot cycles)

// -----------------------------------------------------------------------------
//  Bit‑packing helpers for the simulated “address”/opcode field
// -----------------------------------------------------------------------------
#define FUNC_MASK   0xF000000000000000UL
#define DATA_MASK   0x0FFFFFFFFFFFFFFFUL
#define GET_FUNC(x) ((uint8_t)((x) >> 60))
#define GET_DATA(x) ((x) & DATA_MASK)

#define likely(x)   __builtin_expect((x), 1)
#define unlikely(x) __builtin_expect((x), 0)

// -----------------------------------------------------------------------------
//  Data structures
// -----------------------------------------------------------------------------

typedef struct {
    uint64_t sender;      // high 4 bits = opcode, low 60 bits = account id
    uint64_t receiver;    // same layout
    uint32_t amount;      // tiny fixed‑point token amount
} Transaction;

typedef struct SnapshotSlot {
    uint32_t batch_num;      // batch that produced the snapshot
    uint32_t chunk_offset;   // first account index stored in *state
    int64_t *state;          // owned buffer (malloc‑ed) holding the balances
} SnapshotSlot;

typedef struct TxSlot {
    uint32_t base_snapshot_slot;  // which snapshot chunk we relate to
    uint32_t batch_num;           // batch when recorded
    uint32_t tx_count;            // number of tx stored in *transactions
    Transaction *transactions;    // owned buffer (malloc‑ed)
} TxSlot;

// -----------------------------------------------------------------------------
//  Timing helper
// -----------------------------------------------------------------------------
static inline double get_time_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec * 1000.0 + ts.tv_nsec / 1e6;
}

// -----------------------------------------------------------------------------
//  Fast path: apply a single TX and stage it for flush
// -----------------------------------------------------------------------------
static inline void apply_tx(const Transaction *tx,
                            int64_t           *state,
                            Transaction      **tx_accum,
                            int               *tx_count,
                            uint32_t           accounts_per_chunk,
                            uint32_t           ring_size)
{
    const uint8_t  sfunc = GET_FUNC(tx->sender);
    const uint8_t  rfunc = GET_FUNC(tx->receiver);
    const uint64_t sidx  = GET_DATA(tx->sender);
    const uint64_t ridx  = GET_DATA(tx->receiver);

    if (unlikely(sfunc || rfunc))                   // we only model opcode 0
        return;

    if (unlikely(sidx >= SMALL_ACCOUNT_COUNT || ridx >= SMALL_ACCOUNT_COUNT)) {
        fprintf(stderr, "[apply_tx] index out of bounds\n");
        return;
    }

    // simple debit / credit check
    if (state[sidx] > tx->amount)
        state[sidx] -= tx->amount;
    if (state[ridx] > tx->amount)
        state[ridx] += tx->amount;

    uint32_t chunk_s = sidx / accounts_per_chunk;
    uint32_t chunk_r = ridx / accounts_per_chunk;

    // clamp in case integer division produced ring_size (can only happen for
    // the very last 0‑..(ring_size‑1) leftover accounts)
    if (chunk_s >= ring_size) chunk_s = ring_size - 1;
    if (chunk_r >= ring_size) chunk_r = ring_size - 1;

    if (tx_count[chunk_s] < MAX_TX_COUNT) {
        tx_accum[chunk_s][tx_count[chunk_s]++] = *tx;
    }
    if (chunk_r != chunk_s && tx_count[chunk_r] < MAX_TX_COUNT) {
        tx_accum[chunk_r][tx_count[chunk_r]++] = *tx;
    }
}

// -----------------------------------------------------------------------------
//  Commit one chunk into the simulated checkpoint region
// -----------------------------------------------------------------------------
static void commit_chunk_v2(uint32_t        cycle,
                            uint32_t        chunk_index,
                            uint32_t        batch_num,
                            uint32_t        ring_size,
                            int64_t        *state,
                            Transaction   **tx_accum,
                            int            *tx_count,
                            void           *mapped_region,
                            uint32_t        accounts_per_chunk)
{
    /* header: just three uint32_t placeholders, aligned to 16 bytes */
    const size_t header_size = 16; /* guarantees 8‑byte alignment afterwards */

    const uint32_t slot_index           = cycle * ring_size + chunk_index;
    const uint32_t total_snapshot_slots = ring_size * CYCLES;

    // ----------  SnapshotSlot  ---------------------------------------------
    SnapshotSlot *snap_slot = (SnapshotSlot *)((char *)mapped_region +
                              header_size + slot_index * sizeof(SnapshotSlot));

    if (snap_slot->state == NULL) {
        snap_slot->state = malloc(accounts_per_chunk * sizeof(int64_t));
        if (!snap_slot->state) { perror("malloc snap_slot->state"); exit(EXIT_FAILURE); }
    }

    snap_slot->batch_num    = batch_num;
    snap_slot->chunk_offset = chunk_index * accounts_per_chunk;

    size_t copy_len = SMALL_ACCOUNT_COUNT - snap_slot->chunk_offset;
    if (copy_len > accounts_per_chunk)
        copy_len = accounts_per_chunk;   // every chunk except the last uses full size

    memcpy(snap_slot->state,
           state + snap_slot->chunk_offset,
           copy_len * sizeof(int64_t));

    // ----------  TxSlot  ----------------------------------------------------
    TxSlot *tx_slot = (TxSlot *)((char *)mapped_region +
                       header_size + total_snapshot_slots * sizeof(SnapshotSlot) +
                       slot_index * sizeof(TxSlot));

    if (tx_slot->transactions == NULL) {
        tx_slot->transactions = malloc(MAX_TX_COUNT * sizeof(Transaction));
        if (!tx_slot->transactions) { perror("malloc tx_slot->transactions"); exit(EXIT_FAILURE); }
    }

    tx_slot->base_snapshot_slot = chunk_index;
    tx_slot->batch_num          = batch_num;
    tx_slot->tx_count           = tx_count[chunk_index];

    memcpy(tx_slot->transactions,
           tx_accum[chunk_index],
           tx_slot->tx_count * sizeof(Transaction));

    tx_count[chunk_index] = 0;  // clear accumulator for next round
}

// -----------------------------------------------------------------------------
//  Random transaction generator (opcode 0 only)
// -----------------------------------------------------------------------------
static inline Transaction generate_random_transaction(void) {
    Transaction tx;
    tx.sender   = rand() % SMALL_ACCOUNT_COUNT;  // opcode bits stay zero
    tx.receiver = rand() % SMALL_ACCOUNT_COUNT;
    tx.amount   = (rand() % 100) + 1;
    return tx;
}

// -----------------------------------------------------------------------------
//  Run one benchmark pass for a given ring‑size
// -----------------------------------------------------------------------------
static double run_test_for_ring_size(uint32_t ring_size) {
    const uint32_t accounts_per_chunk = (SMALL_ACCOUNT_COUNT + ring_size - 1) / ring_size; // ceil

    const uint32_t total_snapshot_slots = ring_size * CYCLES;
    const uint32_t total_tx_slots       = ring_size * CYCLES;

    const size_t total_checkpoint_size  = 16 +   // header
        total_snapshot_slots * sizeof(SnapshotSlot) +
        total_tx_slots       * sizeof(TxSlot);

    // full in‑memory state
    int64_t *state = malloc(SMALL_ACCOUNT_COUNT * sizeof(int64_t));
    if (!state) { perror("malloc state"); exit(EXIT_FAILURE); }
    for (uint64_t i = 0; i < SMALL_ACCOUNT_COUNT; ++i) state[i] = 1'000'000;

    Transaction **tx_accum = malloc(ring_size * sizeof(Transaction *));
    int          *tx_count = malloc(ring_size * sizeof(int));
    if (!tx_accum || !tx_count) { perror("malloc tx_accum/tx_count"); exit(EXIT_FAILURE); }
    for (uint32_t i = 0; i < ring_size; ++i) {
        tx_accum[i] = malloc(MAX_TX_COUNT * sizeof(Transaction));
        if (!tx_accum[i]) { perror("malloc tx_accum[i]"); exit(EXIT_FAILURE); }
        tx_count[i] = 0;
    }

    void *mapped_region = calloc(1, total_checkpoint_size);
    if (!mapped_region) { perror("calloc mapped_region"); exit(EXIT_FAILURE); }

    double total_ms = 0.0;

    for (uint32_t batch_num = 0; batch_num < TEST_BATCHES; ++batch_num) {
        const double start_ms = get_time_ms();

        for (uint32_t i = 0; i < BATCH_SIZE; ++i) {
            Transaction tx = generate_random_transaction();
            apply_tx(&tx, state, tx_accum, tx_count,
                     accounts_per_chunk, ring_size);
        }

        const uint32_t chunk = batch_num % ring_size;
        const uint32_t cycle = (batch_num / ring_size) % CYCLES;

        commit_chunk_v2(cycle, chunk, batch_num, ring_size,
                        state, tx_accum, tx_count,
                        mapped_region, accounts_per_chunk);

        total_ms += get_time_ms() - start_ms;
    }

    // --- cleanup (intentionally leak per‑slot mallocs inside mapped_region, they
    //     are small and would complicate the demo) --------------------------------
    free(state);
    for (uint32_t i = 0; i < ring_size; ++i) free(tx_accum[i]);
    free(tx_accum);
    free(tx_count);
    free(mapped_region);

    return total_ms / TEST_BATCHES;
}

// -----------------------------------------------------------------------------
//  Main: search for the best ring‑size
// -----------------------------------------------------------------------------
int main(void) {
    srand((unsigned)time(NULL));

    const uint32_t min_ring = 2;
    const uint32_t max_ring = 20;

    uint32_t best_ring = min_ring;
    double   best_time = 1e30;

    printf("Optimizing ring size over candidate range [%u, %u]\n", min_ring, max_ring);

    for (uint32_t r = min_ring; r <= max_ring; ++r) {
        double avg_ms = run_test_for_ring_size(r);
        printf("Ring size %2u : average batch = %8.3f ms\n", r, avg_ms);
        if (avg_ms < best_time) { best_time = avg_ms; best_ring = r; }
    }

    printf("\nOptimal ring size: %u  (%.3f ms)\n", best_ring, best_time);
    return 0;
}
