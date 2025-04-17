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

typedef struct {
    uint64_t sender;    // Also encodes function in high 4 bits and index in low 60 bits.
    uint64_t receiver;  // Same encoding.
    uint32_t amount;
} Transaction;

typedef struct SnapshotSlot {
    uint32_t batch_num;               // Batch when this snapshot was taken.
    uint32_t chunk_offset;            // Starting index in the full state.
    int64_t *state;                   // Pointer to an array of account balances.
} SnapshotSlot;

typedef struct TxSlot {
    uint32_t base_snapshot_slot;      // Which chunk this log is attached to.
    uint32_t batch_num;               // Batch when recorded.
    uint32_t tx_count;                // How many transactions stored.
    Transaction *transactions;        // Pointer to an array of MAX_TX_COUNT transactions.
} TxSlot;

// --- Utility: current time in milliseconds ---
static double get_time_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec * 1000.0 + ts.tv_nsec / 1e6;
}

// --- Apply a single transaction ---
static inline void apply_tx(const Transaction *tx, int64_t *state,
                             Transaction **tx_accum, int *tx_count,
                             uint32_t state_chunk_count) {
    uint8_t sfunc = GET_FUNC(tx->sender);
    uint8_t rfunc = GET_FUNC(tx->receiver);
    uint64_t sidx = GET_DATA(tx->sender);
    uint64_t ridx = GET_DATA(tx->receiver);

    if(likely(sfunc == 0 && rfunc == 0)) {
        if(unlikely(sidx >= SMALL_ACCOUNT_COUNT || ridx >= SMALL_ACCOUNT_COUNT)) {
            fprintf(stderr, "Out-of-bounds transaction index.\n");
            return;
        }
        if(state[sidx] > tx->amount)
            state[sidx] -= tx->amount;
        if(state[ridx] > tx->amount)
            state[ridx] += tx->amount;

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
}

// --- Commit Function ---
static void commit_chunk_v2(uint32_t cycle, uint32_t chunk_index, uint32_t batch_num,
                            uint32_t ring_size,
                            int64_t *state, Transaction **tx_accum, int *tx_count,
                            void *mapped_region,
                            uint32_t state_chunk_count) {
    const size_t header_size = sizeof(uint32_t) * 3;
    const uint32_t slot_index = cycle * ring_size + chunk_index;
    const uint32_t total_snapshot_slots = ring_size * CYCLES;

    // ----- snapshot slot -----
    SnapshotSlot *snap_slot = (SnapshotSlot *)((char*)mapped_region +
            header_size + slot_index * sizeof(SnapshotSlot));

    if (snap_slot->state == NULL) {
        snap_slot->state = malloc(state_chunk_count * sizeof(int64_t));
        if (!snap_slot->state) { perror("malloc snap_slot->state"); exit(EXIT_FAILURE); }
    }

    snap_slot->batch_num    = batch_num;
    snap_slot->chunk_offset = chunk_index * state_chunk_count;

    memcpy(snap_slot->state,
           state + snap_slot->chunk_offset,
           state_chunk_count * sizeof(int64_t));

    // ----- tx slot -----
    TxSlot *tx_slot = (TxSlot *)((char*)mapped_region +
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

    tx_count[chunk_index] = 0;
}

// --- Helper: Generate a random transaction ---
static Transaction generate_random_transaction(void) {
    Transaction tx;
    uint64_t sender_idx   = rand() % SMALL_ACCOUNT_COUNT;
    uint64_t receiver_idx = rand() % SMALL_ACCOUNT_COUNT;
    tx.sender   = sender_idx;
    tx.receiver = receiver_idx;
    tx.amount   = (rand() % 100) + 1;
    return tx;
}

// --- Run a benchmark for a given ring size ---
static double run_test_for_ring_size(uint32_t ring_size) {
    uint32_t state_chunk_count    = SMALL_ACCOUNT_COUNT / ring_size;
    uint32_t total_snapshot_slots = ring_size * CYCLES;
    uint32_t total_tx_slots       = ring_size * CYCLES;

    size_t total_checkpoint_size  = sizeof(uint32_t) * 3 +
                                    total_snapshot_slots * sizeof(SnapshotSlot) +
                                    total_tx_slots      * sizeof(TxSlot);

    int64_t *state = malloc(SMALL_ACCOUNT_COUNT * sizeof(int64_t));
    if (!state) { perror("malloc state"); exit(EXIT_FAILURE); }
    for (uint64_t i = 0; i < SMALL_ACCOUNT_COUNT; ++i)
        state[i] = 1000000;

    Transaction **tx_accum = malloc(ring_size * sizeof(Transaction *));
    int *tx_count          = malloc(ring_size * sizeof(int));
    if (!tx_accum || !tx_count) { perror("malloc tx_accum/tx_count"); exit(EXIT_FAILURE); }
    for (uint32_t i = 0; i < ring_size; ++i) {
        tx_accum[i] = malloc(MAX_TX_COUNT * sizeof(Transaction));
        if (!tx_accum[i]) { perror("malloc tx_accum[i]"); exit(EXIT_FAILURE); }
        tx_count[i]  = 0;
    }

    void *mapped_region = calloc(1, total_checkpoint_size);
    if (!mapped_region) { perror("calloc mapped_region"); exit(EXIT_FAILURE); }

    double total_time_ms = 0.0;

    for (uint32_t batch_num = 0; batch_num < TEST_BATCHES; ++batch_num) {
        const double start = get_time_ms();

        for (uint32_t i = 0; i < BATCH_SIZE; ++i) {
            Transaction tx = generate_random_transaction();
            apply_tx(&tx, state, tx_accum, tx_count, state_chunk_count);
        }

        const uint32_t chunk = batch_num % ring_size;
        const uint32_t cycle = (batch_num / ring_size) % CYCLES;

        commit_chunk_v2(cycle, chunk, batch_num,
                        ring_size,
                        state, tx_accum, tx_count,
                        mapped_region,
                        state_chunk_count);

        total_time_ms += (get_time_ms() - start);
    }

    free(state);

    for (uint32_t i = 0; i < ring_size; ++i)
        free(tx_accum[i]);
    free(tx_accum);
    free(tx_count);
    free(mapped_region);

    return total_time_ms / TEST_BATCHES;
}

// -----------------------------------------------------------------------
int main(void) {
    srand((unsigned)time(NULL));

    const uint32_t min_ring = 2;
    const uint32_t max_ring = 20;

    uint32_t best_ring = min_ring;
    double   best_time = 1e9;

    printf("Optimizing ring size over candidate range [%u, %u]\n",
           min_ring, max_ring);

    for (uint32_t ring = min_ring; ring <= max_ring; ++ring) {
        double avg = run_test_for_ring_size(ring);
        printf("Ring size %2u : average batch = %8.3f ms\n", ring, avg);

        if (avg < best_time) {
            best_time = avg;
            best_ring = ring;
        }
    }

    printf("\nOptimal ring size: %u  (%.3f ms)\n", best_ring, best_time);
    return 0;
}
