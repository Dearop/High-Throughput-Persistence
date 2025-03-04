#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdbool.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <time.h>
#include <math.h>

// ---------------- Configuration ----------------

// Each batch can have up to 65536 transactions
#define BATCH_SIZE          (1 << 16)

// We have 2 million accounts total
#define MAX_ACCOUNTS        2000000UL

// We'll do 50 total batches
#define TOTAL_BATCHES       5000

// We'll store accounts in chunks of 32768 => ~512 KB each
#define CHUNK_SIZE_ACCOUNTS 32768
#define NUM_CHUNKS          ((MAX_ACCOUNTS + CHUNK_SIZE_ACCOUNTS - 1) / CHUNK_SIZE_ACCOUNTS)

// The ring can store 2 * NUM_CHUNKS entries
#define RING_CAPACITY       (NUM_CHUNKS * 2)

// The initial balance for accounts
#define INITIAL_BALANCE     1000000UL

// The ring log file, and the transaction file
#define RING_FILE           "ring_log.bin"
#define TX_FILE             "transactions.bin"

// -------------- Data Structures --------------

// A single transaction
typedef struct {
    uint64_t sender;
    uint64_t receiver;
    uint32_t amount;
} Transaction;

// Each account snippet item
typedef struct {
    uint64_t address;
    uint64_t balance;
} Account;

/**
 * For each ring entry, we store:
 *   - valid_marker (1 => fully valid, 0 => partial)
 *   - chunk_id (which chunk is this)
 *   - tx_count
 *   - transactions[] (the batch that updated this chunk)
 *   - pre_accounts[] (the old chunk snippet from before these transactions were applied)
 *
 * On normal operation:
 *   1) In memory, chunk is up-to-date. We want to store the "old snippet," so we do:
 *      - read that chunk's snippet from memory (which is actually about to be updated)
 *      - put it into `pre_accounts`
 *      - store the new transactions
 *      - ring two-phase write
 *      - apply transactions in memory => final snippet
 *
 * On recovery:
 *   - read ring from head..tail in chronological order
 *   - for each valid entry:
 *       if we haven't allocated chunk snippet in memory, init it to baseline
 *       copy ring entry's pre_accounts => memory
 *       reapply ring entry's transactions => final snippet
 */
#pragma pack(push,1)
typedef struct {
    uint8_t   valid_marker;  
    uint32_t  chunk_id;
    uint32_t  tx_count;
    Transaction transactions[BATCH_SIZE];
    Account     pre_accounts[CHUNK_SIZE_ACCOUNTS];
} RingEntry;
#pragma pack(pop)

/**
 * We'll store ring metadata (64 bytes) at offset 0:
 *   signature[8]
 *   version
 *   ring_capacity
 *   ring_head
 *   ring_tail
 *   ...
 */
#pragma pack(push,1)
typedef struct {
    char     signature[8];   // e.g. "CHNKRING"
    uint32_t version;
    uint32_t ring_capacity;  
    uint64_t ring_head;
    uint64_t ring_tail;
    uint8_t  reserved[32];  
} RingMetadata;
#pragma pack(pop)

// -------------- Globals --------------

static Account *g_state = NULL;  // entire in-memory array
static bool     g_allocated[NUM_CHUNKS]; // track if chunk is allocated on recovery
static int      g_ring_fd   = -1;
static RingMetadata g_ring_meta;

static uint64_t g_num_writes = 0; // how many ring writes
static uint64_t g_processed_batches = 0; // how many batches processed so far

// -------------- Time / Stats --------------

static double timespec_diff_ms(const struct timespec *start, const struct timespec *end)
{
    double s  = (double)(end->tv_sec - start->tv_sec);
    double ns = (double)(end->tv_nsec - start->tv_nsec)/1e9;
    return (s + ns)*1000.0;
}

static int cmp_doubles(const void *a, const void *b)
{
    double da= *(const double*)a;
    double db= *(const double*)b;
    return (da>db) - (da<db);
}

// -------------- ring metadata I/O --------------

static void ring_read_metadata(int fd, RingMetadata *meta)
{
    if (lseek(fd,0,SEEK_SET)==(off_t)-1){
        perror("lseek ring_read_metadata");
        exit(1);
    }
    ssize_t rc= read(fd, meta,sizeof(*meta));
    if (rc<0){
        perror("read ring metadata");
        exit(1);
    }
    // partial => brand new
    if (rc<(ssize_t)sizeof(*meta)){
        memset(meta,0,sizeof(*meta));
        memcpy(meta->signature,"CHNKOLD",7);
        meta->version=1;
        meta->ring_capacity= RING_CAPACITY;
        meta->ring_head=0;
        meta->ring_tail=0;
    }
}

static void ring_write_metadata(int fd, const RingMetadata *meta)
{
    if (lseek(fd,0,SEEK_SET)==(off_t)-1){
        perror("lseek ring_write_metadata");
        exit(1);
    }
    ssize_t rc= write(fd, meta,sizeof(*meta));
    if (rc<(ssize_t)sizeof(*meta)){
        perror("write ring metadata");
        exit(1);
    }
    fsync(fd);
}

static inline off_t ring_slot_offset(uint64_t slot_index)
{
    return 64 + (off_t)(slot_index % g_ring_meta.ring_capacity)*(off_t)sizeof(RingEntry);
}

// -------------- ring_write_entry (two-phase) --------------

static void ring_write_entry(RingEntry *entry)
{
    uint64_t head= g_ring_meta.ring_head;
    uint64_t tail= g_ring_meta.ring_tail;
    uint64_t cap = g_ring_meta.ring_capacity;
    uint64_t next_tail= (tail+1)%cap;
    if (next_tail== head){
        // ring full => discard oldest
        head= (head+1)%cap;
        g_ring_meta.ring_head= head;
    }
    off_t offset= ring_slot_offset(tail);

    // phase1: valid_marker=0
    entry->valid_marker= 0;
    if (lseek(g_ring_fd, offset, SEEK_SET)==(off_t)-1){
        perror("lseek ring_write p1");
        exit(1);
    }
    size_t esize= sizeof(RingEntry);
    if (write(g_ring_fd,entry,esize)!=(ssize_t)esize){
        perror("write ring p1");
        exit(1);
    }

    // phase2
    entry->valid_marker=1;
    if (lseek(g_ring_fd, offset, SEEK_SET)==(off_t)-1){
        perror("lseek ring_write p2");
        exit(1);
    }
    if (write(g_ring_fd,entry,esize)!=(ssize_t)esize){
        perror("write ring p2");
        exit(1);
    }

    // move tail
    g_ring_meta.ring_tail= next_tail;
    ring_write_metadata(g_ring_fd, &g_ring_meta);

    printf("Ring: wrote chunk_id=%u at slot=%llu\n",
           entry->chunk_id, (unsigned long long)tail);
}

// -------------- ring recover: parse from head..tail in chronological order --------------

static void ring_recover(void)
{
    uint64_t head= g_ring_meta.ring_head;
    uint64_t tail= g_ring_meta.ring_tail;
    if (head==tail){
        printf("Ring empty, nothing to replay.\n");
        return;
    }
    uint64_t cap= g_ring_meta.ring_capacity;

    // track we haven't allocated any chunk snippet yet
    for (uint32_t c=0; c<NUM_CHUNKS; c++){
        g_allocated[c]= false;
    }

    uint64_t idx= head;
    while (idx!= tail){
        off_t offset= ring_slot_offset(idx);
        if (lseek(g_ring_fd, offset, SEEK_SET)==(off_t)-1){
            perror("lseek ring slot");
            break;
        }
        RingEntry entry;
        ssize_t rc= read(g_ring_fd, &entry, sizeof(entry));
        if (rc<(ssize_t)sizeof(entry)){
            // partial => done
            break;
        }
        if (entry.valid_marker==1){
            // we reapply to memory
            uint32_t cid= entry.chunk_id;
            if (cid< NUM_CHUNKS){
                // if first time we see chunk => init snippet
                if (!g_allocated[cid]){
                    uint64_t sidx= (uint64_t)cid*CHUNK_SIZE_ACCOUNTS;
                    uint64_t eidx= sidx+CHUNK_SIZE_ACCOUNTS;
                    if (eidx>MAX_ACCOUNTS) eidx=MAX_ACCOUNTS;
                    // set them to initial or zero
                    for (uint64_t i=sidx; i<eidx; i++){
                        g_state[i].address= i;
                        g_state[i].balance= INITIAL_BALANCE;
                    }
                    g_allocated[cid]= true;
                }
                // Overwrite snippet with the ring entry's old snippet
                {
                    uint64_t start_idx= (uint64_t)cid*CHUNK_SIZE_ACCOUNTS;
                    uint64_t end_idx= start_idx+CHUNK_SIZE_ACCOUNTS;
                    if (end_idx>MAX_ACCOUNTS) end_idx= MAX_ACCOUNTS;
                    uint64_t count= end_idx-start_idx;
                    memcpy(&g_state[start_idx], entry.pre_accounts, count*sizeof(Account));
                }
                // Then reapply transactions => final snippet
                size_t tcount= entry.tx_count;
                if (tcount> BATCH_SIZE) tcount= BATCH_SIZE;
                for (size_t i=0; i< tcount; i++){
                    Transaction *tx= &entry.transactions[i];
                    if (g_state[tx->sender].balance >= tx->amount){
                        g_state[tx->sender].balance -= tx->amount;
                        g_state[tx->receiver].balance+= tx->amount;
                    }
                }
            }
        }
        idx= (idx+1)%cap;
    }
    printf("Ring replay complete. In-memory chunks are up-to-date.\n");
}

// -------------- ring open + recover --------------

static void ring_open_and_recover(void)
{
    g_ring_fd = open(RING_FILE, O_RDWR|O_CREAT, 0644);
    if (g_ring_fd < 0) {
        perror("open ring file");
        exit(1);
    }
    off_t endpos= lseek(g_ring_fd, 0, SEEK_END);
    if (endpos<0){
        perror("lseek ring end");
        exit(1);
    }

    RingMetadata meta;
    ring_read_metadata(g_ring_fd, &meta);
    if (memcmp(meta.signature,"CHNKOLD",7)!=0){
        // brand new
        memcpy(meta.signature,"CHNKOLD",7);
        meta.version=1;
        meta.ring_capacity= RING_CAPACITY;
        meta.ring_head=0;
        meta.ring_tail=0;
        ring_write_metadata(g_ring_fd, &meta);

        // no data => set all accounts to initial
        for (uint64_t i=0; i<MAX_ACCOUNTS; i++){
            g_state[i].address= i;
            g_state[i].balance= INITIAL_BALANCE;
        }
        printf("Created new ring file. All accounts set to initial.\n");
    } else {
        printf("Opened ring file. head=%llu, tail=%llu, capacity=%u\n",
               (unsigned long long)meta.ring_head,
               (unsigned long long)meta.ring_tail,
               meta.ring_capacity);
        g_ring_meta= meta;
        // replay
        ring_recover();
    }
    g_ring_meta= meta;
}

// -------------- incremental_persist --------------

/**
 * We'll do:
 *   1) chunk_id = g_num_writes % NUM_CHUNKS
 *   2) copy chunk's current in-memory snippet into ring entry's pre_accounts[] (old snippet)
 *   3) copy new transactions
 *   4) ring two-phase write
 *   5) apply transactions in memory => final snippet
 */
static void incremental_persist(Transaction *batch, size_t tx_count)
{
    uint32_t cid= (uint32_t)(g_num_writes % NUM_CHUNKS);
    g_num_writes++;

    // build ring entry
    static RingEntry ringbuf;
    memset(&ringbuf,0,sizeof(ringbuf));
    ringbuf.chunk_id= cid;
    ringbuf.tx_count= (uint32_t)tx_count;

    // copy transactions
    if (tx_count> BATCH_SIZE) {
        tx_count= BATCH_SIZE; 
    }
    memcpy(ringbuf.transactions, batch, tx_count*sizeof(Transaction));

    // copy the old snippet from memory
    uint64_t start_idx= (uint64_t)cid*CHUNK_SIZE_ACCOUNTS;
    uint64_t end_idx  = start_idx+CHUNK_SIZE_ACCOUNTS;
    if (end_idx>MAX_ACCOUNTS) end_idx=MAX_ACCOUNTS;
    uint64_t count= end_idx-start_idx;
    memcpy(ringbuf.pre_accounts, &g_state[start_idx], count*sizeof(Account));

    // two-phase ring write
    ring_write_entry(&ringbuf);

    // now in memory, we apply these transactions => final snippet
    for (size_t i=0; i<tx_count; i++){
        Transaction *tx= &batch[i];
        if (g_state[tx->sender].balance>= tx->amount){
            g_state[tx->sender].balance  -= tx->amount;
            g_state[tx->receiver].balance+= tx->amount;
        }
    }
}

// -------------- main --------------

int main(void)
{
    // allocate in-memory entire state
    g_state= (Account*)malloc(MAX_ACCOUNTS*sizeof(Account));
    if (!g_state){
        perror("malloc g_state");
        exit(1);
    }

    // open ring + recover
    ring_open_and_recover();

    // open transaction file
    FILE *txf= fopen(TX_FILE,"rb");
    if (!txf){
        perror("open TX_FILE");
        free(g_state);
        exit(1);
    }

    // track times
    double *batch_times= (double*)malloc(TOTAL_BATCHES*sizeof(double));
    if (!batch_times){
        perror("malloc batch_times");
        fclose(txf);
        free(g_state);
        exit(1);
    }
    Transaction *batch_buf= (Transaction*)malloc(BATCH_SIZE*sizeof(Transaction));
    if (!batch_buf){
        perror("malloc batch_buf");
        free(batch_times);
        fclose(txf);
        free(g_state);
        exit(1);
    }

    double total_ms= 0.0;

    // process 50 batches
    for (uint64_t iteration=0; iteration<TOTAL_BATCHES; iteration++){
        struct timespec st,en;
        clock_gettime(CLOCK_MONOTONIC, &st);

        // read BATCH_SIZE
        size_t read_count= fread(batch_buf, sizeof(Transaction), BATCH_SIZE, txf);
        if (read_count< BATCH_SIZE){
            if (feof(txf)){
                rewind(txf);
                read_count= fread(batch_buf, sizeof(Transaction), BATCH_SIZE, txf);
            } else {
                perror("fread TX_FILE");
                free(batch_buf); free(batch_times);
                fclose(txf);
                free(g_state);
                close(g_ring_fd);
                exit(1);
            }
        }

        // apply in memory => we do that after the ring entry is built 
        // (which includes copying the old snippet)
        // So incremental_persist will do it.

        incremental_persist(batch_buf, read_count);

        clock_gettime(CLOCK_MONOTONIC, &en);
        double ms= timespec_diff_ms(&st, &en);
        batch_times[iteration]= ms;
        total_ms+= ms;
        g_processed_batches++;

        printf("Batch %llu / %llu processed in %.3f ms\n",
               (unsigned long long)(iteration+1),
               (unsigned long long)TOTAL_BATCHES,
               ms);
    }

    // stats
    double avg_ms= total_ms/(double)TOTAL_BATCHES;
    printf("\nProcessed %d batches total.\n", (int)TOTAL_BATCHES);
    printf("Total time:  %.3f ms\n", total_ms);
    printf("Avg batch:   %.3f ms\n", avg_ms);

    // sort for median/p90/p99
    qsort(batch_times, TOTAL_BATCHES,sizeof(double), cmp_doubles);
    double median_ms;
    if (TOTAL_BATCHES%2==0){
        int mid= (int)(TOTAL_BATCHES/2);
        median_ms= (batch_times[mid-1]+batch_times[mid])/2.0;
    } else {
        median_ms= batch_times[TOTAL_BATCHES/2];
    }
    int idx_90= (int)ceil(0.90*(double)TOTAL_BATCHES)-1;
    if (idx_90<0) idx_90=0;
    if (idx_90>=(int)TOTAL_BATCHES) idx_90=(int)TOTAL_BATCHES-1;
    double p90_ms= batch_times[idx_90];

    int idx_99= (int)ceil(0.99*(double)TOTAL_BATCHES)-1;
    if (idx_99<0) idx_99=0;
    if (idx_99>=(int)TOTAL_BATCHES) idx_99=(int)TOTAL_BATCHES-1;
    double p99_ms= batch_times[idx_99];

    printf("\nLatency stats:\n");
    printf("  Median:  %.3f ms\n", median_ms);
    printf("  p90:     %.3f ms\n", p90_ms);
    printf("  p99:     %.3f ms\n", p99_ms);

    // cleanup
    free(batch_buf);
    free(batch_times);
    fclose(txf);
    close(g_ring_fd);
    free(g_state);

    return 0;
}