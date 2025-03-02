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

// We'll run 50 total batches
#define TOTAL_BATCHES    50

// Each batch can have up to 65536 transactions
#define BATCH_SIZE       (1 << 16)

// 2 million accounts
#define MAX_ACCOUNTS     2000000UL

// The initial balance
#define INITIAL_BALANCE  1000000UL

// We'll store the log in a single file each run
#define LOG_FILE         "append_delete_log.bin"
#define TX_FILE          "transactions.bin"

// A single transaction
typedef struct {
    uint64_t sender;
    uint64_t receiver;
    uint32_t amount;
} Transaction;

// Each account
typedef struct {
    uint64_t address;
    uint64_t balance;
} Account;

// We'll store results from each run
typedef struct {
    double total_time_ms;
    double avg_batch_ms;
    double median_ms;
    double p90_ms;
    double p99_ms;
} RunResults;

// Utility for time difference in ms
static double timespec_diff_ms(const struct timespec *start, const struct timespec *end)
{
    double s = (double)(end->tv_sec - start->tv_sec);
    double ns = (double)(end->tv_nsec - start->tv_nsec)/1e9;
    return (s + ns) * 1000.0;
}

// Compare doubles (for qsort)
static int cmp_doubles(const void *a, const void *b)
{
    double da = *(const double*)a;
    double db = *(const double*)b;
    return (da > db) - (da < db);
}

/**
 * We'll do the entire append+delete approach for a single run,
 * but we parametrize by chunk_size_accounts.
 *
 * Then we measure how long it takes for 50 batches, and produce latencies, etc.
 */

// We'll keep references to dynamic arrays in a struct so we can pass them to subfunctions
typedef struct {
    Account *g_state;
    off_t   *old_offsets;
    int      fd;
    size_t   chunk_size_accounts; // how many accounts in each chunk
    size_t   num_chunks;
} RunContext;

// A small helper to zero out an old chunk
static void invalidate_old_chunk(RunContext *ctx, off_t old_off, size_t chunk_struct_size)
{
    if (old_off < 0) return;
    if (lseek(ctx->fd, old_off, SEEK_SET) == (off_t)-1) {
        perror("lseek old_off");
        return;
    }
    static char zeros[8192];
    size_t remain = chunk_struct_size;
    while (remain > 0) {
        size_t wr = (remain > sizeof(zeros))? sizeof(zeros): remain;
        ssize_t rc = write(ctx->fd, zeros, wr);
        if (rc < 0 || (size_t)rc != wr) {
            perror("write old chunk zeros");
            break;
        }
        remain -= wr;
    }
}

// We'll define the safe_write_chunk as a normal function
static void safe_write_chunk(RunContext *ctx, uint32_t chunk_id,
                             Transaction *batch, size_t tx_count)
{
    // Build a big buffer: [valid_marker(1) + chunk_id(4) + tx_count(4)
    //                     + BATCH_SIZE*Transaction + chunk_size_accounts*Account]
    size_t chunk_struct_size =
          1  // valid_marker
        + 4  // chunk_id
        + 4  // tx_count
        + (BATCH_SIZE * sizeof(Transaction))
        + (ctx->chunk_size_accounts * sizeof(Account));

    uint8_t *buf = (uint8_t*)malloc(chunk_struct_size);
    if (!buf) {
        perror("malloc chunk buf");
        exit(1);
    }
    memset(buf, 0, chunk_struct_size);

    // layout:
    // offset 0: valid_marker
    // offset 1: chunk_id (4 bytes)
    // offset 5: tx_count (4 bytes)
    // offset 9: BATCH_SIZE transactions
    // then the accounts

    // 1) Build partial data
    buf[0] = 0; // partial
    *(uint32_t*)(buf+1) = chunk_id;
    *(uint32_t*)(buf+5) = (uint32_t)tx_count;

    // copy transactions
    size_t tx_area_offset = 9;
    size_t tx_bytes = BATCH_SIZE*sizeof(Transaction);
    // user data is tx_count, but we store full BATCH_SIZE region or partial?
    // We'll store full region for simplicity. Just partial is also possible.
    memcpy(buf + tx_area_offset, batch, tx_count*sizeof(Transaction));

    // copy final accounts
    size_t accounts_offset = tx_area_offset + tx_bytes;
    uint64_t start_idx = (uint64_t)chunk_id * ctx->chunk_size_accounts;
    uint64_t end_idx   = start_idx + ctx->chunk_size_accounts;
    if (end_idx > MAX_ACCOUNTS) end_idx = MAX_ACCOUNTS;
    uint64_t count = end_idx - start_idx;
    memcpy(buf + accounts_offset, &ctx->g_state[start_idx], count*sizeof(Account));

    off_t old_off = ctx->old_offsets[chunk_id];

    // find end
    off_t new_off = lseek(ctx->fd, 0, SEEK_END);
    if (new_off==(off_t)-1){
        perror("lseek new_off");
        exit(1);
    }

    // Phase1: write partial
    if (write(ctx->fd, buf, chunk_struct_size)!=(ssize_t)chunk_struct_size){
        perror("write partial chunk");
        exit(1);
    }
    if (fsync(ctx->fd)!=0){
        perror("fsync partial chunk");
        exit(1);
    }

    // Phase2: set valid_marker=1
    buf[0] = 1;
    if (lseek(ctx->fd, new_off, SEEK_SET)==(off_t)-1){
        perror("lseek phase2");
        exit(1);
    }
    if (write(ctx->fd, buf, chunk_struct_size)!=(ssize_t)chunk_struct_size){
        perror("write finalize chunk");
        exit(1);
    }
    if (fsync(ctx->fd)!=0){
        perror("fsync finalize chunk");
        exit(1);
    }

    // Invalidate old
    if (old_off>=0){
        invalidate_old_chunk(ctx, old_off, chunk_struct_size);
    }
    ctx->old_offsets[chunk_id] = new_off;

    free(buf);
}

// We'll define incremental_persist
static uint64_t g_num_writes = 0;

static void incremental_persist(RunContext *ctx, Transaction *batch, size_t tx_count)
{
    uint32_t cid = (uint32_t)(g_num_writes % ctx->num_chunks);
    g_num_writes++;
    safe_write_chunk(ctx, cid, batch, tx_count);
}

static RunResults run_test_for_chunk_size(size_t chunk_size_accounts)
{
    // define how many chunks
    size_t num_chunks = (MAX_ACCOUNTS + chunk_size_accounts -1)/chunk_size_accounts;

    // allocate old_offsets
    off_t *old_offsets = (off_t*)malloc(num_chunks*sizeof(off_t));
    if (!old_offsets) {
        perror("malloc old_offsets");
        exit(1);
    }
    for (size_t i=0; i<num_chunks; i++) {
        old_offsets[i] = -1;
    }

    // allocate in-memory state
    Account *g_state = (Account*)malloc(MAX_ACCOUNTS*sizeof(Account));
    if (!g_state) {
        perror("malloc g_state");
        exit(1);
    }
    memset(g_state, 0, MAX_ACCOUNTS*sizeof(Account));

    // open (or recreate) the log file for this run
    int fd = open(LOG_FILE, O_RDWR|O_CREAT|O_TRUNC, 0644);
    if (fd<0) {
        perror("open log");
        exit(1);
    }

    // init each chunk to initial balance
    for (size_t c=0; c<num_chunks; c++){
        uint64_t start_idx= (uint64_t)c * chunk_size_accounts;
        uint64_t end_idx= start_idx + chunk_size_accounts;
        if (end_idx>MAX_ACCOUNTS) end_idx=MAX_ACCOUNTS;
        for (uint64_t i=start_idx; i<end_idx; i++){
            g_state[i].address = i;
            g_state[i].balance = INITIAL_BALANCE;
        }
    }

    // open TX_FILE
    FILE *txf = fopen(TX_FILE,"rb");
    if (!txf){
        perror("fopen TX_FILE");
        exit(1);
    }

    double *batch_times = (double*)malloc(TOTAL_BATCHES*sizeof(double));
    if (!batch_times){
        perror("batch_times");
        exit(1);
    }
    Transaction *batch_buf = (Transaction*)malloc(BATCH_SIZE*sizeof(Transaction));
    if (!batch_buf){
        perror("batch_buf");
        exit(1);
    }

    // We'll define a context struct
    RunContext ctx;
    ctx.g_state           = g_state;
    ctx.old_offsets       = old_offsets;
    ctx.fd                = fd;
    ctx.chunk_size_accounts = chunk_size_accounts;
    ctx.num_chunks        = num_chunks;

    // reset global g_num_writes
    g_num_writes = 0;

    double total_elapsed=0.0;

    // run 50 batches
    for (int iteration=0; iteration<(int)TOTAL_BATCHES; iteration++){
        struct timespec st,en;
        clock_gettime(CLOCK_MONOTONIC,&st);

        // read batch
        size_t read_count= fread(batch_buf, sizeof(Transaction), BATCH_SIZE, txf);
        if (read_count< BATCH_SIZE){
            if (feof(txf)){
                rewind(txf);
                read_count= fread(batch_buf, sizeof(Transaction), BATCH_SIZE, txf);
            } else {
                perror("fread TX_FILE");
                exit(1);
            }
        }
        // apply
        for (size_t i=0; i<read_count; i++){
            Transaction *tx = &batch_buf[i];
            if (g_state[tx->sender].balance >= tx->amount){
                g_state[tx->sender].balance  -= tx->amount;
                g_state[tx->receiver].balance+= tx->amount;
            }
        }

        // persist
        incremental_persist(&ctx, batch_buf, read_count);

        clock_gettime(CLOCK_MONOTONIC,&en);
        double ms= timespec_diff_ms(&st,&en);
        batch_times[iteration]= ms;
        total_elapsed+= ms;
    }

    fclose(txf);
    close(fd);
    free(batch_buf);

    double avg_ms= total_elapsed/(double)TOTAL_BATCHES;
    // sort
    qsort(batch_times, TOTAL_BATCHES,sizeof(double), cmp_doubles);
    double median_ms;
    if ((TOTAL_BATCHES %2)==0){
        int mid= (int)(TOTAL_BATCHES/2);
        median_ms= (batch_times[mid-1]+batch_times[mid])/2.0;
    } else {
        median_ms= batch_times[TOTAL_BATCHES/2];
    }
    int idx_90= (int)ceil(0.90*(double)TOTAL_BATCHES)-1;
    if (idx_90<0) idx_90=0;
    if (idx_90>=(int)TOTAL_BATCHES) idx_90=(int)TOTAL_BATCHES-1;
    double p90= batch_times[idx_90];

    int idx_99= (int)ceil(0.99*(double)TOTAL_BATCHES)-1;
    if (idx_99<0) idx_99=0;
    if (idx_99>=(int)TOTAL_BATCHES) idx_99=(int)TOTAL_BATCHES-1;
    double p99= batch_times[idx_99];

    free(batch_times);
    free(old_offsets);
    free(g_state);

    RunResults rr;
    rr.total_time_ms= total_elapsed;
    rr.avg_batch_ms= avg_ms;
    rr.median_ms   = median_ms;
    rr.p90_ms      = p90;
    rr.p99_ms      = p99;
    return rr;
}

int main(void)
{
    // We'll define an array of chunk sizes to test
    size_t candidates[] = {8192, 16384, 32768, 65536};
    int N= sizeof(candidates)/sizeof(candidates[0]);

    // Print CSV header
    printf("chunk_size, total_time_ms, avg_batch_ms, median_ms, p90_ms, p99_ms\n");

    for (int i=0; i<N; i++){
        size_t cs= candidates[i];
        RunResults r= run_test_for_chunk_size(cs);
        printf("%zu, %.3f, %.5f, %.3f, %.3f, %.3f\n",
               cs, 
               r.total_time_ms, 
               r.avg_batch_ms, 
               r.median_ms, 
               r.p90_ms, 
               r.p99_ms);
        fflush(stdout);
    }

    return 0;
}