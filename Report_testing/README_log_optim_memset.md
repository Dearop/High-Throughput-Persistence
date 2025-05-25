# Enhanced Log-Optimized State Management with Memset Support

This is an enhanced version of the log_optim state management system that adds support for memset operations (range-set operations) like those found in the remember_glaze system, while maintaining the simpler architecture and performance characteristics of log_optim.

## Key Features

- **Dual Operation Support**: 
  - Balance transfers (func=0): Traditional account-to-account transfers
  - Range-set operations (func=1): Set multiple consecutive accounts to the same value
- **Log-Optimized Architecture**: Simple append-only logging with periodic snapshots
- **Asynchronous Snapshots**: Background thread handles snapshot creation
- **Enhanced Recovery**: Improved log replay with batch processing and performance metrics
- **Comprehensive Statistics**: Detailed timing and success rate reporting

## Architecture Overview

This system combines the best of both worlds:
- **From log_optim**: Simple, efficient logging architecture with asynchronous snapshots
- **From remember_glaze**: Operation encoding system supporting multiple transaction types

### Operation Encoding

Transactions use a packed encoding in the sender/receiver fields:
- **Upper 4 bits**: Function code (0 = transfer, 1 = range-set)
- **Lower 60 bits**: Data (account ID for transfers, start/length for range-set)

### Transaction Types

1. **Balance Transfer (func=0)**:
   - `sender`: `0x0` + sender_account_id
   - `receiver`: `0x0` + receiver_account_id
   - `amount`: transfer amount
   - Transfers `amount` from sender to receiver if sufficient balance

2. **Range-Set (func=1)**:
   - `sender`: `0x1` + start_account_id
   - `receiver`: `0x1` + length
   - `amount`: value to set
   - Sets accounts [start, start+length) to `amount`

## Compilation

Use the provided Makefile:

```bash
make -f Makefile_log_optim_memset
```

Or compile manually:

```bash
gcc -Wall -Wextra -O3 -std=c99 -pthread -D_POSIX_C_SOURCE=200809L \
    -o log_optim_with_memset log_optim_with_memset.c -pthread -lm
```

## Usage

```bash
./log_optim_with_memset
```

The program expects a `transactions.bin` file in the current directory containing encoded transactions.

## Configuration

Key parameters can be modified in the source code:

```c
#define BATCH_SIZE        (1 << 16)  // 65,536 transactions per batch
#define MAX_ACCOUNTS      500000000UL // 500 million accounts
#define TOTAL_BATCHES     5000        // Process 5000 batches total
#define MAX_LOG_BATCHES   100         // Snapshot every 100 batches
#define INITIAL_BALANCE   1000000UL   // Starting balance per account
```

## File Structure

- **Input**: `transactions.bin` - Binary transaction file
- **Output**: 
  - `state_log.bin` - Append-only transaction log
  - `state_snapshot.bin` - Periodic full state snapshots

## Performance Features

### Optimizations
- **Branch Prediction**: Uses `LIKELY`/`UNLIKELY` hints for hot paths
- **Memory Prefetching**: Prefetches account data for transfers
- **Batch Processing**: Processes transactions in 64K batches
- **Asynchronous I/O**: Background snapshot creation doesn't block processing

### Monitoring
- **Real-time Progress**: Per-batch timing and success rates
- **Comprehensive Statistics**: Median, p90, p99 latencies
- **Throughput Metrics**: Transactions per second
- **Recovery Metrics**: Log replay performance

## Recovery Process

1. **Snapshot Loading**: Loads most recent snapshot if available
2. **Log Replay**: Replays all transactions from log with enhanced processing
3. **Performance Reporting**: Reports recovery time and throughput

## Example Output

```
=== Enhanced Log-Optimized State Management with Memset Support ===
Configuration:
  MAX_ACCOUNTS: 500000000
  BATCH_SIZE: 65536
  TOTAL_BATCHES: 5000
  MAX_LOG_BATCHES: 100
  Supports: Balance transfers (func=0) and Range-set operations (func=1)

No snapshot found; initialized fresh state.
No log file found; nothing to replay.
Recovery phase completed in 2847.123 ms (replay: 0.000 ms)

Starting transaction processing...
Batch 1 of 5000 processed in 45.234 ms (commit: 12.456 ms, success: 65536, failed: 0)
Batch 10 of 5000 processed in 43.567 ms (commit: 11.234 ms, success: 65536, failed: 0)
...
Snapshot triggered in 234.567 ms (state copy + thread spawn)
[Async] Snapshot created with full state in 1234.567 ms.
[Async] Log has been reset (old log removed).
...

=== Processing Summary ===
Processed 5000 batches total.
Total transactions: 327680000 (successful: 327680000, failed: 0)
Success rate: 100.00%
Total time:    234567.890 ms
Avg per batch: 46.913 ms
Avg commit:    12.345 ms
Throughput:    1396234 tx/sec

Latency statistics for 5000 batches:
  Median:    45.123 ms
  p90:       52.456 ms
  p99:       67.890 ms

Commit time statistics for 5000 batches:
  Median:    11.234 ms
  p90:       14.567 ms
  p99:       18.901 ms
```

## Differences from Original log_optim

1. **Enhanced Transaction Processing**: Supports both transfers and range-set operations
2. **Improved Error Handling**: Better bounds checking and validation
3. **Enhanced Recovery**: Batch-based log replay with performance metrics
4. **Extended Statistics**: Success/failure rates and operation type breakdown
5. **Signed Balances**: Uses `int64_t` for balances to support negative values

## Differences from remember_glaze

1. **Simpler Architecture**: No complex chunking or ring buffers
2. **Asynchronous Snapshots**: Background snapshot creation vs. synchronous
3. **Append-Only Logging**: Simple log structure vs. complex checkpoint system
4. **Single-Threaded Processing**: Main processing is single-threaded (except snapshots)
5. **Memory Efficiency**: Direct state array vs. chunked state management

## Use Cases

This enhanced version is ideal for:
- **Mixed Workloads**: Applications needing both transfers and bulk operations
- **High Throughput**: Simple architecture optimized for speed
- **Quick Recovery**: Fast startup with efficient log replay
- **Development/Testing**: Simpler codebase for experimentation

## Requirements

- POSIX-compliant system (Linux, macOS)
- GCC or Clang compiler with C99 support
- pthread library
- Sufficient RAM for account state (500M accounts × 16 bytes ≈ 8GB)
- Disk space for logs and snapshots

## Notes

- The system automatically handles log rotation via snapshots
- Range-set operations can efficiently initialize or reset large account ranges
- All I/O operations include proper fsync for durability
- The system gracefully handles transaction file wraparound for continuous operation 