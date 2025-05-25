# Parameterized Log-Optimized State Management with Memset Support

This is a parameterized version of the log_optim_with_memset state management system that accepts the number of accounts as a command-line argument, enabling flexible testing with different account counts while maintaining the simple log-optimized architecture and memset operation support.

## Key Features

- **Configurable Account Count**: Accept any number of accounts as a command-line parameter
- **Dual Operation Support**: 
  - Balance transfers (func=0): Traditional account-to-account transfers
  - Range-set operations (func=1): Set multiple consecutive accounts to the same value
- **Log-Optimized Architecture**: Simple append-only logging with periodic snapshots
- **Asynchronous Snapshots**: Background thread handles snapshot creation
- **Dynamic Memory Allocation**: Allocates memory based on the specified account count
- **Enhanced Recovery**: Improved log replay with batch processing and performance metrics
- **Comprehensive Statistics**: Detailed timing and success rate reporting

## Architecture Overview

This system provides:
- **From log_optim**: Simple, efficient logging architecture with asynchronous snapshots
- **From Report_testing**: Operation encoding system supporting multiple transaction types
- **Parameterization**: Dynamic account count configuration for flexible testing

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
make -f Makefile_log_optim_parameterized
```

Or compile manually:

```bash
gcc -Wall -Wextra -O3 -std=c99 -pthread -D_POSIX_C_SOURCE=200809L \
    -o log_optim_parameterized log_optim_parameterized.c -pthread -lm
```

## Usage

### Command-Line Interface

```bash
./log_optim_parameterized <number_of_accounts>
```

### Examples

```bash
# Manage 5 million accounts
./log_optim_parameterized 5000000

# Manage 1 million accounts
./log_optim_parameterized 1000000

# Manage 100 million accounts
./log_optim_parameterized 100000000

# Show usage help
./log_optim_parameterized
```

### Input Validation

- Account count must be a positive integer
- Maximum limit: 1 billion accounts (to prevent excessive memory usage)
- Invalid inputs show usage information with examples

## Configuration

Key parameters are defined in the source code:

```c
#define BATCH_SIZE        (1 << 16)  // 65,536 transactions per batch
#define TOTAL_BATCHES     5000       // Process 5000 batches total
#define MAX_LOG_BATCHES   100        // Snapshot every 100 batches
#define INITIAL_BALANCE   1000000UL  // Starting balance per account
```

The account count is dynamically set from the command-line argument.

## File Structure

- **Input**: `transactions.bin` - Binary transaction file
- **Output**: 
  - `state_log.bin` - Append-only transaction log
  - `state_snapshot.bin` - Periodic full state snapshots

## Memory Usage

The program displays estimated memory usage at startup:

```
Memory usage: ~76.3 MB for account state
```

Memory usage scales linearly with account count:
- **Formula**: `account_count × 16 bytes` (Account struct size)
- **Examples**:
  - 1M accounts: ~15.3 MB
  - 5M accounts: ~76.3 MB
  - 100M accounts: ~1.5 GB

## Performance Features

### Optimizations
- **Branch Prediction**: Uses `LIKELY`/`UNLIKELY` hints for hot paths
- **Memory Prefetching**: Prefetches account data for transfers
- **Batch Processing**: Processes transactions in 64K batches
- **Asynchronous I/O**: Background snapshot creation doesn't block processing
- **Dynamic Allocation**: Only allocates memory for the specified account count

### Monitoring
- **Real-time Progress**: Per-batch timing and success rates
- **Comprehensive Statistics**: Median, p90, p99 latencies
- **Throughput Metrics**: Transactions per second
- **Recovery Metrics**: Log replay performance
- **Memory Reporting**: Shows actual memory usage

## Recovery Process

1. **Snapshot Loading**: Loads most recent snapshot if available (validates account count)
2. **Log Replay**: Replays all transactions from log with enhanced processing
3. **Performance Reporting**: Reports recovery time and throughput

## Example Output

```
=== Parameterized Log-Optimized State Management with Memset Support ===
Configuration:
  MAX_ACCOUNTS: 5000000
  BATCH_SIZE: 65536
  TOTAL_BATCHES: 5000
  MAX_LOG_BATCHES: 100
  Memory usage: ~76.3 MB for account state
  Supports: Balance transfers (func=0) and Range-set operations (func=1)

No snapshot found; initialized fresh state with 5000000 accounts.
No log file found; nothing to replay.
Recovery phase completed in 1234.567 ms (replay: 0.000 ms)

Starting transaction processing...
Batch 1 of 5000 processed in 45.234 ms (commit: 12.456 ms, success: 65536, failed: 0)
Batch 10 of 5000 processed in 43.567 ms (commit: 11.234 ms, success: 65536, failed: 0)
...
Snapshot triggered in 234.567 ms (state copy + thread spawn)
[Async] Snapshot created with 5000000 accounts in 1234.567 ms.
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

## Differences from Fixed log_optim_with_memset

1. **Parameterized Account Count**: Accepts account count as command-line argument
2. **Dynamic Memory Allocation**: Allocates memory based on specified account count
3. **Input Validation**: Comprehensive validation of account count parameter
4. **Memory Reporting**: Shows estimated memory usage at startup
5. **Flexible Testing**: Enables testing with different account counts without recompilation
6. **Usage Information**: Built-in help and examples

## Differences from breaking_glaze_with_memset

1. **Full Account Range**: Processes all specified accounts vs. 64K chunk limit
2. **Append-Only Logging**: Simple log structure vs. ring-based checkpoints
3. **Asynchronous Snapshots**: Background snapshot creation vs. synchronous checkpoints
4. **Unbounded Growth**: Log grows until snapshot vs. fixed ring buffer
5. **Parameterized Size**: Configurable account count vs. fixed chunk size

## Use Cases

This parameterized version is ideal for:
- **Performance Testing**: Compare performance across different account counts
- **Memory Scaling Studies**: Analyze memory usage patterns
- **Flexible Benchmarking**: Test with various workload sizes
- **Development**: Easy testing without recompilation
- **Research**: Study system behavior with different state sizes

## Requirements

- POSIX-compliant system (Linux, macOS)
- GCC or Clang compiler with C99 support
- pthread library
- Sufficient RAM for account state (varies by account count)
- Disk space for logs and snapshots
- Compatible transaction file generated by Report_testing transaction generator

## Notes

- The system automatically handles log rotation via snapshots
- Range-set operations can efficiently initialize or reset large account ranges
- All I/O operations include proper fsync for durability
- The system gracefully handles transaction file wraparound for continuous operation
- Memory usage is displayed at startup to help with capacity planning
- Snapshot files are validated against the specified account count during recovery 