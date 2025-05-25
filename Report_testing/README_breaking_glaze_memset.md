# Breaking Glaze with Memset Support (Ring-based Recovery)

This is an enhanced version of the breaking_glaze state management system that adds support for memset operations (range-set operations) from the Report_testing transaction format, while maintaining the original ring-based recovery mechanism and checkpoint architecture.

## Key Features

- **Dual Operation Support**: 
  - Balance transfers (func=0): Traditional account-to-account transfers
  - Range-set operations (func=1): Set multiple consecutive accounts to the same value
- **Ring-based Recovery**: Circular log with 8 checkpoint slots for efficient recovery
- **State Chunking**: Processes only the first 64K accounts (512KB state chunk)
- **Encoded Transactions**: Compatible with Report_testing transaction format
- **Hash Verification**: State integrity checking with FNV-1a hash
- **Comprehensive Statistics**: Detailed timing and success rate reporting

## Architecture Overview

This system combines:
- **From breaking_glaze**: Ring-based checkpoint logging with fixed-size slots
- **From Report_testing**: Operation encoding system supporting multiple transaction types

### Ring-based Checkpoint System

The system uses a circular log with 8 slots, each containing:
1. **Checkpoint Header**: Batch number and metadata
2. **State Chunk**: 512KB snapshot of account balances (64K accounts)
3. **Write Set**: Full batch of transactions that created this state

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
make -f Makefile_breaking_glaze_memset
```

Or compile manually:

```bash
gcc -Wall -Wextra -O3 -std=c99 -D_POSIX_C_SOURCE=200809L \
    -o breaking_glaze_with_memset breaking_glaze_with_memset.c -lm
```

## Usage

### Normal Operation
```bash
./breaking_glaze_with_memset
```

### Recovery Mode Only
```bash
./breaking_glaze_with_memset recover
```

The program expects a `transactions.bin` file in the current directory containing encoded transactions.

## Configuration

Key parameters can be modified in the source code:

```c
#define BATCH_SIZE          (1 << 16)    // 65,536 transactions per batch
#define NUMBER_OF_BATCHES   5000         // Process 5000 batches total
#define MAX_ACCOUNTS        500000000UL  // 500 million total accounts
#define STATE_CHUNK_COUNT   65536        // Process first 64K accounts only
#define RING_SIZE           8            // 8 checkpoint slots in ring
#define INITIAL_BALANCE     1000000UL    // Starting balance per account
```

## File Structure

- **Input**: `transactions.bin` - Binary transaction file with encoded operations
- **Output**: 
  - `checkpoint_log.dat` - Ring-based checkpoint log
  - `state_hash.dat` - State integrity hash
  - `reconstructed_state.txt` - Debug output for hash mismatches

## Recovery Process

1. **Ring Scan**: Scans all 8 checkpoint slots to find the latest valid checkpoint
2. **State Restoration**: Loads the most recent state chunk
3. **Hash Verification**: Compares computed hash with stored hash
4. **Integrity Check**: Reports any hash mismatches for debugging

## Performance Features

### Optimizations
- **Branch Prediction**: Uses `LIKELY`/`UNLIKELY` hints for hot paths
- **Memory Prefetching**: Prefetches account data for transfers
- **Batch Processing**: Processes transactions in 64K batches
- **Ring Buffer**: Efficient circular logging without file growth

### Monitoring
- **Real-time Progress**: Per-batch timing and success rates
- **Comprehensive Statistics**: Median, p90, p99 latencies
- **Throughput Metrics**: Transactions per second
- **Success Rate Tracking**: Successful vs. failed transaction counts

## Example Output

```
=== Breaking Glaze with Memset Support (Ring-based Recovery) ===
Configuration:
  MAX_ACCOUNTS: 500000000
  STATE_CHUNK_COUNT: 65536 (processing first 65536 accounts)
  BATCH_SIZE: 65536
  NUMBER_OF_BATCHES: 5000
  RING_SIZE: 8
  Supports: Balance transfers (func=0) and Range-set operations (func=1)

No valid checkpoint found in log.
Starting transaction processing...
Batch 0 processed in 45.234 ms (success: 62341, failed: 3195)
Batch 10 processed in 43.567 ms (success: 61892, failed: 3644)
...

=== Processing Summary ===
Processed 5000 batches total.
Total transactions: 327680000 (successful: 310234567, failed: 17445433)
Success rate: 94.67%
Total processing time: 234567.890 ms
Average batch time: 46.913 ms
Median batch time: 45.123 ms
90th percentile batch time: 52.456 ms
99th percentile batch time: 67.890 ms
Throughput: 1396234 tx/sec
Final state chunk hash: 12345678901234567890
Total time taken: 234567 ms
```

## Key Differences from Original breaking_glaze

1. **Enhanced Transaction Processing**: Supports both transfers and range-set operations
2. **Updated Transaction Format**: Uses 64-bit amounts and encoded operations
3. **Improved Error Handling**: Better bounds checking and validation
4. **Extended Statistics**: Success/failure rates and operation type support
5. **State Chunk Focus**: Only processes first 64K accounts for efficiency

## Key Differences from Report_testing Programs

1. **Ring-based Logging**: Circular checkpoint log vs. append-only logging
2. **State Chunking**: Only processes 64K accounts vs. full 500M accounts
3. **Synchronous Checkpoints**: Every batch creates a checkpoint vs. periodic snapshots
4. **Fixed Memory Usage**: Ring buffer prevents unbounded log growth
5. **Hash Verification**: Built-in state integrity checking

## Limitations

- **Account Range**: Only processes the first 64K accounts (STATE_CHUNK_COUNT)
- **Memory Usage**: Fixed 512KB state chunk size
- **Transaction Scope**: Transactions affecting accounts beyond 64K are rejected
- **Ring Overflow**: Only keeps the last 8 checkpoints (older ones are overwritten)

## Use Cases

This enhanced version is ideal for:
- **Mixed Workloads**: Applications needing both transfers and bulk operations
- **Memory-Constrained Environments**: Fixed memory footprint with state chunking
- **Fast Recovery**: Ring-based checkpoints enable quick restart
- **Development/Testing**: Simpler recovery mechanism for experimentation
- **Bounded Storage**: Ring buffer prevents log file growth

## Requirements

- POSIX-compliant system (Linux, macOS)
- GCC or Clang compiler with C99 support
- Sufficient disk space for ring log (approximately 8 × 4MB = 32MB)
- Compatible transaction file generated by Report_testing transaction generator

## Notes

- The system automatically handles ring buffer wraparound
- Range-set operations can efficiently initialize or reset account ranges within the chunk
- All I/O operations include proper fsync for durability
- The system gracefully handles transaction file wraparound for continuous operation
- Hash verification helps detect corruption or inconsistencies in the checkpoint log 