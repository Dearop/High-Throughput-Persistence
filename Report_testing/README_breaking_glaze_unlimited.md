# Breaking Glaze with Memset Support (Unlimited Accounts)

This is an enhanced version of the breaking_glaze state management system that **removes the artificial 64K account limitation** and supports processing any number of accounts specified via command-line parameter, while maintaining the ring-based checkpoint architecture and adding memset operation support.

## Key Features

- **Unlimited Account Processing**: No more 64K account limitation - processes ALL specified accounts
- **Parameterized Account Count**: Accept any number of accounts as a command-line parameter
- **Dual Operation Support**: 
  - Balance transfers (func=0): Traditional account-to-account transfers
  - Range-set operations (func=1): Set multiple consecutive accounts to the same value
- **Ring-Based Checkpointing**: Maintains the efficient ring buffer architecture with dynamic sizing
- **Dynamic Memory Allocation**: Allocates memory based on the specified account count
- **Enhanced Recovery**: Improved checkpoint validation and state reconstruction
- **Comprehensive Statistics**: Detailed timing and success rate reporting

## What Changed from the Original breaking_glaze

### ❌ **REMOVED: Artificial 64K Limitation**

The original breaking_glaze had this problematic constraint:
```c
#define STATE_CHUNK_SIZE  (512 * 1024)           // 512KB fixed chunk
#define STATE_CHUNK_COUNT (STATE_CHUNK_SIZE / sizeof(int64_t))  // = 65,536 accounts
```

This meant:
- Only the first 64K accounts were processed
- Transactions affecting accounts > 64K were marked as "failed"
- Poor scalability for realistic workloads
- Misleading performance comparisons

### ✅ **ADDED: Dynamic Account Management**

The unlimited version uses:
```c
// Global variables (now parameterized)
static uint64_t g_max_accounts = 0;           // Set from command line
static size_t g_state_size_bytes = 0;         // Calculated dynamically
static size_t g_checkpoint_slot_size = 0;     // Scales with account count
```

Benefits:
- **All accounts processed**: No artificial limitations
- **Realistic testing**: Can handle millions of accounts
- **Fair comparisons**: Success rates reflect actual system performance
- **Memory efficiency**: Only allocates what's needed

## Architecture Overview

This system combines:
- **From breaking_glaze**: Ring-based checkpoint architecture for fast recovery
- **From Report_testing**: Operation encoding system supporting multiple transaction types
- **New enhancement**: Dynamic sizing and unlimited account processing

### Ring-Based Checkpointing

The system maintains a ring buffer of 8 checkpoint slots, each containing:
1. **Checkpoint Header**: Metadata (batch number, account count, etc.)
2. **Full State Snapshot**: Complete account state (now dynamically sized)
3. **Transaction Batch**: The transactions that created this checkpoint

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
make -f Makefile_breaking_glaze_unlimited
```

Or compile manually:

```bash
gcc -Wall -Wextra -O3 -std=c99 -D_POSIX_C_SOURCE=200809L \
    -o breaking_glaze_with_memset_unlimited breaking_glaze_with_memset_unlimited.c -lm
```

## Usage

### Command-Line Interface

```bash
./breaking_glaze_with_memset_unlimited <number_of_accounts>
```

### Examples

```bash
# Manage 5 million accounts
./breaking_glaze_with_memset_unlimited 5000000

# Manage 1 million accounts
./breaking_glaze_with_memset_unlimited 1000000

# Manage 100 million accounts
./breaking_glaze_with_memset_unlimited 100000000

# Show usage help
./breaking_glaze_with_memset_unlimited
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
#define RING_SIZE         8          // Number of checkpoint slots
#define INITIAL_BALANCE   1000000L   // Starting balance per account
```

The account count is dynamically set from the command-line argument.

## File Structure

- **Input**: `transactions.bin` - Binary transaction file
- **Output**: 
  - `checkpoint_log.dat` - Ring-based checkpoint log (dynamically sized)
  - `reconstructed_state.txt` - Human-readable state dump

## Memory Usage

The program displays estimated memory usage at startup:

```
Memory usage: ~76.3 MB for account state
Checkpoint slot size: ~76.8 MB
Total log file size: ~614.7 MB
```

Memory usage scales linearly with account count:
- **Account State**: `account_count × 8 bytes`
- **Checkpoint Slot**: `header + state + transactions ≈ account_count × 8 + 1MB`
- **Total Log File**: `8 × checkpoint_slot_size`

**Examples**:
- 1M accounts: ~8MB state, ~72MB total log
- 5M accounts: ~40MB state, ~328MB total log  
- 100M accounts: ~800MB state, ~6.4GB total log

## Performance Features

### Optimizations
- **Branch Prediction**: Uses `LIKELY`/`UNLIKELY` hints for hot paths
- **Memory Prefetching**: Prefetches account data for transfers
- **Batch Processing**: Processes transactions in 64K batches
- **Ring Buffer**: Efficient checkpoint rotation without file growth
- **Dynamic Allocation**: Only allocates memory for the specified account count

### Monitoring
- **Real-time Progress**: Per-batch timing and success rates
- **Comprehensive Statistics**: Median, p90, p99 latencies
- **Throughput Metrics**: Transactions per second
- **Recovery Metrics**: Checkpoint reconstruction performance
- **Memory Reporting**: Shows actual memory usage and log file sizes

## Recovery Process

1. **Log File Opening**: Opens existing ring-based checkpoint log
2. **Checkpoint Scanning**: Scans all 8 ring slots for the latest valid checkpoint
3. **State Reconstruction**: Loads the most recent complete state
4. **Validation**: Verifies checkpoint integrity and account count compatibility

## Example Output

```
=== Breaking Glaze with Memset Support (Unlimited Accounts) ===
Configuration:
  MAX_ACCOUNTS: 5000000 (processing ALL accounts)
  BATCH_SIZE: 65536
  TOTAL_BATCHES: 5000
  RING_SIZE: 8
  Memory usage: ~38.1 MB for account state
  Checkpoint slot size: ~39.1 MB
  Total log file size: ~312.5 MB
  Supports: Balance transfers (func=0) and Range-set operations (func=1)

No valid checkpoint found in log.
No valid checkpoint found. Using fresh state in 1234.567 ms.

Starting transaction processing...
Batch 1 of 5000 processed in 45.234 ms (success: 65536, failed: 0)
Batch 10 of 5000 processed in 43.567 ms (success: 65536, failed: 0)
...

=== Processing Summary ===
Processed 5000 batches total.
Total transactions: 327680000 (successful: 327680000, failed: 0)
Success rate: 100.00%
Total time: 234567.890 ms
Average batch time: 46.913 ms
Throughput: 1396234 tx/sec

Latency statistics:
  Median batch time: 45.123 ms
  90th percentile batch time: 52.456 ms
  99th percentile batch time: 67.890 ms

Final state hash: 0x1234567890abcdef
```

## Differences from Limited breaking_glaze_with_memset

1. **Account Processing**: Processes ALL accounts vs. only first 64K
2. **Success Rates**: Much higher success rates (no artificial failures)
3. **Memory Scaling**: Dynamic memory allocation vs. fixed 512KB chunks
4. **Parameterization**: Command-line account count vs. compile-time constant
5. **Realistic Testing**: Enables testing with actual workload sizes

## Differences from state_management_parameterized

1. **Checkpoint Strategy**: Ring-based checkpoints vs. append-only logging
2. **Recovery Speed**: Fast checkpoint loading vs. log replay
3. **Disk Usage**: Fixed ring size vs. growing log files
4. **Memory Overhead**: Full state snapshots vs. incremental logging
5. **Durability Model**: Periodic checkpoints vs. continuous logging

## Use Cases

This unlimited version is ideal for:
- **Fair Performance Comparisons**: No artificial account limitations
- **Realistic Workload Testing**: Handle millions of accounts
- **Memory Scaling Studies**: Analyze memory usage patterns with large state
- **Recovery Performance**: Test checkpoint-based recovery at scale
- **Architecture Comparison**: Compare ring-based vs. log-based approaches

## Requirements

- POSIX-compliant system (Linux, macOS)
- GCC or Clang compiler with C99 support
- Sufficient RAM for account state and checkpoint buffers
- Disk space for ring-based checkpoint log
- Compatible transaction file generated by Report_testing transaction generator

## Notes

- The system pre-allocates the entire ring log file for optimal performance
- All checkpoint operations include proper fsync for durability
- The system gracefully handles transaction file wraparound for continuous operation
- Memory usage is displayed at startup to help with capacity planning
- Checkpoint validation ensures compatibility between runs with the same account count
- Range-set operations can efficiently initialize or reset large account ranges

## Integration with Test Scripts

This version integrates seamlessly with the `test_memset_percentages.sh` script:

```bash
# The script automatically compiles and runs both systems
./test_memset_percentages.sh

# Results compare:
# - breaking_glaze_with_memset_unlimited (this system)
# - state_management_parameterized (log-based system)
```

Both systems now process the same account range, enabling fair performance comparisons without the artificial limitations that skewed previous results. 