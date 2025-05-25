# Parameterized State Management System

This is a parameterized version of the high-throughput persistence state management system that allows you to specify the number of accounts as a command-line parameter.

## Key Features

- **Parameterized Account Count**: Specify any number of accounts at runtime
- **Dynamic Memory Allocation**: All data structures are sized based on the account count parameter
- **Optimized Performance**: Maintains all performance optimizations from the original system
- **Threading Support**: Asynchronous commit operations using worker threads
- **Recovery System**: Full state recovery from checkpoint logs
- **Hash Verification**: State integrity verification using hash functions

## Compilation

Use the provided Makefile:

```bash
make -f Makefile_parameterized
```

Or compile manually:

```bash
gcc -Wall -Wextra -O3 -std=c99 -pthread -D_POSIX_C_SOURCE=200809L \
    -o state_management_parameterized state_management_parameterized.c \
    -pthread -lm
```

## Usage

### Basic Usage

```bash
./state_management_parameterized <num_accounts> [saveref]
```

### Parameters

- `num_accounts`: Number of accounts to simulate (required)
  - Must be a positive integer
  - Examples: 1000000 (1 million), 500000000 (500 million)
- `saveref`: Optional flag to run in reference mode
  - Saves the final state as a reference for comparison

### Examples

```bash
# Run with 1 million accounts
./state_management_parameterized 1000000

# Run with 500 million accounts in reference mode
./state_management_parameterized 500000000 saveref

# Run with 10 thousand accounts for testing
./state_management_parameterized 10000
```

## System Configuration

The system automatically calculates optimal parameters based on the account count:

- **Chunk Size**: 512KB target per state chunk
- **Accounts per Chunk**: Calculated as `512KB / 8 bytes = 65,536 accounts`
- **Number of Chunks**: `(account_count + accounts_per_chunk - 1) / accounts_per_chunk`
- **Padded Account Count**: Rounded up to chunk boundary

## Memory Requirements

The system allocates memory based on the account count:

- **State Array**: `account_count * 8 bytes` (aligned to 64-byte boundary)
- **Log File**: Variable size based on number of chunks
- **Transaction Buffer**: Fixed at 65,536 transactions per batch

## Performance Characteristics

- **Batch Processing**: 65,536 transactions per batch
- **Asynchronous Commits**: Background thread handles disk I/O
- **Memory-Mapped I/O**: Efficient log file access
- **Optimized Transaction Processing**: Branch prediction and prefetching

## Output Files

The system creates several files during operation:

- `checkpoint_log.dat`: Main persistence log with state snapshots and transactions
- `state_hash.dat`: Hash of final state for verification
- `reference_state.bin`: Reference state file (when using `saveref` flag)
- `transactions.bin`: Input transaction file (must exist)

## Error Handling

The program includes comprehensive error handling:

- Invalid account count validation
- Memory allocation failure detection
- File I/O error reporting
- Thread synchronization error handling

## Performance Monitoring

The system reports detailed performance metrics:

- Batch processing times (average, median, 99th percentile)
- Recovery time measurements
- Transaction throughput statistics
- Memory usage information

## Differences from Original

This parameterized version differs from the original in several key ways:

1. **Dynamic Sizing**: All constants are now calculated at runtime
2. **Variable-Length Arrays**: ChunkSlot uses flexible array members
3. **Parameter Validation**: Command-line argument parsing and validation
4. **Memory Management**: Dynamic allocation for all size-dependent structures
5. **Usage Information**: Built-in help and usage examples

## Testing

To test with different account counts:

```bash
# Small test (fast)
./state_management_parameterized 1000

# Medium test
./state_management_parameterized 100000

# Large test (requires sufficient memory and disk space)
./state_management_parameterized 10000000
```

## Requirements

- POSIX-compliant system (Linux, macOS)
- GCC or Clang compiler
- pthread library
- Sufficient RAM for account count * 8 bytes
- Disk space for log files (varies with account count)

## Notes

- The transaction file `transactions.bin` must exist before running
- Log files are persistent across runs for recovery testing
- Use `saveref` mode to create reference states for verification
- Performance scales with account count and available system resources 