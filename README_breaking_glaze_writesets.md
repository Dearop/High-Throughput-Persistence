# Breaking Glaze with Write-Sets Implementation

## Overview

This is a **completely rewritten** version of `breaking_glaze_with_memset_unlimited.c` that implements **true write-sets** instead of saving transactions. This follows the correct algorithm for high-throughput persistence systems.

## Key Concept: Write-Sets vs Transactions

### What is a Write-Set?
A **write-set** captures the **actual state changes** that occurred during transaction execution:
- **Account Index**: Which account was modified
- **New Value**: The final value of that account after the transaction

### Before (Incorrect - Saving Transactions):
```c
// Saved the transaction itself
typedef struct {
    uint64_t sender;    // Transaction input
    uint64_t receiver;  // Transaction input  
    uint64_t amount;    // Transaction input
} Transaction;
```

### After (Correct - Saving Write-Sets):
```c
// Saves the actual state changes
typedef struct {
    uint64_t account_index;  // Which account was modified
    int64_t new_value;       // The new value after transaction
} WriteSetEntry;
```

## Algorithm Implementation

### 1. Transaction Processing with Write-Set Collection
```c
// For each transaction in batch:
for (size_t i = 0; i < read_count; i++) {
    apply_with_writeset(&batch[i], state, write_set, &write_set_count, ...);
}
```

**Balance Transfer Example:**
- Transaction: `transfer(account_5 -> account_10, amount=100)`
- Write-Set Generated:
  ```
  write_set[0] = {account_index: 5, new_value: 900000}   // sender after deduction
  write_set[1] = {account_index: 10, new_value: 1000100} // receiver after addition
  ```

**Memset Example:**
- Transaction: `memset(start=1000, len=500, value=42)`
- Write-Set Generated:
  ```
  write_set[0] = {account_index: 1000, new_value: 42}
  write_set[1] = {account_index: 1001, new_value: 42}
  ...
  write_set[499] = {account_index: 1499, new_value: 42}
  ```

### 2. Checkpoint Structure
```
[CheckpointHeader] [StateChunk] [WriteSetEntries]
     16 bytes        ≤512KB      Variable size
```

**CheckpointHeader:**
```c
typedef struct {
    uint32_t batch_num;         // Batch number
    uint32_t chunk_offset;      // Starting account index
    uint32_t chunk_count;       // Accounts in this chunk
    uint32_t write_set_count;   // Number of write-set entries
} CheckpointHeader;
```

### 3. Recovery Algorithm
```c
// Step 1: Load chunk state
memcpy(state + chunk_offset, state_chunk, chunk_count * sizeof(int64_t));

// Step 2: Apply write-sets sequentially
for (uint32_t j = 0; j < write_set_count; j++) {
    state[write_set[j].account_index] = write_set[j].new_value;
}
```

## Performance Characteristics

### Write-Set Size Analysis

**Balance Transfers:**
- Each transaction modifies exactly 2 accounts
- Write-set size: `2 * batch_size = 2 * 65,536 = 131,072 entries`
- Memory: `131,072 * 16 bytes = 2.0MB per batch`

**Memset Operations:**
- Each transaction can modify up to `range_length` accounts
- Worst case: Large memset operations
- Conservative allocation: `10 * batch_size = 655,360 entries`
- Memory: `655,360 * 16 bytes = 10.0MB per batch`

### Checkpoint Size Comparison

| Component | Size | Description |
|-----------|------|-------------|
| Header | 16 bytes | Checkpoint metadata |
| State Chunk | ≤512KB | Account state snapshot |
| Write-Sets | Variable | Actual state changes |

**Typical Sizes:**
- **Transfer-heavy workload**: ~2.5MB per checkpoint
- **Memset-heavy workload**: ~10.5MB per checkpoint
- **Mixed workload**: ~3-8MB per checkpoint

## Implementation Details

### Write-Set Collection
```c
static inline bool apply_transaction_to_state_array_with_writeset(
    const Transaction *tx, 
    int64_t *state, 
    WriteSetEntry *write_set, 
    uint32_t *write_set_count
) {
    // Apply transaction to state
    // Record each modified account in write_set
    // Increment write_set_count for each modification
}
```

### Memory Management
- **Write-Set Buffer**: Allocated once per process
- **Size**: `MAX_WRITE_SET_SIZE = BATCH_SIZE * 10 * sizeof(WriteSetEntry)`
- **Reused**: Buffer is reset for each batch

### Disk Layout
```
Ring Buffer (8 slots):
┌─────────────────────────────────────────────────────────────┐
│ Slot 0: [Header][Chunk 0][WriteSet] │ Slot 1: [Header][Chunk 1][WriteSet] │
├─────────────────────────────────────────────────────────────┤
│ Slot 2: [Header][Chunk 2][WriteSet] │ Slot 3: [Header][Chunk 3][WriteSet] │
├─────────────────────────────────────────────────────────────┤
│ ...                                                         │
└─────────────────────────────────────────────────────────────┘
```

## Usage

### Compilation
```bash
gcc -Wall -Wextra -O3 -std=c99 -D_POSIX_C_SOURCE=200809L \
    -o breaking_glaze_with_memset_unlimited \
    breaking_glaze_with_memset_unlimited.c -lm
```

### Execution
```bash
./breaking_glaze_with_memset_unlimited 5000000
```

### Example Output
```
Batch 1 of 50000 processed in 45.123 ms (success: 65536, failed: 0, 
checkpointed chunk 0: accounts 0-65535, 131072 write-sets, 2621456 bytes)

Batch 10 of 50000 processed in 42.567 ms (success: 65536, failed: 0, 
checkpointed chunk 1: accounts 65536-131071, 98304 write-sets, 2097168 bytes)
```

## Correctness Verification

### State Consistency
1. **Chunk State**: Provides baseline account values
2. **Write-Sets**: Apply all modifications in order
3. **Final State**: Mathematically equivalent to applying all transactions

### Recovery Verification
```c
// Original state after applying all transactions
uint64_t original_hash = fnv1a_hash(state, account_count);

// Reconstructed state from checkpoint + write-sets  
uint64_t reconstructed_hash = fnv1a_hash(reconstructed_state, account_count);

assert(original_hash == reconstructed_hash);  // Must be identical
```

## Performance Benefits

### 1. Accurate State Capture
- **Before**: Transactions needed to be re-executed during recovery
- **After**: Direct state application, no re-execution needed

### 2. Efficient Recovery
- **Before**: `O(transactions)` recovery time
- **After**: `O(write_sets)` recovery time (often much smaller)

### 3. Correct Semantics
- **Before**: Risk of non-deterministic re-execution
- **After**: Deterministic state reconstruction

## Limitations and Considerations

### Memory Usage
- Write-set buffer requires significant memory (up to 10MB per batch)
- Conservative allocation may waste memory for transfer-only workloads

### Disk Usage
- Variable checkpoint sizes based on workload characteristics
- Memset-heavy workloads require more disk space

### Future Optimizations
1. **Adaptive Allocation**: Adjust write-set buffer size based on workload
2. **Compression**: Compress write-sets for repeated values
3. **Delta Encoding**: Store only changes from previous checkpoint
4. **Batch Optimization**: Merge consecutive write-sets to same account

## Compatibility

- **Input**: Same transaction format as before
- **Output**: Enhanced checkpoint format with write-sets
- **Recovery**: Improved accuracy and performance
- **Monitoring**: Enhanced progress reporting with write-set counts 