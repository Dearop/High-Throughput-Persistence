# High-Throughput Persistence: Transactional State Management Benchmarks

This project is a research and benchmarking suite for high-throughput, persistent, and recoverable state management systems, focusing on transaction processing, logging, and recovery in C. It includes a variety of implementations, each exploring different trade-offs in logging, snapshotting, batching, and recovery strategies.

## Table of Contents

- [Project Structure](#project-structure)
- [Core Concepts](#core-concepts)
- [Main Modules](#main-modules)
- [Testing and Benchmarks](#testing-and-benchmarks)
- [Building and Running](#building-and-running)
- [Data and Results](#data-and-results)
- [License](#license)
- [Acknowledgements](#acknowledgements)

---

## Project Structure

```
.
├── glaze_and_r_glaze/
├── remember_glaze/
├── breaking_glaze/
├── glaze/
├── Report_testing/
├── local_testing*/ (various)
├── log_optim/
├── opcode_glaze/
├── ring_buffer_glaze*/
├── s_fixing_glaze/
├── transaction_ring_glaze/
├── multithreaded_server_testing*/
├── main.tex
└── ...
```

- **glaze_and_r_glaze/**: Advanced, parameterized, chunked, and multi-threaded state management with hybrid logging (write sets for transfers, transaction logs for range sets).
- **remember_glaze/**: Multi-version and parameterized state management, with various output and debug modes.
- **breaking_glaze/**: Simpler, reference implementations for basic and memset-augmented transaction logging.
- **glaze/**: Early, basic chunked state management and logging.
- **Report_testing/**: Scripts and binaries for automated benchmarking (chunk size, memset, scalability).
- **local_testing*/**, **log_optim/**, **opcode_glaze/**, **ring_buffer_glaze*/**, **s_fixing_glaze/**, **transaction_ring_glaze/**: Variants exploring different logging, addressing, and batching strategies.
- **main.tex**: LaTeX source for the main report.

---

## Core Concepts

- **Chunked State**: The account state is divided into chunks for efficient snapshotting and logging.
- **Batching**: Transactions are processed in large batches (default: 2^16 per batch).
- **Hybrid Logging**: 
  - **Transfers**: Only the changed balances (write set) are logged.
  - **Range Sets**: The full transaction is logged.
- **Recovery**: On startup, the system reconstructs the state from the latest valid snapshot and replays logs.
- **Parameterization**: Most implementations allow tuning of account count, chunk size, and batch size via command-line arguments.

---

## Main Modules

### glaze_and_r_glaze/

- **state_management.c**: Main implementation. Supports hybrid logging, chunked snapshots, and multi-threaded commit.
- **generate_transactions.c**: Generates a mix of transfer and range set transactions, parameterized by account count and memset percentage.

### remember_glaze/

- **state_management_parametrized.c**, **state_management_multi.c**, **state_management_multi_txt_output.c**: Variants for parameterized, multi-version, and text-output state management.
- **debug.c**, **test.c**: Debugging and test harnesses.
- **generate_transactions.c**: Transaction generator.

### breaking_glaze/

- **breaking_glaze_with_memset_unlimited.c**: Reference for hybrid logging (write set + memset).
- **state_management.c**: Simple chunked state management.
- **generate_transactions.c**, **generate_transactions_memset.c**: Transaction generators.

### glaze/

- **state_management.c**: Early chunked state management using write sets and only handles transfers.
- **generate_transactions.c**: Transaction generator.

### Report_testing/
Measurements used for tests in the report that can be found in main.tex.
- **dealing_with_memset/**: Scripts and binaries for memset (range set) performance testing.
- **chunk_size_testing/**: Scripts and binaries for chunk size sensitivity testing.
- **scalability_testing/**: Scripts and binaries for scalability testing.
- **memset_test_results/**, **scalability_test_results/**: Collected benchmark results.

---

## Testing and Benchmarks

- **Automated scripts**:  
  - `Report_testing/dealing_with_memset/test_memset_percentages.sh`
  - `Report_testing/chunk_size_testing/run_recovery_tests.sh`
  - `Report_testing/scalability_testing/test_scalability.sh`
- **Results**:  
  - CSV and Markdown summaries in `Report_testing/memset_test_results/` and `Report_testing/scalability_testing/scalability_test_results/`.

---

## Building and Running

### Prerequisites

- GCC or Clang (C17 or later)
- POSIX environment (Linux, macOS)
- OpenMP (for parallel transaction generation)
- `make` (optional, for scripting builds)

### Example: Running the Main Benchmark

```sh
cd glaze_and_r_glaze
gcc -O3 -fopenmp -o state_management state_management.c -lpthread
gcc -O3 -fopenmp -o generate_transactions generate_transactions.c

# Generate transactions: 5% memset, 10 million accounts
./generate_transactions 5.0 10000000

# Run the state manager: 10 million accounts, 512KB chunk size
./state_management 10000000 512
```

### Other Variants

- See each subdirectory for its own `state_management.c` and `generate_transactions.c`.
- Most accept similar command-line arguments for account count, chunk size, and mode.

---

## Data and Results

- **Transaction logs**: `transactions.bin`
- **State snapshots**: `checkpoint_log.dat`
- **State hash**: `state_hash.dat`
- **Reference state**: `reference_state.bin`
- **Benchmark results**: See `Report_testing/*/` for CSV and Markdown summaries.

---

## License

This project is for academic and benchmarking purposes. Please cite appropriately if used in research.

---

## Acknowledgements

Developed as part of a Bachelor project at EPFL's Decentralised Computing Lab (DCL).

---

**For more details, see the source code and the main report (`main.tex`).** 