#!/bin/bash

# Scalability test script for log_optim_parameterized and state_management_parameterized
# Tests with increasing account counts from 2M to 100M
# RUN THIS ON AWS UBUNTU SERVER ONLY

set -e  # Exit on any error

# Configuration
MEMSET_PERCENTAGE=50  # Fixed at 50% for scalability testing
OUTPUT_DIR="scalability_test_results"
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
LOG_FILE="${OUTPUT_DIR}/scalability_test_log_${TIMESTAMP}.txt"

# Account count progression (in millions)
ACCOUNT_COUNTS=(2000000 5000000 10000000 20000000 50000000 100000000)
ACCOUNT_LABELS=("2M" "5M" "10M" "20M" "50M" "100M")

# Base directory where script and C files are located
BASE_DIR=$(pwd) # Assumes script is run from its location

# Create output directory
mkdir -p "$OUTPUT_DIR"

# Function to log messages
log_message() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

# Function to format bytes to human readable
format_bytes() {
    local bytes=$1
    if [ "$bytes" -gt 1073741824 ]; then # Use "$bytes" for string comparison with -gt in arithmetic context
        echo "$(echo "scale=2; $bytes / 1073741824" | bc)GB"
    elif [ "$bytes" -gt 1048576 ]; then
        echo "$(echo "scale=2; $bytes / 1048576" | bc)MB"
    elif [ "$bytes" -gt 1024 ]; then
        echo "$(echo "scale=2; $bytes / 1024" | bc)KB"
    else
        echo "${bytes}B"
    fi
}

# Function to compile programs (force recompilation for Ubuntu)
compile_programs() {
    log_message "Compiling programs for Ubuntu..."
    
    # Force recompilation of transaction generator (now parameterized)
    log_message "Compiling transaction generator (generate_transactions.c)..."
    if ! gcc -Wall -Wextra -O3 -std=c99 -fopenmp -o "${BASE_DIR}/generate_transactions" "${BASE_DIR}/generate_transactions.c" -lm; then
        log_message "ERROR: Compilation of generate_transactions.c failed."
        exit 1
    fi
    
    # Force recompilation of log_optim_parameterized
    log_message "Compiling log_optim_parameterized..."
    if ! gcc -Wall -Wextra -O3 -std=c99 -fopenmp -D_GNU_SOURCE -o "${BASE_DIR}/log_optim_parameterized" "${BASE_DIR}/log_optim_parameterized.c" -lm; then
        log_message "ERROR: Compilation of log_optim_parameterized.c failed."
        exit 1
    fi
    
    # Force recompilation of state_management_parameterized
    log_message "Compiling state_management_parameterized..."
    if ! gcc -Wall -Wextra -O3 -std=c99 -fopenmp -D_GNU_SOURCE -o "${BASE_DIR}/state_management_parameterized" "${BASE_DIR}/state_management_parameterized.c" -lm; then
        log_message "ERROR: Compilation of state_management_parameterized.c failed."
        exit 1
    fi
    
    log_message "All programs compiled successfully for Ubuntu."
}

# Function to clean up files
cleanup_files() {
    log_message "Cleaning up temporary files..."
    rm -f "${BASE_DIR}/transactions.bin"
    rm -f "${BASE_DIR}/checkpoint_log.dat" "${BASE_DIR}/state_hash.dat" "${BASE_DIR}/reconstructed_state.txt"
    rm -f "${BASE_DIR}/state_management_output.txt"
    # Be cautious with broad rm commands, ensure they target the correct directory if needed
    # rm -f ${BASE_DIR}/*.bin ${BASE_DIR}/*.dat ${BASE_DIR}/*.txt 2>/dev/null || true
}

# Function to check available disk space
check_disk_space() {
    local required_gb=$1
    local available_kb=$(df "${BASE_DIR}" | tail -1 | awk '{print $4}')
    local available_gb=$(echo "scale=2; $available_kb / 1024 / 1024" | bc)
    
    log_message "Available disk space in ${BASE_DIR}: ${available_gb}GB, Required: ${required_gb}GB"
    
    if (( $(echo "${available_gb} < ${required_gb}" | bc -l) )); then
        log_message "WARNING: Insufficient disk space. Available: ${available_gb}GB, Required: ${required_gb}GB"
        return 1
    fi
    return 0
}

# Function to estimate memory and disk requirements
estimate_requirements() {
    local account_count=$1
    
    # State size: account_count * 8 bytes (int64_t)
    local state_size_bytes=$(echo "${account_count} * 8" | bc)
    
    # Transaction file size: Based on TOTAL_TRANSACTIONS * sizeof(Transaction) which is 24 bytes
    # TOTAL_TRANSACTIONS is (1<<16) * 50000 = 3,276,800,000
    # Size = 3,276,800,000 * 24 bytes = 78,643,200,000 bytes = ~73.24 GB
    # This seems independent of account_count in the current generate_transactions.c
    local tx_file_gb=73.24 
    
    # Log files can be large, estimate 2x transaction file size + state size
    # This estimation is very rough.
    local log_disk_estimate_gb=$(echo "scale=2; $tx_file_gb * 2" | bc)
    local state_disk_gb=$(echo "scale=2; $state_size_bytes / 1073741824" | bc)
    local total_disk_gb=$(echo "scale=2; $tx_file_gb + $log_disk_estimate_gb + $state_disk_gb" | bc)
    
    log_message "Estimated requirements for $account_count accounts:"
    log_message "  State size: $(format_bytes $state_size_bytes)"
    log_message "  Transaction file (fixed size): ${tx_file_gb}GB"
    log_message "  Total disk needed (rough estimate): ${total_disk_gb}GB"
    
    echo "$total_disk_gb"
}

# Function to run a single test
run_scalability_test() {
    local account_count=$1
    local account_label=$2
    local system_name=$3
    local command_template=$4 # Command template, e.g., "./program_name ACCOUNTS_ARG"

    # Replace placeholder in command with actual account count
    local command=${command_template//ACCOUNTS_ARG/$account_count}
    
    log_message "=== Testing ${system_name} with ${account_count} accounts (${account_label}) ==="
    
    # Estimate and check requirements
    local required_disk=$(estimate_requirements $account_count)
    if ! check_disk_space "$required_disk"; then
        log_message "Skipping ${system_name} test for ${account_count} accounts due to insufficient disk space"
        return 1 # Indicate failure/skip
    fi
    
    # Generate transactions for this account count
    log_message "Generating transactions for ${account_count} accounts with ${MEMSET_PERCENTAGE}% memset..."
    log_message "Executing: ${BASE_DIR}/generate_transactions \"$MEMSET_PERCENTAGE\" \"$account_count\""
    local gen_start=$(date +%s.%N)
    
    # All paths should be relative to BASE_DIR or absolute
    # Output of generate_transactions is transactions.bin in the CWD of generate_transactions
    # which is BASE_DIR because we execute it as ${BASE_DIR}/generate_transactions
    if ! "${BASE_DIR}/generate_transactions" "$MEMSET_PERCENTAGE" "$account_count" > "${BASE_DIR}/${OUTPUT_DIR}/generation_${system_name}_${account_label}_${TIMESTAMP}.log" 2>&1; then
        log_message "ERROR: Transaction generation failed for ${account_count} accounts. Check ${BASE_DIR}/${OUTPUT_DIR}/generation_${system_name}_${account_label}_${TIMESTAMP}.log"
        return 1
    fi
    
    local gen_end=$(date +%s.%N)
    local gen_duration=$(echo "$gen_end - $gen_start" | bc -l)
    
    if [ ! -f "${BASE_DIR}/transactions.bin" ]; then
        log_message "ERROR: transactions.bin not created at ${BASE_DIR}/transactions.bin for ${account_count} accounts"
        return 1
    fi
    
    local tx_file_size=$(stat -c%s "${BASE_DIR}/transactions.bin" 2>/dev/null || stat -f%z "${BASE_DIR}/transactions.bin" 2>/dev/null || echo "0")
    log_message "Transaction file generated in ${gen_duration}s, size: $(format_bytes $tx_file_size)"
    
    # Run the system
    log_message "Running ${system_name} with ${account_count} accounts... Command: $command"
    local start_time=$(date +%s.%N)
    
    # Execute the command, ensuring it runs in BASE_DIR context if necessary, or uses absolute paths for its files.
    # The provided command for state_management_parameterized and log_optim_parameterized needs to handle its own paths.
    # We assume they create output files like checkpoint_log.dat in their CWD (which is BASE_DIR).
    if eval "${command}" > "${BASE_DIR}/${OUTPUT_DIR}/${system_name}_${account_label}_${TIMESTAMP}.log" 2>&1; then
        local end_time=$(date +%s.%N)
        local duration=$(echo "$end_time - $start_time" | bc -l)
        log_message "${system_name} completed successfully in ${duration} seconds"
        
        # Extract key metrics from the output
        extract_scalability_metrics "${BASE_DIR}/${OUTPUT_DIR}/${system_name}_${account_label}_${TIMESTAMP}.log" "$system_name" "$account_count" "$account_label" "$duration" "$gen_duration" "$tx_file_size"
    else
        log_message "ERROR: ${system_name} failed for ${account_count} accounts. Check ${BASE_DIR}/${OUTPUT_DIR}/${system_name}_${account_label}_${TIMESTAMP}.log"
        return 1
    fi
    
    # Clean up for next test
    cleanup_files
    return 0 # Indicate success
}

# Function to extract metrics from output logs
extract_scalability_metrics() {
    local log_file=$1
    local system_name=$2
    local account_count=$3
    local account_label=$4
    local duration=$5
    local gen_duration=$6
    local tx_file_size=$7
    
    # Extract throughput and timing information
    # Add defensive checks for grep results
    local throughput=$(grep -o "Throughput: [0-9.]* tx/sec" "$log_file" | grep -o "[0-9.]*" || echo "N/A")
    if [[ "$throughput" == "N/A" ]]; then throughput=$(grep -i "transactions/sec:" "$log_file" | awk '{print $NF}' || echo "N/A"); fi # For log_optim
    if [[ "$throughput" == "N/A" ]]; then throughput=$(grep -i "Overall throughput:" "$log_file" | awk '{print $3}' || echo "N/A"); fi # For state_management_parameterized new format

    local total_tx=$(grep -o "Total transactions: [0-9]*" "$log_file" | grep -o "[0-9]*" || echo "N/A")
    local avg_batch_time=$(grep -o "Average batch time: [0-9.]* ms" "$log_file" | grep -o "[0-9.]*" || echo "N/A")
    local median_time=$(grep -o "Median batch time: [0-9.]* ms" "$log_file" | grep -o "[0-9.]*" || echo "N/A")
    local p99_time=$(grep -o "99th percentile batch time: [0-9.]* ms" "$log_file" | grep -o "[0-9.]*" || echo "N/A")
    local total_processing_time_ms=$(grep -o "Total time: [0-9.]* ms" "$log_file" | grep -o "[0-9.]*" || echo "N/A")
    if [[ "$total_processing_time_ms" == "N/A" ]]; then total_processing_time_ms=$(grep -i "Total processing time:" "$log_file" | awk '{print $4}' || echo "N/A"); fi
    
    # Calculate memory usage (state size)
    local state_size_mb=$(echo "scale=2; $account_count * 8 / 1048576" | bc)
    
    # Append to CSV results file
    echo "${system_name},${account_count},${account_label},${duration},${gen_duration},${throughput},${total_tx},${avg_batch_time},${median_time},${p99_time},${total_processing_time_ms},${state_size_mb},${tx_file_size}" >> "${BASE_DIR}/${OUTPUT_DIR}/scalability_results_${TIMESTAMP}.csv"
    
    # Log the extracted metrics
    log_message "  Metrics: Throughput=${throughput} tx/sec, AvgBatch=${avg_batch_time}ms, StateSize=${state_size_mb}MB, TotalProcessingTime=${total_processing_time_ms}ms"
}

# Function to create CSV header
create_csv_header() {
    echo "System,Account_Count,Account_Label,Total_Test_Duration_Sec,Generation_Duration_Sec,Throughput_TxPerSec,Total_Transactions,Avg_Batch_Time_Ms,Median_Batch_Time_Ms,P99_Batch_Time_Ms,Total_Processing_Time_Ms,State_Size_MB,Transaction_File_Size_Bytes" > "${BASE_DIR}/${OUTPUT_DIR}/scalability_results_${TIMESTAMP}.csv"
}

# Function to generate summary report
generate_summary() {
    log_message "Generating scalability summary report..."
    
    local summary_file="${BASE_DIR}/${OUTPUT_DIR}/scalability_summary_${TIMESTAMP}.md"
    
    cat > "$summary_file" << EOF
# Scalability Test Results

**Test Date:** $(date)
**Memset Percentage:** ${MEMSET_PERCENTAGE}%
**Account Count Range:** ${ACCOUNT_LABELS[0]} to ${ACCOUNT_LABELS[-1]} accounts
**Systems Tested:** log_optim_parameterized, state_management_parameterized

## Test Configuration

- **Transaction Generation:**
  - Batch size: $(printf "%'d" $((1<<16)) ) transactions
  - Total batches: $(printf "%'d" 50000)
  - Total transactions: $(printf "%'d" $(( (1<<16) * 50000 )) ) per test
  - Memset percentage: ${MEMSET_PERCENTAGE}% (fixed)

- **Account Count Progression:**
EOF

    for i in "${!ACCOUNT_COUNTS[@]}"; do
        echo "  - ${ACCOUNT_LABELS[$i]}: $(printf "%'d" ${ACCOUNT_COUNTS[$i]}) accounts" >> "$summary_file"
    done

    cat >> "$summary_file" << EOF

## Results Summary

The detailed results are available in: \`${OUTPUT_DIR}/scalability_results_${TIMESTAMP}.csv\`

### Performance vs Account Count

EOF

    # Add performance comparison table
    if [ -f "${BASE_DIR}/${OUTPUT_DIR}/scalability_results_${TIMESTAMP}.csv" ]; then
        echo "| Accounts | log_optim Throughput (tx/s) | state_mgmt Throughput (tx/s) | log_optim Avg Batch (ms) | state_mgmt Avg Batch (ms) | State Size (MB) |" >> "$summary_file"
        echo "|----------|-----------------------------|------------------------------|--------------------------|---------------------------|-----------------|" >> "$summary_file"
        
        # Process CSV to create comparison table
        for i in "${!ACCOUNT_COUNTS[@]}"; do
            local count=${ACCOUNT_COUNTS[$i]}
            local label=${ACCOUNT_LABELS[$i]}
            
            local log_throughput=$(awk -F, -v ac="$count" '$1 == "log_optim_parameterized" && $2 == ac {print $6}' "${BASE_DIR}/${OUTPUT_DIR}/scalability_results_${TIMESTAMP}.csv" || echo "N/A")
            local state_throughput=$(awk -F, -v ac="$count" '$1 == "state_management_parameterized" && $2 == ac {print $6}' "${BASE_DIR}/${OUTPUT_DIR}/scalability_results_${TIMESTAMP}.csv" || echo "N/A")
            local log_avg_batch=$(awk -F, -v ac="$count" '$1 == "log_optim_parameterized" && $2 == ac {print $8}' "${BASE_DIR}/${OUTPUT_DIR}/scalability_results_${TIMESTAMP}.csv" || echo "N/A")
            local state_avg_batch=$(awk -F, -v ac="$count" '$1 == "state_management_parameterized" && $2 == ac {print $8}' "${BASE_DIR}/${OUTPUT_DIR}/scalability_results_${TIMESTAMP}.csv" || echo "N/A")
            local state_size=$(awk -F, -v ac="$count" '$1 == "log_optim_parameterized" && $2 == ac {print $12}' "${BASE_DIR}/${OUTPUT_DIR}/scalability_results_${TIMESTAMP}.csv" || echo "N/A") # State size should be same for both for a given account count
            
            echo "| $label | $log_throughput | $state_throughput | ${log_avg_batch} | ${state_avg_batch} | ${state_size} |" >> "$summary_file"
        done
    fi
cat >> "$summary_file" << EOF

### Notes:
- 'N/A' indicates data was not found or an error occurred during extraction.
- Throughput is calculated based on the total processing time reported by the application.
EOF

    log_message "Scalability summary report generated: ${summary_file}"
}


# --- Main script execution ---

log_message "Starting scalability test suite."
log_message "Output directory: ${OUTPUT_DIR}"
log_message "Log file: ${LOG_FILE}"

cd "${BASE_DIR}" # Ensure we are in the script's directory

compile_programs
create_csv_header

# Main test loop
for i in "${!ACCOUNT_COUNTS[@]}"; do
    current_account_count=${ACCOUNT_COUNTS[$i]}
    current_account_label=${ACCOUNT_LABELS[$i]}

    log_message "--- Iteration for ${current_account_label} (${current_account_count} accounts) ---"
    
    # Test log_optim_parameterized
    # It takes NUM_ACCOUNTS, BATCH_SIZE, NUM_BATCHES, CHUNK_SIZE_KB, RING_SLOTS, EXPENSIVE_OP_RATIO
    # For scalability, let's keep BATCH_SIZE, NUM_BATCHES fixed. CHUNK_SIZE_KB can be 512.
    # RING_SLOTS can be dynamic or fixed e.g. 8 or more. Let's use 8.
    # EXPENSIVE_OP_RATIO = 0 for these tests. We are testing with memset_percentage from generate_transactions.
    cmd_log_optim="./log_optim_parameterized ${current_account_count} $((1<<16)) 50000 512 8 0"
    if ! run_scalability_test "$current_account_count" "$current_account_label" "log_optim_parameterized" "$cmd_log_optim"; then
        log_message "Test run failed for log_optim_parameterized with ${current_account_label}. See logs."
    fi

    # Test state_management_parameterized
    # It takes SMALL_ACCOUNT_COUNT, NUM_BATCHES, BATCH_SIZE, CHUNK_SIZE_PARAM_KB, RING_SIZE_PARAM, MAX_WRITE_SET_COUNT_MULTIPLIER
    # Let's use similar fixed params where applicable.
    # MAX_WRITE_SET_COUNT_MULTIPLIER could be e.g. 16
    cmd_state_mgmt="./state_management_parameterized ${current_account_count} 50000 $((1<<16)) 512 8 16"
    if ! run_scalability_test "$current_account_count" "$current_account_label" "state_management_parameterized" "$cmd_state_mgmt"; then
        log_message "Test run failed for state_management_parameterized with ${current_account_label}. See logs."
    fi

done

generate_summary

log_message "Scalability test suite finished." 