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

# Create output directory
mkdir -p "$OUTPUT_DIR"

# Function to log messages
log_message() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

# Function to format bytes to human readable
format_bytes() {
    local bytes=$1
    if [ $bytes -gt 1073741824 ]; then
        echo "$(echo "scale=2; $bytes / 1073741824" | bc)GB"
    elif [ $bytes -gt 1048576 ]; then
        echo "$(echo "scale=2; $bytes / 1048576" | bc)MB"
    elif [ $bytes -gt 1024 ]; then
        echo "$(echo "scale=2; $bytes / 1024" | bc)KB"
    else
        echo "${bytes}B"
    fi
}

# Function to compile programs (force recompilation for Ubuntu)
compile_programs() {
    log_message "Compiling programs for Ubuntu..."
    
    # Force recompilation of transaction generator
    log_message "Compiling transaction generator..."
    gcc -Wall -Wextra -O3 -std=c99 -fopenmp -o generate_transactions generate_transactions.c -lm
    
    # Force recompilation of log_optim_parameterized
    log_message "Compiling log_optim_parameterized..."
    gcc -Wall -Wextra -O3 -std=c99 -fopenmp -D_GNU_SOURCE -o log_optim_parameterized log_optim_parameterized.c -lm
    
    # Force recompilation of state_management_parameterized
    log_message "Compiling state_management_parameterized..."
    gcc -Wall -Wextra -O3 -std=c99 -fopenmp -D_GNU_SOURCE -o state_management_parameterized state_management_parameterized.c -lm
    
    log_message "All programs compiled successfully for Ubuntu."
}

# Function to clean up files
cleanup_files() {
    log_message "Cleaning up temporary files..."
    rm -f transactions.bin
    rm -f checkpoint_log.dat state_hash.dat reconstructed_state.txt
    rm -f state_management_output.txt
    rm -f *.bin *.dat *.txt 2>/dev/null || true
}

# Function to check available disk space
check_disk_space() {
    local required_gb=$1
    local available_kb=$(df . | tail -1 | awk '{print $4}')
    local available_gb=$(echo "scale=2; $available_kb / 1024 / 1024" | bc)
    
    log_message "Available disk space: ${available_gb}GB, Required: ${required_gb}GB"
    
    if (( $(echo "$available_gb < $required_gb" | bc -l) )); then
        log_message "WARNING: Insufficient disk space. Available: ${available_gb}GB, Required: ${required_gb}GB"
        return 1
    fi
    return 0
}

# Function to estimate memory and disk requirements
estimate_requirements() {
    local account_count=$1
    
    # State size: account_count * 8 bytes (int64_t)
    local state_size_bytes=$(echo "$account_count * 8" | bc)
    
    # Transaction file size: ~7.3GB for 5M accounts, scales linearly
    local tx_file_gb=$(echo "scale=2; $account_count * 7.3 / 5000000" | bc)
    
    # Log files can be large, estimate 2x transaction file size
    local total_disk_gb=$(echo "scale=2; $tx_file_gb * 3" | bc)
    
    log_message "Estimated requirements for $account_count accounts:"
    log_message "  State size: $(format_bytes $state_size_bytes)"
    log_message "  Transaction file: ${tx_file_gb}GB"
    log_message "  Total disk needed: ${total_disk_gb}GB"
    
    echo "$total_disk_gb"
}

# Function to run a single test
run_scalability_test() {
    local account_count=$1
    local account_label=$2
    local system_name=$3
    local command=$4
    
    log_message "=== Testing ${system_name} with ${account_count} accounts (${account_label}) ==="
    
    # Estimate and check requirements
    local required_disk=$(estimate_requirements $account_count)
    if ! check_disk_space "$required_disk"; then
        log_message "Skipping ${system_name} test for ${account_count} accounts due to insufficient disk space"
        return 1
    fi
    
    # Generate transactions for this account count
    log_message "Generating transactions for ${account_count} accounts with ${MEMSET_PERCENTAGE}% memset..."
    local gen_start=$(date +%s.%N)
    
    if ! ./generate_transactions "$MEMSET_PERCENTAGE" > "${OUTPUT_DIR}/generation_${system_name}_${account_label}_${TIMESTAMP}.log" 2>&1; then
        log_message "ERROR: Transaction generation failed for ${account_count} accounts"
        return 1
    fi
    
    local gen_end=$(date +%s.%N)
    local gen_duration=$(echo "$gen_end - $gen_start" | bc -l)
    
    if [ ! -f "transactions.bin" ]; then
        log_message "ERROR: transactions.bin not created for ${account_count} accounts"
        return 1
    fi
    
    local tx_file_size=$(stat -c%s "transactions.bin" 2>/dev/null || stat -f%z "transactions.bin" 2>/dev/null || echo "0")
    log_message "Transaction file generated in ${gen_duration}s, size: $(format_bytes $tx_file_size)"
    
    # Run the system
    log_message "Running ${system_name} with ${account_count} accounts..."
    local start_time=$(date +%s.%N)
    
    if eval "$command" > "${OUTPUT_DIR}/${system_name}_${account_label}_${TIMESTAMP}.log" 2>&1; then
        local end_time=$(date +%s.%N)
        local duration=$(echo "$end_time - $start_time" | bc -l)
        log_message "${system_name} completed successfully in ${duration} seconds"
        
        # Extract key metrics from the output
        extract_scalability_metrics "${OUTPUT_DIR}/${system_name}_${account_label}_${TIMESTAMP}.log" "$system_name" "$account_count" "$account_label" "$duration" "$gen_duration" "$tx_file_size"
    else
        log_message "ERROR: ${system_name} failed for ${account_count} accounts"
        return 1
    fi
    
    # Clean up for next test
    cleanup_files
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
    local throughput=$(grep -o "Throughput: [0-9.]* tx/sec" "$log_file" | grep -o "[0-9.]*" || echo "N/A")
    local total_tx=$(grep -o "Total transactions: [0-9]*" "$log_file" | grep -o "[0-9]*" || echo "N/A")
    local avg_batch_time=$(grep -o "Average batch time: [0-9.]* ms" "$log_file" | grep -o "[0-9.]*" || echo "N/A")
    local median_time=$(grep -o "Median batch time: [0-9.]* ms" "$log_file" | grep -o "[0-9.]*" || echo "N/A")
    local p99_time=$(grep -o "99th percentile batch time: [0-9.]* ms" "$log_file" | grep -o "[0-9.]*" || echo "N/A")
    local total_time=$(grep -o "Total time: [0-9.]* ms" "$log_file" | grep -o "[0-9.]*" || echo "N/A")
    
    # Calculate memory usage (state size)
    local state_size_mb=$(echo "scale=2; $account_count * 8 / 1048576" | bc)
    
    # Append to CSV results file
    echo "${system_name},${account_count},${account_label},${duration},${gen_duration},${throughput},${total_tx},${avg_batch_time},${median_time},${p99_time},${total_time},${state_size_mb},${tx_file_size}" >> "${OUTPUT_DIR}/scalability_results_${TIMESTAMP}.csv"
    
    # Log the extracted metrics
    log_message "  Metrics: Throughput=${throughput} tx/sec, AvgBatch=${avg_batch_time}ms, StateSize=${state_size_mb}MB"
}

# Function to create CSV header
create_csv_header() {
    echo "System,Account_Count,Account_Label,Processing_Duration_Sec,Generation_Duration_Sec,Throughput_TxPerSec,Total_Transactions,Avg_Batch_Time_Ms,Median_Batch_Time_Ms,P99_Batch_Time_Ms,Total_Processing_Time_Ms,State_Size_MB,Transaction_File_Size_Bytes" > "${OUTPUT_DIR}/scalability_results_${TIMESTAMP}.csv"
}

# Function to generate summary report
generate_summary() {
    log_message "Generating scalability summary report..."
    
    local summary_file="${OUTPUT_DIR}/scalability_summary_${TIMESTAMP}.md"
    
    cat > "$summary_file" << EOF
# Scalability Test Results

**Test Date:** $(date)
**Memset Percentage:** ${MEMSET_PERCENTAGE}%
**Account Count Range:** 2M to 100M accounts
**Systems Tested:** log_optim_parameterized, state_management_parameterized

## Test Configuration

- **Transaction Generation:**
  - Batch size: 65,536 transactions
  - Total batches: 50,000
  - Total transactions: ~3.28 billion per test
  - Memset percentage: ${MEMSET_PERCENTAGE}% (fixed)

- **Account Count Progression:**
EOF

    for i in "${!ACCOUNT_COUNTS[@]}"; do
        echo "  - ${ACCOUNT_LABELS[$i]}: ${ACCOUNT_COUNTS[$i]} accounts" >> "$summary_file"
    done

    cat >> "$summary_file" << EOF

## Results Summary

The detailed results are available in: \`scalability_results_${TIMESTAMP}.csv\`

### Performance vs Account Count

EOF

    # Add performance comparison table
    if [ -f "${OUTPUT_DIR}/scalability_results_${TIMESTAMP}.csv" ]; then
        echo "| Accounts | log_optim Throughput | state_mgmt Throughput | log_optim Avg Batch | state_mgmt Avg Batch | State Size |" >> "$summary_file"
        echo "|----------|---------------------|----------------------|--------------------|--------------------|------------|" >> "$summary_file"
        
        # Process CSV to create comparison table
        for i in "${!ACCOUNT_COUNTS[@]}"; do
            local count=${ACCOUNT_COUNTS[$i]}
            local label=${ACCOUNT_LABELS[$i]}
            
            local log_throughput=$(grep "log_optim_parameterized,$count," "${OUTPUT_DIR}/scalability_results_${TIMESTAMP}.csv" | cut -d',' -f6 || echo "N/A")
            local state_throughput=$(grep "state_management_parameterized,$count," "${OUTPUT_DIR}/scalability_results_${TIMESTAMP}.csv" | cut -d',' -f6 || echo "N/A")
            local log_avg_batch=$(grep "log_optim_parameterized,$count," "${OUTPUT_DIR}/scalability_results_${TIMESTAMP}.csv" | cut -d',' -f8 || echo "N/A")
            local state_avg_batch=$(grep "state_management_parameterized,$count," "${OUTPUT_DIR}/scalability_results_${TIMESTAMP}.csv" | cut -d',' -f8 || echo "N/A")
            local state_size=$(grep "log_optim_parameterized,$count," "${OUTPUT_DIR}/scalability_results_${TIMESTAMP}.csv" | cut -d',' -f12 || echo "N/A")
            
            echo "| $label | $log_throughput | $state_throughput | ${log_avg_batch}ms | ${state_avg_batch}ms | ${state_size}MB |" >> "$summary_file"
        done
    fi
    
    cat >> "$summary_file" << EOF

## Analysis Notes

- **log_optim_parameterized**: Optimized logging with parameterized account counts
- **state_management_parameterized**: Full state management with parameterized account counts
- **State Size**: Linear growth with account count (8 bytes per account)
- **Transaction File Size**: Scales with account count and transaction complexity
- **Memory Requirements**: Increase significantly with larger account counts

## Performance Expectations

- **Throughput**: May decrease with larger account counts due to memory pressure
- **Batch Time**: May increase with larger state sizes and I/O overhead
- **Disk Usage**: Transaction files and logs grow substantially with account count
- **Memory Usage**: State size grows linearly (8 bytes × account count)

EOF

    log_message "Scalability summary report generated: $summary_file"
}

# Main execution
main() {
    log_message "Starting scalability test on Ubuntu AWS server"
    log_message "Testing account counts: ${ACCOUNT_LABELS[*]}"
    log_message "Memset percentage: ${MEMSET_PERCENTAGE}%"
    log_message "Output directory: ${OUTPUT_DIR}"
    
    # Check if bc is available
    if ! command -v bc &> /dev/null; then
        log_message "ERROR: bc calculator not found. Please install bc."
        exit 1
    fi
    
    # Compile programs
    compile_programs
    
    # Create CSV header
    create_csv_header
    
    # Test each account count
    for i in "${!ACCOUNT_COUNTS[@]}"; do
        local account_count=${ACCOUNT_COUNTS[$i]}
        local account_label=${ACCOUNT_LABELS[$i]}
        
        log_message ""
        log_message "=========================================="
        log_message "Testing with ${account_count} accounts (${account_label})"
        log_message "=========================================="
        
        # Test log_optim_parameterized
        if ! run_scalability_test "$account_count" "$account_label" "log_optim_parameterized" "./log_optim_parameterized ${account_count}"; then
            log_message "WARNING: log_optim_parameterized test failed for ${account_count} accounts"
        fi
        
        # Test state_management_parameterized
        if ! run_scalability_test "$account_count" "$account_label" "state_management_parameterized" "./state_management_parameterized ${account_count}"; then
            log_message "WARNING: state_management_parameterized test failed for ${account_count} accounts"
        fi
        
        # Brief pause between test sets
        sleep 5
        
        # Check if we should continue (disk space, memory, etc.)
        log_message "Completed testing with ${account_count} accounts"
    done
    
    # Generate summary
    generate_summary
    
    log_message ""
    log_message "=========================================="
    log_message "Scalability tests completed!"
    log_message "Results saved in: ${OUTPUT_DIR}/"
    log_message "Summary: ${OUTPUT_DIR}/scalability_summary_${TIMESTAMP}.md"
    log_message "CSV data: ${OUTPUT_DIR}/scalability_results_${TIMESTAMP}.csv"
    log_message "=========================================="
}

# Run main function
main "$@" 