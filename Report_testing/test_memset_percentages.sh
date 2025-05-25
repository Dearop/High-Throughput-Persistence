#!/bin/bash

# Test script for comparing breaking_glaze_with_memset_unlimited and state_management_parameterized
# with different memset percentages from 0% to 100%

set -e  # Exit on any error

# Configuration
ACCOUNT_COUNT=5000000
TOTAL_TESTS=20
OUTPUT_DIR="memset_test_results"
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
LOG_FILE="${OUTPUT_DIR}/test_log_${TIMESTAMP}.txt"

# Create output directory
mkdir -p "$OUTPUT_DIR"

# Function to log messages
log_message() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

# Function to compile programs if needed
compile_programs() {
    log_message "Compiling programs..."
    
    # Compile transaction generator
    if [ ! -f "generate_transactions" ] || [ "generate_transactions.c" -nt "generate_transactions" ]; then
        log_message "Compiling transaction generator..."
        gcc -Wall -Wextra -O3 -std=c99 -fopenmp -o generate_transactions generate_transactions.c -lm
    fi
    
    # Compile breaking_glaze_with_memset_unlimited
    if [ ! -f "breaking_glaze_with_memset_unlimited" ] || [ "breaking_glaze_with_memset_unlimited.c" -nt "breaking_glaze_with_memset_unlimited" ]; then
        log_message "Compiling breaking_glaze_with_memset_unlimited..."
        gcc -Wall -Wextra -O3 -std=c99 -D_POSIX_C_SOURCE=200809L -o breaking_glaze_with_memset_unlimited breaking_glaze_with_memset_unlimited.c -lm
    fi
    
    # Compile state_management_parameterized
    if [ ! -f "state_management_parameterized" ] || [ "state_management_parameterized.c" -nt "state_management_parameterized" ]; then
        log_message "Compiling state_management_parameterized..."
        gcc -Wall -Wextra -O3 -std=c99 -fopenmp -D_POSIX_C_SOURCE=200809L -o state_management_parameterized state_management_parameterized.c -lm
    fi
    
    log_message "All programs compiled successfully."
}

# Function to clean up files
cleanup_files() {
    log_message "Cleaning up temporary files..."
    rm -f transactions.bin
    rm -f checkpoint_log.dat state_hash.dat reconstructed_state.txt
    rm -f state_management_output.txt
    rm -f *.bin *.dat *.txt 2>/dev/null || true
}

# Function to run a single test
run_test() {
    local memset_percentage=$1
    local test_num=$2
    local system_name=$3
    local command=$4
    
    log_message "=== Test ${test_num}/20: ${system_name} with ${memset_percentage}% memset ==="
    
    # Generate transactions for this percentage
    log_message "Generating transactions with ${memset_percentage}% memset operations..."
    ./generate_transactions "$memset_percentage" > "${OUTPUT_DIR}/generation_${system_name}_${memset_percentage}pct_${TIMESTAMP}.log" 2>&1
    
    if [ ! -f "transactions.bin" ]; then
        log_message "ERROR: Transaction generation failed for ${memset_percentage}%"
        return 1
    fi
    
    # Run the system
    log_message "Running ${system_name}..."
    local start_time=$(date +%s.%N)
    
    if eval "$command" > "${OUTPUT_DIR}/${system_name}_${memset_percentage}pct_${TIMESTAMP}.log" 2>&1; then
        local end_time=$(date +%s.%N)
        local duration=$(echo "$end_time - $start_time" | bc -l)
        log_message "${system_name} completed successfully in ${duration} seconds"
        
        # Extract key metrics from the output
        extract_metrics "${OUTPUT_DIR}/${system_name}_${memset_percentage}pct_${TIMESTAMP}.log" "$system_name" "$memset_percentage" "$duration"
    else
        log_message "ERROR: ${system_name} failed for ${memset_percentage}% memset"
        return 1
    fi
    
    # Clean up for next test
    cleanup_files
}

# Function to extract metrics from output logs
extract_metrics() {
    local log_file=$1
    local system_name=$2
    local memset_percentage=$3
    local duration=$4
    
    # Extract throughput, success rate, and timing information
    local throughput=$(grep -o "Throughput: [0-9.]* tx/sec" "$log_file" | grep -o "[0-9.]*" || echo "N/A")
    local success_rate=$(grep -o "Success rate: [0-9.]*%" "$log_file" | grep -o "[0-9.]*" || echo "N/A")
    local total_tx=$(grep -o "Total transactions: [0-9]*" "$log_file" | grep -o "[0-9]*" || echo "N/A")
    local avg_batch_time=$(grep -o "Average batch time: [0-9.]* ms" "$log_file" | grep -o "[0-9.]*" || echo "N/A")
    local median_time=$(grep -o "Median batch time: [0-9.]* ms" "$log_file" | grep -o "[0-9.]*" || echo "N/A")
    local p99_time=$(grep -o "99th percentile batch time: [0-9.]* ms" "$log_file" | grep -o "[0-9.]*" || echo "N/A")
    
    # Append to CSV results file
    echo "${system_name},${memset_percentage},${duration},${throughput},${success_rate},${total_tx},${avg_batch_time},${median_time},${p99_time}" >> "${OUTPUT_DIR}/results_summary_${TIMESTAMP}.csv"
}

# Function to create CSV header
create_csv_header() {
    echo "System,Memset_Percentage,Total_Duration_Sec,Throughput_TxPerSec,Success_Rate_Percent,Total_Transactions,Avg_Batch_Time_Ms,Median_Batch_Time_Ms,P99_Batch_Time_Ms" > "${OUTPUT_DIR}/results_summary_${TIMESTAMP}.csv"
}

# Function to generate summary report
generate_summary() {
    log_message "Generating summary report..."
    
    local summary_file="${OUTPUT_DIR}/test_summary_${TIMESTAMP}.md"
    
    cat > "$summary_file" << EOF
# Memset Percentage Test Results

**Test Date:** $(date)
**Account Count:** ${ACCOUNT_COUNT:?}
**Total Tests:** ${TOTAL_TESTS}
**Memset Percentages:** 0% to 100% (in 5% increments)

## Test Configuration

- **Systems Tested:**
  - breaking_glaze_with_memset_unlimited (Ring-based recovery, ALL ${ACCOUNT_COUNT} accounts)
  - state_management_parameterized (Full ${ACCOUNT_COUNT} accounts)

- **Transaction Generation:**
  - Batch size: 65,536 transactions
  - Total batches: 125,000
  - Total transactions: ~8.2 billion per test
  - Account range: 0 to $(($ACCOUNT_COUNT - 1))

## Results Summary

The detailed results are available in: \`results_summary_${TIMESTAMP}.csv\`

### Key Findings

EOF

    # Add some basic analysis
    if [ -f "${OUTPUT_DIR}/results_summary_${TIMESTAMP}.csv" ]; then
        echo "### Performance Comparison" >> "$summary_file"
        echo "" >> "$summary_file"
        echo "| Memset % | breaking_glaze Throughput | parameterized Throughput | breaking_glaze Success % | parameterized Success % |" >> "$summary_file"
        echo "|----------|---------------------------|--------------------------|--------------------------|-------------------------|" >> "$summary_file"
        
        # Process CSV to create comparison table
        for pct in $(seq 0 5 100); do
            bg_throughput=$(grep "breaking_glaze_with_memset_unlimited,$pct," "${OUTPUT_DIR}/results_summary_${TIMESTAMP}.csv" | cut -d',' -f4 || echo "N/A")
            param_throughput=$(grep "state_management_parameterized,$pct," "${OUTPUT_DIR}/results_summary_${TIMESTAMP}.csv" | cut -d',' -f4 || echo "N/A")
            bg_success=$(grep "breaking_glaze_with_memset_unlimited,$pct," "${OUTPUT_DIR}/results_summary_${TIMESTAMP}.csv" | cut -d',' -f5 || echo "N/A")
            param_success=$(grep "state_management_parameterized,$pct," "${OUTPUT_DIR}/results_summary_${TIMESTAMP}.csv" | cut -d',' -f5 || echo "N/A")
            
            echo "| $pct% | $bg_throughput | $param_throughput | $bg_success% | $param_success% |" >> "$summary_file"
        done
    fi
    
    cat >> "$summary_file" << EOF

## Files Generated

- \`test_log_${TIMESTAMP}.txt\` - Complete test execution log
- \`results_summary_${TIMESTAMP}.csv\` - Machine-readable results
- Individual test logs: \`{system}_{percentage}pct_${TIMESTAMP}.log\`
- Transaction generation logs: \`generation_{system}_{percentage}pct_${TIMESTAMP}.log\`

## Notes

- Both systems now process ALL ${ACCOUNT_COUNT} accounts (no artificial limitations)
- breaking_glaze_with_memset_unlimited uses ring-based checkpointing with dynamic sizing
- state_management_parameterized uses append-only logging with asynchronous snapshots
- All timing measurements include transaction processing, logging, and checkpointing overhead
- Success rates should be much higher now that account range limitations are removed

EOF

    log_message "Summary report generated: $summary_file"
}

# Main execution
main() {
    log_message "Starting memset percentage comparison test"
    log_message "Account count: ${ACCOUNT_COUNT}"
    log_message "Output directory: ${OUTPUT_DIR}"
    
    # Check if bc is available for floating point arithmetic
    if ! command -v bc &> /dev/null; then
        log_message "ERROR: bc calculator not found. Please install bc for timing calculations."
        exit 1
    fi
    
    # Compile programs
    compile_programs
    
    # Create CSV header
    create_csv_header
    
    # Test percentages from 0% to 100% in 5% increments (20 tests total)
    local test_counter=1
    
    for memset_pct in $(seq 0 5 100); do
        log_message ""
        log_message "=========================================="
        log_message "Starting test set ${test_counter}/20 with ${memset_pct}% memset"
        log_message "=========================================="
        
        # Test breaking_glaze_with_memset_unlimited
        if ! run_test "$memset_pct" "$test_counter" "breaking_glaze_with_memset_unlimited" "./breaking_glaze_with_memset_unlimited ${ACCOUNT_COUNT}"; then
            log_message "WARNING: breaking_glaze_with_memset_unlimited test failed for ${memset_pct}%"
        fi
        
        # Test state_management_parameterized
        if ! run_test "$memset_pct" "$test_counter" "state_management_parameterized" "./state_management_parameterized ${ACCOUNT_COUNT}"; then
            log_message "WARNING: state_management_parameterized test failed for ${memset_pct}%"
        fi
        
        test_counter=$((test_counter + 1))
        
        # Brief pause between test sets
        sleep 2
    done
    
    # Generate summary
    generate_summary
    
    log_message ""
    log_message "=========================================="
    log_message "All tests completed!"
    log_message "Results saved in: ${OUTPUT_DIR}/"
    log_message "Summary: ${OUTPUT_DIR}/test_summary_${TIMESTAMP}.md"
    log_message "CSV data: ${OUTPUT_DIR}/results_summary_${TIMESTAMP}.csv"
    log_message "=========================================="
}

# Run main function
main "$@" 