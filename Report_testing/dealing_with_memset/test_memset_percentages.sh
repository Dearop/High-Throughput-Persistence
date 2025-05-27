#!/bin/bash

# Test script for comparing glaze_with_memset and state_management_multi
# RUN THIS ON AWS UBUNTU SERVER ONLY

set -e  # Exit on any error

# Configuration
ACCOUNT_COUNT=5000000
OUTPUT_DIR="memset_test_results"
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
LOG_FILE="${OUTPUT_DIR}/test_log_${TIMESTAMP}.txt"

# Define the exponential memset percentages
MEMSET_PERCENTAGES=(0 1 2 4 8 16 32 64 100)

# Create output directory
mkdir -p "$OUTPUT_DIR"

# Function to log messages
log_message() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

# Function to compile programs (force recompilation for Ubuntu)
compile_programs() {
    log_message "Compiling programs for Ubuntu..."
    
    # Force recompilation of transaction generator
    log_message "Compiling transaction generator..."
    gcc -Wall -Wextra -O3 -std=c99 -fopenmp -o generate_transactions generate_transactions.c -lm
    
    # Force recompilation of glaze_with_memset
    log_message "Compiling glaze_with_memset..."
    gcc -Wall -Wextra -O3 -std=c99 -D_POSIX_C_SOURCE=200809L -o glaze_with_memset glaze_with_memset.c -lm -pthread
    
    # Force recompilation of state_management_multi
    log_message "Compiling state_management_multi..."
    gcc -Wall -Wextra -O3 -std=c99 -fopenmp -D_GNU_SOURCE -pthread -o state_management_multi state_management_multi.c -lm
    
    log_message "All programs compiled successfully for Ubuntu."
}

# Function to clean up files
cleanup_files() {
    log_message "Cleaning up temporary files (transactions.bin, checkpoint_log.dat, etc.)..."
    rm -f transactions.bin
    rm -f checkpoint_log.dat state_hash.dat reconstructed_state.txt
    rm -f state_management_output.txt
    # Remove any other .bin, .dat, .txt files that might be lingering from previous tests
    # Be careful with broad rm commands; ensuring this is scoped to what's expected.
    # For now, keeping it as is from previous version, assuming specific file names cover it.
    # rm -f *.bin *.dat *.txt 2>/dev/null || true 
}

# Function to generate transactions
generate_transactions_once() {
    local memset_percentage=$1
    local test_num=$2
    
    log_message "Generating transactions with ${memset_percentage}% memset operations..."
    ./generate_transactions "$memset_percentage" "$ACCOUNT_COUNT" > "${OUTPUT_DIR}/generation_${memset_percentage}pct_${TIMESTAMP}.log" 2>&1
    
    if [ ! -f "transactions.bin" ]; then
        log_message "ERROR: Transaction generation failed for ${memset_percentage}%"
        return 1
    fi
    return 0
}

# Function to run a single test
run_test() {
    local memset_percentage=$1
    local test_num=$2
    local system_name=$3
    local command=$4
    
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
}

# Function to create CSV header
create_csv_header() {
    # Create a clean CSV file with headers
    echo "Memset%,System,TotalTime_s,Throughput_Ktx/s,AvgBatchTime_ms,MedianBatchTime_ms,P99BatchTime_ms,RecoveryTime_ms" > "${OUTPUT_DIR}/results_${TIMESTAMP}.csv"
    
    # Create a summary CSV for easier plotting
    echo "Memset%,GlazeWithMemset_Throughput,StateManagementMulti_Throughput,GlazeWithMemset_AvgBatch,StateManagementMulti_AvgBatch" > "${OUTPUT_DIR}/summary_${TIMESTAMP}.csv"
}

# Function to print section header
print_section_header() {
    local title="$1"
    local width=80
    local padding=$(( (width - ${#title}) / 2 ))
    printf "\n%${width}s\n" | tr ' ' '='
    printf "%${padding}s%s%${padding}s\n" "" "$title" ""
    printf "%${width}s\n" | tr ' ' '='
}

# Function to extract metrics from output logs
extract_metrics() {
    local log_file=$1
    local system_name=$2
    local memset_percentage=$3
    local duration=$4
    
    local throughput=0
    local avg_batch_time=0
    local median_time=0
    local p99_time=0
    local recovery_time=0
    
    # Extract throughput
    # glaze_with_memset: "Total throughput:       X Ktx/s"
    # state_management_multi: potentially "Throughput: X Ktx/s" (fallback)
    throughput=$(grep "Total throughput:" "$log_file" | grep -o '[0-9.]\\+' || \
                 grep "Throughput:" "$log_file" | grep -o '[0-9.]\\+' || echo "0")
    
    # Extract batch times
    # glaze_with_memset:
    # "Average batch time:     X ms"
    # "Median batch time:      X ms"
    # "99th percentile:        X ms"
    # General patterns for fallback if needed for state_management_multi
    avg_batch_time=$(grep "Average batch time:" "$log_file" | grep -o '[0-9.]\\+' || \
                     grep "Average batch.*time:" "$log_file" | grep -o '[0-9.]\\+' || echo "0")
    
    median_time=$(grep "Median batch time:" "$log_file" | grep -o '[0-9.]\\+' || \
                  grep "Median batch.*time:" "$log_file" | grep -o '[0-9.]\\+' || echo "0")

    p99_time=$(grep "99th percentile:" "$log_file" | grep -o '[0-9.]\\+' || echo "0")
    # If "99th percentile:" is not found (value is 0), try specific "99th percentile batch time:" for compatibility
    if [ "$(echo "$p99_time == 0" | bc -l)" -eq 1 ] && grep -q "99th percentile batch time:" "$log_file"; then
        p99_time=$(grep "99th percentile batch time:" "$log_file" | grep -o '[0-9.]\\+' || echo "0")
    fi
        
    # Try different recovery time formats (remains as is, glaze_with_memset does not print this explicitly)
    recovery_time=$(grep -o "Recovery phase took [0-9.]* ms" "$log_file" | grep -o "[0-9.]*" || \
                   grep -o "Recovery time:.*ms" "$log_file" | grep -o "[0-9.]*" || echo "0")
    
    # Append to detailed CSV
    echo "${memset_percentage},${system_name},${duration},${throughput},${avg_batch_time},${median_time},${p99_time},${recovery_time}" >> "${OUTPUT_DIR}/results_${TIMESTAMP}.csv"
    
    # Store metrics for summary
    # This logic assumes glaze_with_memset runs first for a given percentage, then state_management_multi
    if [ "$system_name" = "glaze_with_memset" ]; then
        glaze_throughput=$throughput
        glaze_avg_batch=$avg_batch_time
    else
        echo "${memset_percentage},${glaze_throughput},${throughput},${glaze_avg_batch},${avg_batch_time}" >> "${OUTPUT_DIR}/summary_${TIMESTAMP}.csv"
    fi
    
    # Print nice console output
    printf "\n%-25s Results for %s (%s%% memset)\n" "🔍" "$system_name" "$memset_percentage"
    printf "%-25s %.2f Ktx/s\n" "Throughput:" "$throughput"
    printf "%-25s %.2f ms\n" "Avg Batch Time:" "$avg_batch_time"
    printf "%-25s %.2f ms\n" "Median Batch Time:" "$median_time"
    printf "%-25s %.2f ms\n" "P99 Batch Time:" "$p99_time"
    printf "%-25s %.2f ms\n" "Recovery Time:" "$recovery_time"
    printf "%-25s %.2f s\n" "Total Run Time:" "$duration"
}

# Function to generate summary report
generate_summary() {
    log_message "Generating summary report..."
    
    local summary_file="${OUTPUT_DIR}/test_summary_${TIMESTAMP}.md"
    local num_percentages=${#MEMSET_PERCENTAGES[@]}
    local percentage_list_string=$(IFS=, ; echo "${MEMSET_PERCENTAGES[*]}") # Comma-separated list
    
    cat > "$summary_file" << EOF
# Memset Percentage Test Results

**Test Date:** $(date)
**Account Count:** ${ACCOUNT_COUNT}
**Total Test Sets (Percentages):** ${num_percentages}
**Memset Percentages Tested:** ${percentage_list_string}%

## Test Configuration

- **Systems Tested:**
  - glaze_with_memset (Optimized for memset operations, asynchronous checkpointing)
  - state_management_multi (Multi-threaded state management)

- **Transaction Generation Details (per test set):**
  - Target Account Count: ${ACCOUNT_COUNT}
  - Memset operations varied: ${percentage_list_string}%

- **glaze_with_memset Processing Details (per run):**
  - Batch size: 65,536 transactions (1 << 16)
  - Total batches processed: 5,000 
  - Total transactions processed: ~328 million (5000 * 65536)

## Results Summary

The detailed results are available in: \`results_${TIMESTAMP}.csv\`
The summary data for plotting is in: \`summary_${TIMESTAMP}.csv\`

### Performance Comparison (Throughput Ktx/s and Average Batch Time ms)

EOF

    # Add performance comparison table
    if [ -f "${OUTPUT_DIR}/summary_${TIMESTAMP}.csv" ]; then
        echo "| Memset % | glaze_with_memset Throughput (Ktx/s) | state_management_multi Throughput (Ktx/s) | glaze_with_memset Avg Batch (ms) | state_management_multi Avg Batch (ms) |" >> "$summary_file"
        echo "|----------|---------------------------------------|------------------------------------------|-----------------------------------|--------------------------------------|" >> "$summary_file"
        
        # Process CSV to create comparison table
        for pct in "${MEMSET_PERCENTAGES[@]}"; do
            # Read the line corresponding to the percentage from summary.csv
            local data_line=$(grep "^${pct}," "${OUTPUT_DIR}/summary_${TIMESTAMP}.csv" || echo "$pct,N/A,N/A,N/A,N/A")
            
            local glaze_thr=$(echo "$data_line" | cut -d',' -f2)
            local multi_thr=$(echo "$data_line" | cut -d',' -f3)
            local glaze_avg_b=$(echo "$data_line" | cut -d',' -f4)
            local multi_avg_b=$(echo "$data_line" | cut -d',' -f5)
            
            echo "| $pct% | $glaze_thr | $multi_thr | $glaze_avg_b | $multi_avg_b |" >> "$summary_file"
        done
    else
        echo "Summary CSV file not found. Table could not be generated." >> "$summary_file"
    fi
    
    cat >> "$summary_file" << EOF

## Notes

- Throughput is measured in Kilo-transactions per second (Ktx/s).
- Average Batch Time is measured in milliseconds (ms).
- glaze_with_memset processes 5,000 batches as per its internal configuration.
- Ensure \`generate_transactions\` creates sufficient data for all tests.

EOF

    log_message "Summary report generated: $summary_file"
}

# Main execution
main() {
    print_section_header "Performance Test Configuration"
    printf "Account Count: %'d\n" "$ACCOUNT_COUNT"
    printf "Output Directory: %s\n" "$OUTPUT_DIR"
    printf "Batch Size (as per glaze_with_memset): %'d transactions\n" "65536"
    printf "Total Batches Processed by glaze_with_memset: %'d\n" "5000"
    local percentage_list_string=$(IFS=, ; echo "${MEMSET_PERCENTAGES[*]}")
    printf "Test Memset Percentages: %s%%\n" "$percentage_list_string"
    
    # Check if bc is available
    if ! command -v bc &> /dev/null; then
        log_message "ERROR: bc calculator not found. Please install bc."
        exit 1
    fi
    
    # Compile programs
    print_section_header "Compiling Programs"
    compile_programs

    # Initial cleanup before any tests start
    print_section_header "Initial Workspace Cleanup"
    cleanup_files
    log_message "Initial cleanup complete."
    
    # Create CSV headers
    create_csv_header
    
    local test_counter=1
    local total_tests_to_run=${#MEMSET_PERCENTAGES[@]}
    
    for memset_pct in "${MEMSET_PERCENTAGES[@]}"; do
        print_section_header "Test Set ${test_counter}/${total_tests_to_run} (${memset_pct}% memset)"
        
        # Generate transactions once for this percentage
        if ! generate_transactions_once "$memset_pct" "$test_counter"; then
            log_message "❌ Failed to generate transactions for ${memset_pct}%. Skipping this percentage."
            continue
        fi
        printf "✅ Generated transactions with %d%% memset operations\n" "$memset_pct"
        
        # Test both systems
        for system in "glaze_with_memset" "state_management_multi"; do
            local cmd="./glaze_with_memset ${ACCOUNT_COUNT}"
            # state_management_multi command does not take ACCOUNT_COUNT as argument in this script.
            # Assuming it uses an internal or different configuration method for account count.
            [ "$system" = "state_management_multi" ] && cmd="./state_management_multi" 
            
            printf "\n🔄 Testing %s with %s%% memset...\n" "$system" "$memset_pct"
            if ! run_test "$memset_pct" "$test_counter" "$system" "$cmd"; then
                printf "❌ %s test failed for %s%% memset\n" "$system" "$memset_pct"
            fi
        done
        
        test_counter=$((test_counter + 1))
        
        # Clean up only after both systems have been tested for a given percentage
        cleanup_files
        
        # Brief pause between test sets
        log_message "Pausing for 2 seconds before next test set..."
        sleep 2
    done
    
    print_section_header "Test Summary"
    # Generate summary report at the end of all tests
    generate_summary

    printf "✅ All test sets completed.\n"
    printf "📊 Detailed results: %s/results_%s.csv\n" "$OUTPUT_DIR" "$TIMESTAMP"
    printf "📈 Summary data for plotting: %s/summary_%s.csv\n" "$OUTPUT_DIR" "$TIMESTAMP"
    printf "📄 Markdown summary report: %s/test_summary_%s.md\n" "$OUTPUT_DIR" "$TIMESTAMP"
    printf "\nUse the summary CSV file for easy plotting of throughput comparisons.\n"
}

# Run main function
main "$@"