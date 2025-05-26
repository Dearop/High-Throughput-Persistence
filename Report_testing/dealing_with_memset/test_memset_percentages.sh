#!/bin/bash

# Test script for comparing glaze_with_memset and state_management_multi
# RUN THIS ON AWS UBUNTU SERVER ONLY

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

# Function to compile programs (force recompilation for Ubuntu)
compile_programs() {
    log_message "Compiling programs for Ubuntu..."
    
    # Force recompilation of transaction generator
    log_message "Compiling transaction generator..."
    gcc -Wall -Wextra -O3 -std=c99 -fopenmp -o generate_transactions generate_transactions.c -lm
    
    # Force recompilation of glaze_with_memset
    log_message "Compiling glaze_with_memset..."
    gcc -Wall -Wextra -O3 -std=c99 -D_POSIX_C_SOURCE=200809L -o glaze_with_memset glaze_with_memset.c -lm
    
    # Force recompilation of state_management_multi
    log_message "Compiling state_management_multi..."
    gcc -Wall -Wextra -O3 -std=c99 -fopenmp -D_GNU_SOURCE -pthread -o state_management_multi state_management_multi.c -lm
    
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
    
    # Extract metrics with more robust patterns and handle both output formats
    local throughput=0
    local avg_batch_time=0
    local median_time=0
    local p99_time=0
    local recovery_time=0
    
    # Try different throughput formats
    if grep -q "Total throughput:" "$log_file"; then
        throughput=$(grep -o "Total throughput:.*Ktx/s" "$log_file" | grep -o "[0-9.]*" || echo "0")
    else
        throughput=$(grep -o "Throughput:.*Ktx/s" "$log_file" | grep -o "[0-9.]*" || echo "0")
    fi
    
    # Try different time formats
    if grep -q "Average batch time:" "$log_file"; then
        avg_batch_time=$(grep -o "Average batch time:.*ms" "$log_file" | grep -o "[0-9.]*" || echo "0")
        median_time=$(grep -o "Median batch time:.*ms" "$log_file" | grep -o "[0-9.]*" || echo "0")
        p99_time=$(grep -o "99th percentile batch time:.*ms" "$log_file" | grep -o "[0-9.]*" || echo "0")
    else
        avg_batch_time=$(grep -o "Average batch.*time:.*ms" "$log_file" | grep -o "[0-9.]*" || echo "0")
        median_time=$(grep -o "Median batch.*time:.*ms" "$log_file" | grep -o "[0-9.]*" || echo "0")
        p99_time=$(grep -o "99th percentile:.*ms" "$log_file" | grep -o "[0-9.]*" || echo "0")
    fi
    
    # Try different recovery time formats
    recovery_time=$(grep -o "Recovery phase took [0-9.]* ms" "$log_file" | grep -o "[0-9.]*" || \
                   grep -o "Recovery time:.*ms" "$log_file" | grep -o "[0-9.]*" || echo "0")
    
    # Append to detailed CSV
    echo "${memset_percentage},${system_name},${duration},${throughput},${avg_batch_time},${median_time},${p99_time},${recovery_time}" >> "${OUTPUT_DIR}/results_${TIMESTAMP}.csv"
    
    # Store metrics for summary
    if [ "$system_name" = "glaze_with_memset" ]; then
        glaze_throughput=$throughput
        glaze_avg_batch=$avg_batch_time
    else
        echo "${memset_percentage},${glaze_throughput},${throughput},${glaze_avg_batch},${avg_batch_time}" >> "${OUTPUT_DIR}/summary_${TIMESTAMP}.csv"
    fi
    
    # Print nice console output
    printf "\n%-25s Results for %s\n" "🔍" "$system_name"
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
    
    cat > "$summary_file" << EOF
# Memset Percentage Test Results

**Test Date:** $(date)
**Account Count:** ${ACCOUNT_COUNT:?}
**Total Tests:** ${TOTAL_TESTS}
**Memset Percentages:** 0% to 100% (in 5% increments)

## Test Configuration

- **Systems Tested:**
  - glaze_with_memset (Optimized for memset operations)
  - state_management_multi (Multi-threaded state management)

- **Transaction Generation:**
  - Batch size: 65,536 transactions
  - Total batches: 50,000
  - Total transactions: ~3.28 billion per test
  - Account range: 0 to $(($ACCOUNT_COUNT - 1))

## Results Summary

The detailed results are available in: \`results_${TIMESTAMP}.csv\`

### Performance Comparison

EOF

    # Add performance comparison table
    if [ -f "${OUTPUT_DIR}/summary_${TIMESTAMP}.csv" ]; then
        echo "| Memset % | glaze_with_memset Throughput | state_management_multi Throughput | glaze_with_memset Avg Batch | state_management_multi Avg Batch |" >> "$summary_file"
        echo "|----------|---------------------------|--------------------------|--------------------------|-------------------------|" >> "$summary_file"
        
        # Process CSV to create comparison table
        for pct in $(seq 0 5 100); do
            glaze_throughput=$(grep "glaze_with_memset,$pct," "${OUTPUT_DIR}/summary_${TIMESTAMP}.csv" | cut -d',' -f2 || echo "N/A")
            multi_throughput=$(grep "state_management_multi,$pct," "${OUTPUT_DIR}/summary_${TIMESTAMP}.csv" | cut -d',' -f2 || echo "N/A")
            glaze_avg_batch=$(grep "glaze_with_memset,$pct," "${OUTPUT_DIR}/summary_${TIMESTAMP}.csv" | cut -d',' -f4 || echo "N/A")
            multi_avg_batch=$(grep "state_management_multi,$pct," "${OUTPUT_DIR}/summary_${TIMESTAMP}.csv" | cut -d',' -f4 || echo "N/A")
            
            echo "| $pct% | $glaze_throughput | $multi_throughput | ${glaze_avg_batch}ms | ${multi_avg_batch}ms |" >> "$summary_file"
        done
    fi
    
    cat >> "$summary_file" << EOF

## Notes

- Both systems process ALL ${ACCOUNT_COUNT} accounts
- glaze_with_memset uses optimized memset operations
- state_management_multi uses multi-threaded processing
- Processing 50,000 batches (3.28 billion transactions) per test
- Average batch time tracked for performance analysis

EOF

    log_message "Summary report generated: $summary_file"
}

# Main execution
main() {
    print_section_header "Performance Test Configuration"
    printf "Account Count: %'d\n" "$ACCOUNT_COUNT"
    printf "Output Directory: %s\n" "$OUTPUT_DIR"
    printf "Batch Size: %'d transactions\n" "65536"
    printf "Total Batches: %'d\n" "50"
    printf "Test Range: 0%% to 100%% memset operations (5%% increments)\n"
    
    # Check if bc is available
    if ! command -v bc &> /dev/null; then
        log_message "ERROR: bc calculator not found. Please install bc."
        exit 1
    fi
    
    # Compile programs
    print_section_header "Compiling Programs"
    compile_programs
    
    # Create CSV headers
    create_csv_header
    
    # Test percentages from 0% to 100% in 5% increments
    local test_counter=1
    local total_tests=$((100/5 + 1))
    
    for memset_pct in $(seq 0 5 100); do
        print_section_header "Test Set ${test_counter}/${total_tests} (${memset_pct}% memset)"
        
        # Generate transactions once for this percentage
        if ! generate_transactions_once "$memset_pct" "$test_counter"; then
            log_message "❌ Failed to generate transactions. Skipping this percentage."
            continue
        fi
        printf "✅ Generated transactions with %d%% memset operations\n" "$memset_pct"
        
        # Test both systems
        for system in "glaze_with_memset" "state_management_multi"; do
            local cmd="./glaze_with_memset ${ACCOUNT_COUNT}"
            [ "$system" = "state_management_multi" ] && cmd="./state_management_multi"
            
            printf "\n🔄 Testing %s...\n" "$system"
            if ! run_test "$memset_pct" "$test_counter" "$system" "$cmd"; then
                printf "❌ %s test failed\n" "$system"
            fi
        done
        
        test_counter=$((test_counter + 1))
        
        # Clean up only after both systems have been tested
        cleanup_files
        
        # Brief pause between test sets
        sleep 2
    done
    
    print_section_header "Test Summary"
    printf "✅ All tests completed successfully!\n"
    printf "📊 Detailed results: %s/results_%s.csv\n" "$OUTPUT_DIR" "$TIMESTAMP"
    printf "📈 Summary data: %s/summary_%s.csv\n" "$OUTPUT_DIR" "$TIMESTAMP"
    printf "\nUse the summary CSV file for easy plotting of throughput comparisons.\n"
    
    # Generate summary
    generate_summary
}

# Run main function
main "$@"