#!/bin/bash

# Script to test recovery times for state_management_parameterized.c
# by iterating through different account numbers and chunk sizes.

# --- Configuration ---
CHUNK_SIZES_KB=(64 128 256 512 1024) # Chunk sizes in Kilobytes
ACCOUNT_COUNTS=(1000000 10000000 50000000 100000000) # Number of accounts
MEMSET_PERCENTAGE="5.0" # Memset percentage for transaction generation
NUM_TEST_ITERATIONS=2     # Number of independent test iterations per (account, chunk) config
OUTPUT_CSV="recovery_times_summary.csv"
GENERATOR_EXEC="./generate_transactions"
STATE_EXEC="./state_management_parameterized"

# --- Pre-flight checks and Compilation (optional, assumes binaries exist) ---
echo "INFO: Ensuring executables are present..."
if [ ! -f "$GENERATOR_EXEC" ]; then
    echo "ERROR: $GENERATOR_EXEC not found. Please compile generate_transactions.c first."
    echo "Attempting to compile generate_transactions.c..."
    gcc generate_transactions.c -o generate_transactions -O3 -fopenmp -std=c11
    if [ $? -ne 0 ]; then
        echo "ERROR: Compilation of generate_transactions.c failed."
        exit 1
    fi
    echo "INFO: Compiled $GENERATOR_EXEC."
fi

if [ ! -f "$STATE_EXEC" ]; then
    echo "ERROR: $STATE_EXEC not found. Please compile state_management_parameterized.c first."
    echo "Attempting to compile state_management_parameterized.c..."
    gcc state_management_parameterized.c -o state_management_parameterized -O3 -lm -pthread -std=c11
    if [ $? -ne 0 ]; then
        echo "ERROR: Compilation of state_management_parameterized.c failed."
        exit 1
    fi
    echo "INFO: Compiled $STATE_EXEC."
fi
echo "INFO: Executables checked."
echo ""

# --- CSV Header ---
echo "chunk_size_kb,account_count,test_iteration,recovery_time_ms" > "$OUTPUT_CSV"

# --- Main Loop ---
for acc_count in "${ACCOUNT_COUNTS[@]}"; do
    echo "======================================================================"
    echo "Processing all chunk sizes for Account Count: ${acc_count}"
    echo "======================================================================"

    # Step 1: Regenerate transactions for the current account count
    echo "INFO: Regenerating transactions for ${acc_count} accounts with ${MEMSET_PERCENTAGE}% memset..."
    echo "Executing: $GENERATOR_EXEC "$MEMSET_PERCENTAGE" "$acc_count""
    "$GENERATOR_EXEC" "$MEMSET_PERCENTAGE" "$acc_count"
    if [ $? -ne 0 ]; then
        echo "ERROR: Failed to generate transactions for ${acc_count} accounts."
        echo "       Skipping all chunk sizes for this account count."
        # Log error to CSV for all chunk sizes for this acc_count
        for chunk_kb_skip in "${CHUNK_SIZES_KB[@]}"; do
            for iteration_id_skip in $(seq 1 "$NUM_TEST_ITERATIONS"); do
                echo "${chunk_kb_skip},${acc_count},${iteration_id_skip},ERROR_TX_GENERATION" >> "$OUTPUT_CSV"
            done
        done
        echo ""
        continue # Skip to the next account count
    else
        echo "INFO: Successfully generated transactions.bin for ${acc_count} accounts."
    fi
    echo ""

    for chunk_kb in "${CHUNK_SIZES_KB[@]}"; do
        echo "----------------------------------------------------------------------"
        echo "Configuration: Account Count = ${acc_count}, Chunk Size = ${chunk_kb}KB"
        echo "----------------------------------------------------------------------"

        # Step 2: Perform NUM_TEST_ITERATIONS
        for iteration_id in $(seq 1 "$NUM_TEST_ITERATIONS"); do
            echo "INFO: Starting Test Iteration ${iteration_id}/${NUM_TEST_ITERATIONS}"

            # 2a: Cleanup logs before the priming run
            echo "  Cleaning up old log files: checkpoint_log.dat, state_hash.dat"
            rm -f checkpoint_log.dat state_hash.dat

            # 2b: Priming run (to populate checkpoint_log.dat)
            echo "  Iteration ${iteration_id} - Step 1: Priming run (populating log)..."
            prime_run_output_file="prime_output_a${acc_count}_c${chunk_kb}_i${iteration_id}.txt"
            "$STATE_EXEC" "$acc_count" "$chunk_kb" > "$prime_run_output_file"
            if [ $? -ne 0 ]; then
                echo "  ERROR: Priming run of $STATE_EXEC failed."
                echo "${chunk_kb},${acc_count},${iteration_id},ERROR_PRIME_RUN" >> "$OUTPUT_CSV"
                cat "$prime_run_output_file" # Print output for debugging
                continue # Skip to the next test_iteration
            fi
            echo "  Priming run complete. Log should be populated."

            # 2c: Measurement run (to recover from the populated log)
            echo "  Iteration ${iteration_id} - Step 2: Measurement run (recovering from log)..."
            measurement_run_output_file="measure_output_a${acc_count}_c${chunk_kb}_i${iteration_id}.txt"
            RUN_OUTPUT=$("$STATE_EXEC" "$acc_count" "$chunk_kb" 2>&1 | tee "$measurement_run_output_file")
            
            if [ $? -ne 0 ]; then
                echo "  ERROR: Measurement run of $STATE_EXEC failed."
                echo "${chunk_kb},${acc_count},${iteration_id},ERROR_MEASURE_RUN" >> "$OUTPUT_CSV"
            fi

            # 2d: Extract recovery time
            RECOVERY_TIME_MS=$(echo "$RUN_OUTPUT" | grep "Recovery phase took" | awk '{print $4}')

            if [ -z "$RECOVERY_TIME_MS" ]; then
                RECOVERY_TIME_MS="NOT_FOUND"
                echo "  WARNING: Could not parse recovery time from measurement run output."
                echo "           Output for this run was saved to $measurement_run_output_file"
            else
                echo "  INFO: Recovery Time: ${RECOVERY_TIME_MS} ms"
            fi
            
            echo "${chunk_kb},${acc_count},${iteration_id},${RECOVERY_TIME_MS}" >> "$OUTPUT_CSV"
            echo "INFO: Test Iteration ${iteration_id} complete."
            echo "" # Newline for readability
        done # End of NUM_TEST_ITERATIONS loop
        echo ""
    done # End of CHUNK_SIZES_KB loop
    echo ""
done # End of ACCOUNT_COUNTS loop

echo "======================================================================"
echo "All tests complete. Results saved to ${OUTPUT_CSV}"
echo "======================================================================"

exit 0 