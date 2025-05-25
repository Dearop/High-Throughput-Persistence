# Function to compile programs if needed
compile_programs() {
    log_message "Compiling programs..."
    
    # Compile transaction generator (force recompilation)
    log_message "Compiling transaction generator..."
    gcc -Wall -Wextra -O3 -std=c99 -fopenmp -o generate_transactions generate_transactions.c -lm
    
    # Compile breaking_glaze_with_memset_unlimited (force recompilation)
    log_message "Compiling breaking_glaze_with_memset_unlimited..."
    gcc -Wall -Wextra -O3 -std=c99 -D_POSIX_C_SOURCE=200809L -o breaking_glaze_with_memset_unlimited breaking_glaze_with_memset_unlimited.c -lm
    
    # Compile state_management_parameterized (force recompilation)
    log_message "Compiling state_management_parameterized..."
    gcc -Wall -Wextra -O3 -std=c99 -fopenmp -D_GNU_SOURCE -o state_management_parameterized state_management_parameterized.c -lm
    
    log_message "All programs compiled successfully."
} 