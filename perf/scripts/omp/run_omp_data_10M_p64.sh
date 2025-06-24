#!/bin/bash

INPUT="data/data_10M_p64.bin"
MEMORY=256  # MB
THREADS=(1 2 4 8 16)

# Output file for results
RESULTS_FILE="omp_results_10M_p64.csv"

# CSV header
echo "Filename,Threads,Time(s)" > $RESULTS_FILE

for T in "${THREADS[@]}"; do
    echo "Running OpenMP with $T threads on $INPUT"
    export OMP_NUM_THREADS=$T

    # Run sort, capture only the last line with timing
    output=$(./omp_mergesort benchmark "$INPUT" "$MEMORY" "$T" | tail -n 1)

    # ./omp_mergesort benchmark "$INPUT" "$MEMORY" "$T" >> results/omp_data_10M_p64.csv

    # Extract time from output
    time=$(echo "$output" | grep -oP 'Time=\K[0-9.]+')

    # Append to CSV
    echo "$INPUT,$T,$time" >> $RESULTS_FILE
done

echo "All done! Results saved to $RESULTS_FILE"