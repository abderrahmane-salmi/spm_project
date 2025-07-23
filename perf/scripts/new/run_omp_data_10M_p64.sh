#!/bin/bash

SCRIPT_DIR="$(dirname "$0")"
cd "$SCRIPT_DIR/../../.."

INPUT="data/data_10M_p64.bin"
THREADS=(1 2 4 8 16 32 64)
M=512  # Memory in MB

RESULTS_FILE="$SCRIPT_DIR/omp_results_10M_p64.csv"

echo "Filename,Threads,Memory(MB),Time(s)" > $RESULTS_FILE

for i in "${!THREADS[@]}"; do
    T=${THREADS[$i]}

    echo "Running OpenMP with $T threads and $M MB on $INPUT"
    export OMP_NUM_THREADS=$T

    output=$(srun ./omp_mergesort benchmark "$INPUT" "$M" "$T" | tail -n 1)
    time=$(echo "$output" | grep -oP 'Time=\K[0-9.]+')

    echo "$INPUT,$T,$M,$time" >> $RESULTS_FILE
done

echo "All done! Results saved to $RESULTS_FILE"
