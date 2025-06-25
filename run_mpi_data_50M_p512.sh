#!/bin/bash

INPUT="data/data_50M_p512.bin"
MEMORY=512
THREADS=4
PROCS=4

RESULTS_FILE="mpi_results_50M_p512.csv"
ERROR_LOG="mpi_error_p4.log"

echo "Running MPI with $PROCS procs and $THREADS threads on $INPUT"

output=$(srun --ntasks=$PROCS --ntasks-per-node=1 --time=00:10:00 \
              --mpi=pmix ./mpi_mergesort benchmark "$INPUT" "$MEMORY" "$THREADS" \
              2> "$ERROR_LOG" | tail -n 1)

time=$(echo "$output" | grep -oP 'Time=\K[0-9.]+')

if [[ -n "$time" ]]; then
    echo "$INPUT,$PROCS,$THREADS,$time" >> $RESULTS_FILE
    echo "✅ Success: Appended result to $RESULTS_FILE"
else
    echo "❌ Error: No time recorded. Check $ERROR_LOG"
fi
