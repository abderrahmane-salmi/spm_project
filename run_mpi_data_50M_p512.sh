#!/bin/bash

INPUT="data/data_50M_p512.bin"
MEMORY=512
THREADS=4
PROCS=4

RESULTS_FILE="mpi_results_50M_p512.csv"

echo "Running MPI with $PROCS procs and $THREADS threads on $INPUT"
output=$(mpirun -np $PROCS ./mpi_mergesort benchmark "$INPUT" "$MEMORY" "$THREADS" | tail -n 1)
time=$(echo "$output" | grep -oP 'Time=\K[0-9.]+')
echo "$INPUT,$PROCS,$THREADS,$time" >> $RESULTS_FILE

echo "Done. Appended result to $RESULTS_FILE"
