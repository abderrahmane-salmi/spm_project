#!/bin/bash

INPUT="data/data_50M_p64.bin"
MEMORY=256
THREADS=4
PROCS=(1 2 4 8)

RESULTS_FILE="mpi_results_50M_p64.csv"
echo "Filename,MPI_Procs,OMP_Threads,Time(s)" > $RESULTS_FILE

for P in "${PROCS[@]}"; do
    echo "Running MPI with $P procs and $THREADS threads on $INPUT"
    output=$(mpirun -np $P ./mpi_mergesort benchmark "$INPUT" "$MEMORY" "$THREADS" | tail -n 1)
    time=$(echo "$output" | grep -oP 'Time=\K[0-9.]+')
    echo "$INPUT,$P,$THREADS,$time" >> $RESULTS_FILE
done

echo "Done. Results in $RESULTS_FILE"
