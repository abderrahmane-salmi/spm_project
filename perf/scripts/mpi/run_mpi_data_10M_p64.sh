#!/bin/bash

INPUT="data/data_10M_p64.bin"
MEMORY=512
PROCS=(1 2 4 8)
THREADS=(1 4 16 32 64 128)

RESULTS_FILE="mpi_results_10M_p64.csv"
echo "Filename,MPI_Procs,OMP_Threads,Time(s)" > $RESULTS_FILE

for P in "${PROCS[@]}"; do
    for T in "${THREADS[@]}"; do
        echo "Running MPI with $P procs and $T threads on $INPUT"
        output=$(OMP_NUM_THREADS=$T mpirun -np $P ./mpi_mergesort benchmark "$INPUT" "$MEMORY" "$T" | tail -n 1)
        time=$(echo "$output" | grep -oP 'Time=\K[0-9.]+')
        echo "$INPUT,$P,$T,$time" >> $RESULTS_FILE
    done
done

echo "Done for $INPUT. Results in $RESULTS_FILE"
