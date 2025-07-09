#!/bin/bash

SCRIPT_DIR="$(dirname "$0")"
cd "$SCRIPT_DIR/../../.."

INPUT="data/data_10M_p64.bin"
PROCS=(1 2 4 8)
MEMORIES=(512 512 512 512)
THREADS=(1 4 16 32)

RESULTS_FILE="$SCRIPT_DIR/mpi_results_10M_p64.csv"
echo "Filename,MPI_Procs,Memory(MB),OMP_Threads,Time(s)" > $RESULTS_FILE

for i in "${!PROCS[@]}"; do
    P=${PROCS[$i]}
    M=${MEMORIES[$i]}
    for T in "${THREADS[@]}"; do
        echo "Running MPI=$P procs, OMP_THREADS=$T, MEM=${M}MB on $INPUT"
        output=$(mpirun -x OMP_NUM_THREADS=$T -x OMP_DISPLAY_AFFINITY=true --bind-to none -np $P ./mpi_mergesort benchmark "$INPUT" "$M" "$T" | tail -n 1)
        time=$(echo "$output" | grep -oP 'Time=\K[0-9.]+')
        echo "$INPUT,$P,$M,$T,$time" >> $RESULTS_FILE
    done
done

echo "Done! Results in $RESULTS_FILE"
