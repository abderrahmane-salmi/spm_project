#!/bin/bash

INPUT=data/data_50M_p512.bin
MEM_BUDGET=256
THREADS=4
RESULTS=mpi_results_50M_p512.csv

echo "Filename,MPI_Procs,OMP_Threads,Time(s)" > $RESULTS

for procs in 1 2 4 8; do
    echo "Running MPI with $procs procs and $THREADS threads on $INPUT"
    mpirun -np $procs ./mpi_mergesort benchmark $INPUT $MEM_BUDGET $THREADS 2> mpi_error_p${procs}.log | tee -a $RESULTS
done

echo "Done. Results in $RESULTS"
