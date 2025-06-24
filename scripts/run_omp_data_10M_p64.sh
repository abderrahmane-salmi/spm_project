#!/bin/bash

INPUT=../data/data_10M_p64.bin
MEMORY=256  # MB

for T in 1 2 4 8 16; do
    echo "Running OpenMP with $T threads on $INPUT"
    ./omp_mergesort benchmark "$INPUT" "$MEMORY" "$T" >> ../results/omp_data_10M_p64.csv
done
