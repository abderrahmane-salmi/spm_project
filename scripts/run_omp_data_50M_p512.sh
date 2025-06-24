#!/bin/bash

INPUT=../data/data_50M_p512.bin
MEMORY=256  # MB

for T in 1 2 4 8 16; do
    echo "Running OpenMP with $T threads on $INPUT"
    ./omp_mergesort benchmark "$INPUT" "$MEMORY" "$T" >> ../results/omp_data_50M_p512.csv
done
