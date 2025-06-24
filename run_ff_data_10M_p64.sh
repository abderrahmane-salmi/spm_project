#!/bin/bash

INPUT="data/data_10M_p64.bin"
MEMORY=256  # MB
WORKERS=(1 2 4 8 16)

RESULTS_FILE="ff_results_10M_p64.csv"

echo "Filename,Workers,Time(s)" > $RESULTS_FILE

for W in "${WORKERS[@]}"; do
    echo "Running FastFlow with $W workers on $INPUT"

    output=$(./fastflow_mergesort benchmark "$INPUT" "$MEMORY" "$W" | tail -n 1)
    time=$(echo "$output" | grep -oP 'Time=\K[0-9.]+' )

    echo "$INPUT,$W,$time" >> $RESULTS_FILE
done

echo "All done! Results saved to $RESULTS_FILE"
