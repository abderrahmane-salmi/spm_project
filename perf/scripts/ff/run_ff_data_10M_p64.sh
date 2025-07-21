#!/bin/bash

SCRIPT_DIR="$(dirname "$0")"   # folder where the script is
cd "$(dirname "$0")/../../.." # go to project root (for relative INPUT path)

INPUT="data/data_10M_p64.bin"
WORKERS=(1 2 4 8 16 32 64)
M=512  # Memory in MB

RESULTS_FILE="$SCRIPT_DIR/ff_results_10M_p64.csv"

echo "Filename,Workers,Memory(MB),Time(s)" > $RESULTS_FILE

for i in "${!WORKERS[@]}"; do
    W=${WORKERS[$i]}

    echo "Running FastFlow with $W workers and $M MB memory on $INPUT"

    output=$(./ff_mergesort benchmark "$INPUT" "$M" "$W" | tail -n 1)
    time=$(echo "$output" | grep -oP 'Time=\K[0-9.]+' )

    echo "$INPUT,$W,$M,$time" >> $RESULTS_FILE
done

echo "All done! Results saved to $RESULTS_FILE"
