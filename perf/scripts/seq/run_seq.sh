#!/bin/bash

SCRIPT_DIR="$(dirname "$0")"
cd "$(dirname "$0")/../../.."  # Go to project root

INPUTS=(
    "data/data_10M_p64.bin"
    "data/data_10M_p512.bin"
    "data/data_50M_p64.bin"
    "data/data_50M_p512.bin"
)

MEMORIES=(
    512    # For data_10M_p64.bin
    2048   # For data_10M_p512.bin
    1024   # For data_50M_p64.bin
    10240  # For data_50M_p512.bin
)

RESULTS_FILE="$SCRIPT_DIR/seq_results.csv"
echo "Filename,Memory(MB),Time(s)" > "$RESULTS_FILE"

for i in "${!INPUTS[@]}"; do
    INPUT="${INPUTS[$i]}"
    MEM="${MEMORIES[$i]}"

    echo "Running Sequential on $INPUT with $MEM MB memory"

    output=$(./seq_mergesort benchmark "$INPUT" "$MEM" | tail -n 1)
    time=$(echo "$output" | grep -oP 'Time=\K[0-9.]+')

    echo "$INPUT,$MEM,$time" >> "$RESULTS_FILE"
done

echo "All done! Results saved to $RESULTS_FILE"
