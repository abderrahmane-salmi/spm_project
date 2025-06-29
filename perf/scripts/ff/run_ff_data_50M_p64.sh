#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"  # Absolute path of script dir
INPUT="$SCRIPT_DIR/../../../data/data_50M_p64.bin"
WORKERS=(1 2 4 8 16)
RESULTS_FILE="$SCRIPT_DIR/ff_results_50M_p64.csv"

get_memory_budget() {
    local file="$1"
    local workers="$2"

    case "$file" in
        *data_10M_p64.bin)
            case $workers in
                1) echo 512 ;; 2) echo 256 ;; 4) echo 128 ;; 8) echo 64 ;; 16) echo 32 ;;
            esac ;;
        *data_10M_p512.bin)
            case $workers in
                1) echo 1024 ;; 2) echo 512 ;; 4) echo 256 ;; 8) echo 128 ;; 16) echo 64 ;;
            esac ;;
        *data_50M_p64.bin)
            case $workers in
                1) echo 1024 ;; 2) echo 512 ;; 4) echo 256 ;; 8) echo 128 ;; 16) echo 64 ;;
            esac ;;
        *data_50M_p512.bin)
            case $workers in
                1) echo 1024 ;; 2) echo 1024 ;; 4) echo 512 ;; 8) echo 256 ;; 16) echo 128 ;;
            esac ;;
        *)
            echo "Unknown file for memory allocation" >&2
            exit 1
    esac
}

echo "Filename,Workers,Memory(MB),Time(s)" > "$RESULTS_FILE"

for W in "${WORKERS[@]}"; do
    MEMORY=$(get_memory_budget "$INPUT" "$W")
    echo "Running FastFlow with $W workers, $MEMORY MB on $INPUT"

    output=$(../../../ff_mergesort benchmark "$INPUT" "$MEMORY" "$W" | tail -n 1)
    time=$(echo "$output" | grep -oP 'Time=\K[0-9.]+' )

    echo "$INPUT,$W,$MEMORY,$time" >> "$RESULTS_FILE"
done

echo "All done! Results saved to $RESULTS_FILE"
