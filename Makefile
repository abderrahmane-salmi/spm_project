# Makefile for SPM Project 1 - Distributed Out-of-Core MergeSort

CXX = g++
CXXFLAGS = -std=c++17 -O3 -Wall -Wextra

# FastFlow installation path
FF_PATH = ./fastflow
FF_INCLUDES = -I$(FF_PATH)

# OpenMP flags
OMP_FLAGS = -fopenmp

# Directories
SRC_DIR = src
INCLUDE_DIR = $(SRC_DIR)/include
OMP_DIR = $(SRC_DIR)/openmp
FF_DIR = $(SRC_DIR)/fastflow
FILEGEN_DIR = $(SRC_DIR)/filegen

# Output binaries
OMP_BIN = omp_mergesort
FF_BIN = ff_mergesort
FILEGEN_BIN = filegen

# Includes
INCLUDES = -I$(INCLUDE_DIR)

.PHONY: all clean openmp fastflow filegen test quick-test perf-test

all: openmp fastflow filegen

# Build OpenMP binary
openmp: $(OMP_BIN)

$(OMP_BIN): $(OMP_DIR)/omp_main.cpp $(INCLUDE_DIR)/record.hpp $(OMP_DIR)/omp.hpp src/filegen/filegen.cpp src/chunking/chunking.cpp src/merging/merging.cpp
	$(CXX) $(CXXFLAGS) $(OMP_FLAGS) $(INCLUDES) -o $@ $(OMP_DIR)/omp_main.cpp src/filegen/filegen.cpp src/chunking/chunking.cpp src/merging/merging.cpp

# Build FastFlow binary
fastflow: $(FF_BIN)

$(FF_BIN): $(FF_DIR)/ff_main.cpp $(INCLUDE_DIR)/record.hpp $(FF_DIR)/ff.hpp src/filegen/filegen.cpp src/chunking/chunking.cpp src/merging/merging.cpp
	$(CXX) $(CXXFLAGS) $(FF_INCLUDES) $(INCLUDES) -o $@ $(FF_DIR)/ff_main.cpp src/filegen/filegen.cpp src/chunking/chunking.cpp src/merging/merging.cpp -pthread

# Build filegen CLI tool
filegen: $(FILEGEN_BIN)

$(FILEGEN_BIN): $(FILEGEN_DIR)/filegen_main.cpp $(FILEGEN_DIR)/filegen.cpp $(FILEGEN_DIR)/filegen.hpp
	$(CXX) $(CXXFLAGS) $(INCLUDES) -o $@ $(FILEGEN_DIR)/filegen_main.cpp $(FILEGEN_DIR)/filegen.cpp

# Test both implementations
test: openmp fastflow filegen
	@echo "=== Generating input files ==="
	./$(FILEGEN_BIN) gen_count small.bin 1000

	@echo "=== OpenMP Sort ==="
	./$(OMP_BIN) sort small.bin

	@echo "=== FastFlow Sort ==="
	./$(FF_BIN) sort small.bin

	@echo "=== Compare Results ==="
	./$(FILEGEN_BIN) compare small_omp_output.bin small_ff_output.bin

# Test both implementations (large)
test_large: openmp fastflow filegen
	@echo "=== Generating input files ==="
	./$(FILEGEN_BIN) gen_size large.bin 512

	@echo "=== OpenMP Sort ==="
	./$(OMP_BIN) sort large.bin

	@echo "=== FastFlow Sort ==="
	./$(FF_BIN) sort large.bin

	@echo "=== Compare Results ==="
	./$(FILEGEN_BIN) compare large_omp_output.bin large_ff_output.bin

# Quick correctness check
quick-test: openmp fastflow filegen
	@echo "=== Quick Correctness Test ==="
	./$(FILEGEN_BIN) gen_count quick.bin 1000
	./$(OMP_BIN) sort quick.bin
	./$(FF_BIN) sort quick.bin
	./$(FILEGEN_BIN) verify quick_omp_output.bin
	./$(FILEGEN_BIN) verify quick_ff_output.bin
	./$(FILEGEN_BIN) compare quick_omp_output.bin quick_ff_output.bin

# Performance comparison on medium-sized input
perf-test: openmp fastflow filegen
	@echo "=== Performance Comparison ==="
	./$(FILEGEN_BIN) gen_size medium.bin 100
	@echo "--- OpenMP ---"
	./$(OMP_BIN) performance medium.bin
	@echo "--- FastFlow ---"
	./$(FF_BIN) performance medium.bin

# Clean everything
clean:
	rm -f $(OMP_BIN) $(FF_BIN) $(FILEGEN_BIN)
	rm -f *.bin
	rm -rf temp
