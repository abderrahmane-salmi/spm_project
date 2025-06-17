# Makefile for SPM Project 1 - Distributed Out-of-Core MergeSort

CXX = g++
CXXFLAGS = -std=c++17 -O3 -Wall -Wextra

# FastFlow installation path (adjust as needed)
FF_PATH = ./fastflow
FF_INCLUDES = -I$(FF_PATH)

# OpenMP flags
OMP_FLAGS = -fopenmp

# Source directories
SRC_DIR = src
INCLUDE_DIR = src/include
OMP_DIR = $(SRC_DIR)/openmp
FF_DIR = $(SRC_DIR)/fastflow

# Output binaries
OMP_BIN = omp_mergesort
FF_BIN = ff_mergesort

# Common includes
INCLUDES = -I$(INCLUDE_DIR)

.PHONY: all clean openmp fastflow test

all: openmp fastflow

# OpenMP version
openmp: $(OMP_BIN)

$(OMP_BIN): $(OMP_DIR)/omp_main.cpp $(INCLUDE_DIR)/record.hpp $(OMP_DIR)/omp.hpp
	$(CXX) $(CXXFLAGS) $(OMP_FLAGS) $(INCLUDES) -o $@ $(OMP_DIR)/omp_main.cpp

# FastFlow version
fastflow: $(FF_BIN)

$(FF_BIN): $(FF_DIR)/ff_main.cpp $(INCLUDE_DIR)/record.hpp $(FF_DIR)/ff.hpp
	$(CXX) $(CXXFLAGS) $(FF_INCLUDES) $(INCLUDES) -o $@ $(FF_DIR)/ff_main.cpp -pthread

# Test both versions
test: openmp fastflow
	@echo "=== Testing OpenMP version ==="
	./$(OMP_BIN) generate
	./$(OMP_BIN) basic small_omp.bin
	@echo ""
	@echo "=== Testing FastFlow version ==="
	./$(FF_BIN) generate
	./$(FF_BIN) basic small_ff.bin
	@echo ""
	@echo "=== Comparing outputs ==="
	./$(OMP_BIN) compare output_omp_basic.bin output_ff_basic.bin

# Quick correctness test
quick-test: openmp fastflow
	@echo "=== Quick Correctness Test ==="
	./$(OMP_BIN) generate
	./$(OMP_BIN) basic small_omp.bin
	./$(FF_BIN) basic small_omp.bin
	./$(OMP_BIN) verify output_omp_basic.bin
	./$(FF_BIN) verify output_ff_basic.bin
	./$(OMP_BIN) compare output_omp_basic.bin output_ff_basic.bin

# Performance comparison
perf-test: openmp fastflow
	@echo "=== Performance Comparison ==="
	./$(OMP_BIN) generate
	@echo "OpenMP Performance:"
	./$(OMP_BIN) performance medium_omp.bin
	@echo ""
	@echo "FastFlow Performance:"
	./$(FF_BIN) performance medium_omp.bin

# Clean up
clean:
	rm -f $(OMP_BIN) $(FF_BIN)
	rm -f *.bin
	rm -rf temp