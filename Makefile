# Makefile for SPM Project 1 - Distributed Out-of-Core MergeSort

CXX = g++
MPICXX = mpicxx
CXXFLAGS = -std=c++17 -O3 -Wall -Wextra

# FastFlow installation path
FF_PATH = ./fastflow
FF_INCLUDES = -I$(FF_PATH)

# OpenMP flags
OMP_FLAGS = -fopenmp

# MPI flags
MPIFLAGS = -fopenmp

# Directories
SRC_DIR = src
INCLUDE_DIR = $(SRC_DIR)/include
OMP_DIR = $(SRC_DIR)/openmp
FF_DIR = $(SRC_DIR)/fastflow
MPI_DIR = $(SRC_DIR)/mpi
FILEGEN_DIR = $(SRC_DIR)/filegen

# Output binaries
OMP_BIN = omp_mergesort
FF_BIN = ff_mergesort
MPI_BIN = mpi_mergesort
FILEGEN_BIN = filegen

# Includes
INCLUDES = -I$(INCLUDE_DIR)

.PHONY: all clean openmp fastflow mpi filegen test quick-test perf-test test_mpi

all: openmp fastflow mpi filegen

# Build OpenMP binary
openmp: $(OMP_BIN)

$(OMP_BIN): $(OMP_DIR)/omp_main.cpp $(INCLUDE_DIR)/record.hpp $(OMP_DIR)/omp.hpp src/filegen/filegen.cpp src/chunking/adaptive_chunker.cpp src/merging/merging.cpp
	$(CXX) $(CXXFLAGS) $(OMP_FLAGS) $(INCLUDES) -o $@ $(OMP_DIR)/omp_main.cpp src/filegen/filegen.cpp src/chunking/adaptive_chunker.cpp src/merging/merging.cpp

# Build FastFlow binary
fastflow: $(FF_BIN)

$(FF_BIN): $(FF_DIR)/ff_main.cpp $(INCLUDE_DIR)/record.hpp $(FF_DIR)/ff.hpp src/filegen/filegen.cpp src/chunking/adaptive_chunker.cpp src/merging/merging.cpp
	$(CXX) $(CXXFLAGS) $(FF_INCLUDES) $(INCLUDES) -o $@ $(FF_DIR)/ff_main.cpp src/filegen/filegen.cpp src/chunking/adaptive_chunker.cpp src/merging/merging.cpp -pthread

# Build MPI binary
mpi: $(MPI_BIN)

$(MPI_BIN): $(MPI_DIR)/mpi_main.cpp $(MPI_DIR)/mpi.cpp $(MPI_DIR)/mpi.hpp $(INCLUDE_DIR)/record.hpp src/filegen/filegen.cpp src/chunking/adaptive_chunker.cpp src/merging/merging.cpp
	$(MPICXX) $(CXXFLAGS) $(MPIFLAGS) $(INCLUDES) -o $@ $(MPI_DIR)/mpi_main.cpp $(MPI_DIR)/mpi.cpp $(MPI_DIR)/mpi.hpp src/filegen/filegen.cpp src/chunking/adaptive_chunker.cpp src/merging/merging.cpp

# Build filegen CLI tool
filegen: $(FILEGEN_BIN)

$(FILEGEN_BIN): $(FILEGEN_DIR)/filegen_main.cpp $(FILEGEN_DIR)/filegen.cpp $(FILEGEN_DIR)/filegen.hpp
	$(CXX) $(CXXFLAGS) $(INCLUDES) -o $@ $(FILEGEN_DIR)/filegen_main.cpp $(FILEGEN_DIR)/filegen.cpp

# Test both OpenMP and FastFlow implementations
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

# Test only MPI implementation
test_mpi: mpi filegen
	@echo "=== Generating input file for MPI ==="
	./$(FILEGEN_BIN) gen_count file1000.bin 1000

	@echo "=== Running MPI Sort ==="
	mpirun -np 4 ./$(MPI_BIN) sort file1000.bin 256 4

	@echo "=== Verifying MPI Output ==="
	./$(FILEGEN_BIN) verify file1000_mpi_output.bin

	@echo "=== Deleting input/output files ==="
	./$(FILEGEN_BIN) delete file1000.bin
	./$(FILEGEN_BIN) delete file1000_mpi_output.bin

test_mpi2: mpi filegen
	@echo "=== Generating input file for MPI ==="
	./$(FILEGEN_BIN) gen_size file512.bin 512

	@echo "=== Running MPI Sort ==="
	mpirun -np 4 ./$(MPI_BIN) sort file512.bin 256 4

	@echo "=== Verifying MPI Output ==="
	./$(FILEGEN_BIN) verify file512_mpi_output.bin

	@echo "=== Deleting input/output files ==="
	./$(FILEGEN_BIN) delete file512.bin
	./$(FILEGEN_BIN) delete file512_mpi_output.bin

test_mpi3: mpi filegen
	@echo "=== Generating input file for MPI ==="
	./$(FILEGEN_BIN) gen_size file2gb.bin 2000

	@echo "=== Running MPI Sort ==="
	mpirun -np 4 ./$(MPI_BIN) sort file2gb.bin 256 4

	@echo "=== Verifying MPI Output ==="
	./$(FILEGEN_BIN) verify file2gb_mpi_output.bin

	@echo "=== Deleting input/output files ==="
	./$(FILEGEN_BIN) delete file2gb.bin
	./$(FILEGEN_BIN) delete file2gb_mpi_output.bin

gen4gb: mpi filegen
	@echo "=== Generating input file 4GB ==="
	./$(FILEGEN_BIN) gen_size file4gb.bin 4000

test_mpi4: mpi filegen
	@echo "=== Running MPI Sort ==="
	mpirun -np 4 ./$(MPI_BIN) sort file4gb.bin 512 4

	@echo "=== Verifying MPI Output ==="
	./$(FILEGEN_BIN) verify file4gb_mpi_output.bin

	@echo "=== Deleting output file ==="
	./$(FILEGEN_BIN) delete file4gb_mpi_output.bin


# Clean everything
clean:
	rm -f $(OMP_BIN) $(FF_BIN) $(MPI_BIN) $(FILEGEN_BIN)
	rm -f *.bin
	rm -rf temp
