# Parallel Sorting Project

This repository is for the project of the module Parallel and Distributed Systems: Paradigms and Models, at the University Of Pisa, for the academic year 2024-25, in which we implemented three parallel sorting solutions (OpenMP, FastFlow, and MPI) for large datasets, along with utilities for generating and validating input files.

---

## Features

- **OpenMP-based mergesort**  
- **FastFlow-based mergesort** (requires external FastFlow library)  
- **MPI-based mergesort** with OpenMP threading support  
- Input file generation tools  
- Flexible memory and parallelism configuration  

---

## Prerequisites

- A C++ compiler with C++11 (or later) support  
- MPI implementation (e.g., OpenMPI or MPICH) for the MPI version  
- [FastFlow library](https://github.com/fastflow/fastflow) for the FastFlow version  
- Make tool  

---

## FastFlow Library Setup

The FastFlow library is **not included** in this repository due to size and licensing constraints. After cloning this repo, you must manually download or clone FastFlow into the root folder:

```bash
git clone https://github.com/fastflow/fastflow.git
```

Make sure the directory structure looks like this:

```css
spm_project/
├── fastflow/
├── src/
├── Makefile
└── README.md
```

The build system expects the FastFlow headers and source code in the fastflow/ directory at the root level.

---

## Building

Use the provided Makefile to build all or selected components.

Build all components (OpenMP, FastFlow, MPI, FileGen)

```bash
make
```

Clean build artifacts and temporary files

```bash
make clean
```

---

## Generating Test Input Files

The filegen utility allows you to create test files.

Generate a file with a fixed number of records and payload size:

```bash
./filegen gen_count <output_file> <num_records> <payload_bytes>
```

Generate a file approximately a given size (in MB):

```bash
./filegen gen_size <output_file> <approximate_size_MB>
```

Examples:

```bash
./filegen gen_count data_10M_p64.bin 10000000 64
./filegen gen_size file4gb.bin 4000
```

---

## Generating Experiment Data Files

Use the following commands to generate the data files used in our experiments. All files will be created in the `data/` folder (make sure this folder exists or create it manually):

```bash
# Generate 10 million records, 64 bytes payload (~725 MB)
./filegen gen_count data/data_10M_p64.bin 10000000 64

# Generate 10 million records, 512 bytes payload (~4.9 GB)
./filegen gen_count data/data_10M_p512.bin 10000000 512

# Generate 50 million records, 64 bytes payload (~3.6 GB)
./filegen gen_count data/data_50M_p64.bin 50000000 64

# Generate 50 million records, 512 bytes payload (~25 GB)
./filegen gen_count data/data_50M_p512.bin 50000000 512
```

Make sure you have built the filegen executable before running these commands:

---

# Running the Sorters

## OpenMP Sorter

```bash
./omp_mergesort benchmark <input_file> [memory_MB] [num_threads]
./omp_mergesort benchmark data_10M_p512.bin 512 8
```

## FastFlow Sorter

```bash
./ff_mergesort benchmark <input_file> [memory_MB] [num_workers]
./ff_mergesort benchmark data_50M_p512.bin 512 8
```

## OpenMP Sorter

```bash
mpirun -np <num_processes> ./mpi_mergesort benchmark <input_file> [memory_MB] [omp_threads]
mpirun -np 2 ./mpi_mergesort benchmark data_50M_p512.bin 256 4
```

## Notes

* Memory arguments specify the memory budget for chunking and sorting (in megabytes).
* Thread and worker counts control parallelism for each implementation.
* Large files may require adjusting memory budget and parallelism parameters for best performance.
