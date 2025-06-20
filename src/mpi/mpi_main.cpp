#include "mpi.hpp"
#include "../include/record.hpp"
#include <iostream>
#include <mpi.h>
#include <omp.h>
#include <unistd.h>

void print_usage(const char* program_name) {
    std::cout << "Usage: " << program_name << " <command> [arguments]" << std::endl;
    std::cout << "Commands:" << std::endl;
    std::cout << "  sort <input_file> [output_file] [memory_budget_mb]" << std::endl;
    std::cout << "    - Sort input_file using MPI + OpenMP" << std::endl;
    std::cout << "    - output_file: default is <input_file>_mpi_output.bin" << std::endl;
    std::cout << "    - memory_budget_mb: default is 256 MB per rank" << std::endl;
    std::cout << "  perf <input_file>" << std::endl;
    std::cout << "    - Run performance test with different configurations" << std::endl;
    std::cout << "  info" << std::endl;
    std::cout << "    - Show MPI and OpenMP configuration" << std::endl;
}

void show_system_info() {
    int rank, size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    
    if (rank == 0) {
        std::cout << "=== System Information ===" << std::endl;
        std::cout << "MPI Processes: " << size << std::endl;
        
        #pragma omp parallel
        {
            if (omp_get_thread_num() == 0) {
                std::cout << "OpenMP Threads per process: " << omp_get_num_threads() << std::endl;
                std::cout << "Total parallel units: " << size * omp_get_num_threads() << std::endl;
            }
        }
        
        std::cout << "Hybrid parallelization: MPI (distributed) + OpenMP (shared memory)" << std::endl;
        std::cout << "===========================" << std::endl;
    }
    
    // Each rank reports its hostname and thread count
    char hostname[256];
    gethostname(hostname, sizeof(hostname));
    
    int num_threads;
    #pragma omp parallel
    {
        #pragma omp master
        num_threads = omp_get_num_threads();
    }
    
    // Use a barrier to ensure ordered output
    for (int i = 0; i < size; ++i) {
        if (rank == i) {
            std::cout << "Rank " << rank << " on " << hostname 
                      << " with " << num_threads << " OpenMP threads" << std::endl;
        }
        MPI_Barrier(MPI_COMM_WORLD);
    }
}

int main(int argc, char* argv[]) {
    // Initialize MPI
    MPI_Init(&argc, &argv);
    
    int rank, size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    
    // Set OpenMP thread count if not already set
    if (!getenv("OMP_NUM_THREADS")) {
        omp_set_num_threads(4); // Default to 4 threads per process
    }
    
    if (argc < 2) {
        if (rank == 0) {
            print_usage(argv[0]);
        }
        MPI_Finalize();
        return 1;
    }
    
    std::string command = argv[1];
    
    try {
        if (command == "sort") {
            if (argc < 3) {
                if (rank == 0) {
                    std::cerr << "Error: sort command requires input file" << std::endl;
                    print_usage(argv[0]);
                }
                MPI_Finalize();
                return 1;
            }
            
            std::string input_file = argv[2];
            std::string output_file = (argc >= 4) ? argv[3] : input_file + "_mpi_output.bin";
            size_t memory_budget_mb = (argc >= 5) ? std::stoull(argv[4]) : 256;
            size_t memory_budget = memory_budget_mb * 1024 * 1024;
            
            if (rank == 0) {
                std::cout << "Starting MPI MergeSort..." << std::endl;
                std::cout << "Input: " << input_file << std::endl;
                std::cout << "Output: " << output_file << std::endl;
                std::cout << "Memory budget per rank: " << memory_budget_mb << " MB" << std::endl;
                std::cout << "MPI processes: " << size << std::endl;
            }
            
            mpi_sort_file(input_file, output_file, memory_budget, "temp_mpi");
            
        } else if (command == "perf") {
            if (argc < 3) {
                if (rank == 0) {
                    std::cerr << "Error: perf command requires input file" << std::endl;
                    print_usage(argv[0]);
                }
                MPI_Finalize();
                return 1;
            }
            
            std::string input_file = argv[2];
            mpi_performance_test(input_file);
            
        } else if (command == "info") {
            show_system_info();
            
        } else {
            if (rank == 0) {
                std::cerr << "Error: Unknown command '" << command << "'" << std::endl;
                print_usage(argv[0]);
            }
            MPI_Finalize();
            return 1;
        }
        
    } catch (const std::exception& e) {
        std::cerr << "Rank " << rank << " error: " << e.what() << std::endl;
        MPI_Abort(MPI_COMM_WORLD, 1);
        return 1;
    }
    
    MPI_Finalize();
    return 0;
}