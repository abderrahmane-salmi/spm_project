#include "../include/record.hpp"
#include <iostream>
#include <mpi.h>
#include <omp.h>
#include <unistd.h>

#include "mpi.hpp"

void print_usage(const char* program_name) {
    std::cout << "Usage: " << program_name << " <command> [arguments]" << std::endl;
    std::cout << "Commands:" << std::endl;
    std::cout << "  sort <input_file> [memory_budget_mb] [num_threads]" << std::endl;
    std::cout << "    - Sort input_file using MPI + OpenMP" << std::endl;
    std::cout << "    - memory_budget_mb: default is 256 MB per rank" << std::endl;
    std::cout << "    - num_threads: number of OpenMP threads (default: 4)" << std::endl;
    std::cout << "  perf <input_file>" << std::endl;
    std::cout << "    - Run performance test with different configurations" << std::endl;
}

int main(int argc, char* argv[]) {
    // Initialize MPI
    MPI_Init(&argc, &argv);
    
    // Get the rank and size of the MPI world
    int rank, size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank); // the ID of the current process
    MPI_Comm_size(MPI_COMM_WORLD, &size); // total number of processes
    
    // Handle command-line arguments
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
            // Parse command-line arguments
            if (argc < 3) {
                if (rank == 0) {
                    std::cerr << "Error: sort command requires input file" << std::endl;
                    print_usage(argv[0]);
                }
                MPI_Finalize();
                return 1;
            }

            std::string input_file = argv[2];
            std::string output_file = input_file + "_mpi_output.bin";

            size_t memory_budget_mb = (argc >= 4) ? std::stoull(argv[3]) : 256;
            size_t memory_budget = memory_budget_mb * 1024 * 1024;

            int num_threads = (argc >= 5) ? std::stoi(argv[4]) : 4;
            omp_set_num_threads(num_threads);

            // Print summary (only rank 0 does this)
            if (rank == 0) {
                std::cout << "Starting MPI MergeSort..." << std::endl;
                std::cout << "Input: " << input_file << std::endl;
                std::cout << "Output: " << output_file << std::endl;
                std::cout << "Memory budget per rank: " << memory_budget_mb << " MB" << std::endl;
                std::cout << "MPI processes: " << size << std::endl;
                std::cout << "OpenMP threads per rank: " << num_threads << std::endl;
            }

            // Run the hybrid MPI + OpenMP sort
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
