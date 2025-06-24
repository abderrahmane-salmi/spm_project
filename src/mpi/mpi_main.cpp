#include "../include/record.hpp"
#include <iostream>
#include <mpi.h>
#include <omp.h>
#include <unistd.h>
#include <filesystem>
#include <chrono>

#include "mpi.hpp"

void print_usage(const char* program_name) {
    std::cout << "Usage: " << program_name << " <command> [arguments]" << std::endl;
    std::cout << "Commands:" << std::endl;
    std::cout << "  sort <input_file> [memory_budget_mb] [num_threads]" << std::endl;
    std::cout << "    - Sort input_file using MPI + OpenMP" << std::endl;
    std::cout << "    - memory_budget_mb: default is 256 MB per rank" << std::endl;
    std::cout << "    - num_threads: number of OpenMP threads (default: 4)" << std::endl;
    std::cout << "  benchmark <input_file> [memory_budget_mb] [num_threads]" << std::endl;
    std::cout << "    - Run benchmark and output performance timing only (CSV style)" << std::endl;
}

int main(int argc, char* argv[]) {
    MPI_Init(&argc, &argv);
    
    int rank, size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

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
            std::filesystem::path input_path(input_file);
            std::string output_file = input_path.stem().string() + "_mpi_output" + input_path.extension().string();

            size_t memory_budget_mb = (argc >= 4) ? std::stoull(argv[3]) : 256;
            size_t memory_budget = memory_budget_mb * 1024 * 1024;
            int num_threads = (argc >= 5) ? std::stoi(argv[4]) : 4;

            omp_set_num_threads(num_threads);

            if (rank == 0) {
                std::cout << "Starting MPI MergeSort..." << std::endl;
                std::cout << "Input: " << input_file << std::endl;
                std::cout << "Output: " << output_file << std::endl;
                std::cout << "Memory budget per rank: " << memory_budget_mb << " MB" << std::endl;
                std::cout << "MPI processes: " << size << std::endl;
                std::cout << "OpenMP threads per rank: " << num_threads << std::endl;
            }

            mpi_sort_file(input_file, output_file, memory_budget, "temp_mpi");

        } else if (command == "benchmark") {
            if (argc < 3) {
                if (rank == 0) {
                    std::cerr << "Error: benchmark command requires input file" << std::endl;
                    print_usage(argv[0]);
                }
                MPI_Finalize();
                return 1;
            }

            std::string input_file = argv[2];
            std::filesystem::path input_path(input_file);
            std::string output_file = input_path.stem().string() + "_mpi_output" + input_path.extension().string();

            size_t memory_budget_mb = (argc >= 4) ? std::stoull(argv[3]) : 256;
            size_t memory_budget = memory_budget_mb * 1024 * 1024;
            int num_threads = (argc >= 5) ? std::stoi(argv[4]) : 4;

            omp_set_num_threads(num_threads);

            auto start = std::chrono::high_resolution_clock::now();
            mpi_sort_file(input_file, output_file, memory_budget, "temp_mpi");
            auto end = std::chrono::high_resolution_clock::now();
            double duration = std::chrono::duration<double>(end - start).count();

            if (rank == 0) {
                // Optional cleanup
                if (std::filesystem::exists(output_file)) std::filesystem::remove(output_file);
                
                std::cout << "[MPI] Procs=" << size
                          << " Threads=" << num_threads
                          << " Time=" << duration << std::endl;
            }

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
