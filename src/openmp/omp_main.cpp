#include <iostream>
#include <string>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <cstring>
#include <vector>
#include <omp.h>

#include "../include/record.hpp"
#include "omp.hpp"
#include "../filegen/filegen.hpp"

void print_usage(const std::string& prog_name) {
    std::cout << "\n========================================\n";
    std::cout << " OpenMP External MergeSort - CLI Usage\n";
    std::cout << "========================================\n\n";
    std::cout << "Usage:\n";
    std::cout << "  " << prog_name << " sort [input_file] [memory_multiplier] [num_threads]\n";
    std::cout << "  " << prog_name << " performance [input_file]\n";
    std::cout << "  " << prog_name << " memory [input_file]\n\n";

    std::cout << "Commands:\n";
    std::cout << "  sort             Run a configurable sort\n";
    std::cout << "                   - memory_multiplier: multiplier of 1024 * 1024 (e.g., 256 = 256MB)\n";
    std::cout << "                   - num_threads: number of OpenMP threads (optional)\n\n";

    std::cout << "  performance      Run sorting with various thread counts (1, 2, 4, 8, 16)\n";
    std::cout << "  memory           Run sorting with various memory budgets (64MB - 1GB)\n\n";

    std::cout << "Examples:\n";
    std::cout << "  " << prog_name << " sort myfile.bin            # uses default 256MB & 4 threads\n";
    std::cout << "  " << prog_name << " sort myfile.bin 512 8      # 512MB, 8 threads\n";
    std::cout << "  " << prog_name << " performance myfile.bin     # run performance test\n";
    std::cout << "  " << prog_name << " memory myfile.bin          # run memory budget test\n\n";
}

int main(int argc, char* argv[]) {
    // print_usage(argv[0]);

    // Check command-line arguments
    if (argc < 2) {
        std::cout << "Error usage command" << std::endl;
        return 1;
    }

    std::string command = argv[1];
    std::string input_file = (argc >= 3) ? argv[2] : "medium_test_omp.bin";

    if (!std::filesystem::exists(input_file)) {
        std::cerr << "Error: Input file not found: " << input_file << "\n";
        return 1;
    }

    if (command == "sort") {
        // Parse command-line arguments (if they exist)
        size_t memory_mb = (argc >= 4) ? std::stoul(argv[3]) : 256;
        size_t memory_bytes = memory_mb * 1024 * 1024;
        int threads = (argc >= 5) ? std::stoi(argv[4]) : 4;

        // Generate output file name based on input - ex: data.bin -> data_omp_output.bin
        std::filesystem::path input_path(input_file);
        std::string output_file = input_path.stem().string() + "_omp_output" + input_path.extension().string();

        OpenMPExternalMergeSort sorter(memory_bytes, threads);

        auto start = std::chrono::high_resolution_clock::now();
        bool success = sorter.sort_file(input_file, output_file);
        auto end = std::chrono::high_resolution_clock::now();

        double duration = std::chrono::duration<double>(end - start).count();
        std::cout << "Sort completed in " << duration << " seconds\n";

        if (success) {
            verify_sorted_output(output_file);
        } else {
            std::cerr << "Sort failed.\n";
        }

        // this line deletes the output file, use it only when needed
        // if (std::filesystem::exists(output_file)) std::filesystem::remove(output_file);
    }
    else if (command == "benchmark") {
        size_t memory_mb = (argc >= 4) ? std::stoul(argv[3]) : 256;
        size_t memory_bytes = memory_mb * 1024 * 1024;
        int threads = (argc >= 5) ? std::stoi(argv[4]) : 4;

        std::filesystem::path input_path(input_file);
        std::string output_file = input_path.stem().string() + "_omp_output" + input_path.extension().string();

        OpenMPExternalMergeSort sorter(memory_bytes, threads);

        auto start = std::chrono::high_resolution_clock::now();
        bool success = sorter.sort_file(input_file, output_file);
        auto end = std::chrono::high_resolution_clock::now();

        double duration = std::chrono::duration<double>(end - start).count();

        // Optionally: remove output file to keep disk clean
        if (std::filesystem::exists(output_file)) std::filesystem::remove(output_file);

        // Output only performance data (file name, number of threads, time ) for CSV
        std::cout << "[OMP] File=" << input_file
                  << " Threads=" << threads
                  << " Memory=" << memory_mb
                  << " Time=" << duration << std::endl;

        // std::cout << threads << "," << duration << std::endl;
    }
    else {
        std::cerr << "Unknown command: " << command << "\n";
        return 1;
    }

    return 0;
}
