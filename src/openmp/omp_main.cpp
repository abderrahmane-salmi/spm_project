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

/**
 * Run a performance test on the given input file with various thread counts.
 * 
 * This function will attempt to sort the given input file with each thread count
 * in the given list. It will output the time taken to sort the file for each
 * thread count, and verify that the output file is sorted correctly.
 */
void performance_test(const std::string& input_file) {
    // Define the list of thread counts to test
    std::vector<size_t> thread_counts = {1, 2, 4, 8, 16};

    // Iterate over each thread count
    for (size_t threads : thread_counts) {
        // Skip thread counts that exceed the maximum number of threads supported by the system
        if (threads > static_cast<size_t>(omp_get_max_threads())) continue;

        // Generate the output file name based on the thread count
        std::string out = "output_" + std::to_string(threads) + "_threads.bin";

        // Create an instance of the OpenMPExternalMergeSort class with a buffer size of 1GB and the current thread count
        OpenMPExternalMergeSort sorter(1024 * 1024 * 1024, threads);

        auto start = std::chrono::high_resolution_clock::now();

        // Attempt to sort the input file and write the result to the output file
        bool success = sorter.sort_file(input_file, out);

        auto end = std::chrono::high_resolution_clock::now();

        // Calculate the duration of the sorting operation
        double duration = std::chrono::duration<double>(end - start).count();

        // If the sorting operation was successful, output the results and verify that the output file is sorted correctly
        if (success) {
            std::cout << "Threads: " << threads << ", Time: " << duration << " sec" << std::endl;
            verify_sorted_output(out);
        } else {
            // If the sorting operation failed, output an error message
            std::cout << "Failed with " << threads << " threads" << std::endl;
        }

        // Remove the output file to clean up
        if (std::filesystem::exists(out)) std::filesystem::remove(out);
    }
}

/**
 * Test effect of varying memory budget on sort performance
 * Run the OpenMPExternalMergeSort with different memory budgets.
 */
void memory_budget_test(const std::string& input_file) {
    // Define a list of memory budgets to test (in bytes)
    std::vector<size_t> budgets = {
        64 * 1024 * 1024,  // 64 MB
        128 * 1024 * 1024,  // 128 MB
        256 * 1024 * 1024,  // 256 MB
        512 * 1024 * 1024,  // 512 MB
        1024 * 1024 * 1024  // 1 GB
    };

    // Iterate over each memory budget
    for (size_t mem : budgets) {
        // Create an output file name based on the memory budget
        std::string out = "output_" + std::to_string(mem / (1024*1024)) + "MB.bin";

        // Create an instance of OpenMPExternalMergeSort with the current memory budget and 4 threads
        OpenMPExternalMergeSort sorter(mem, 4);

        auto start = std::chrono::high_resolution_clock::now();

        // Sort the input file and write the result to the output file
        bool success = sorter.sort_file(input_file, out);

        auto end = std::chrono::high_resolution_clock::now();

        // Calculate the elapsed time
        double duration = std::chrono::duration<double>(end - start).count();

        // Print the memory budget and elapsed time
        std::cout << "Memory: " << (mem / (1024*1024)) << " MB, Time: " << duration << " sec" << std::endl;

        // Verify that the output file is sorted correctly
        if (success) verify_sorted_output(out);

        // Delete the output file
        if (std::filesystem::exists(out)) std::filesystem::remove(out);
    }
}

int main(int argc, char* argv[]) {
    print_usage(argv[0]);

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

        // Output only performance data for CSV
        std::cout << "[OMP] Threads=" << threads
                  << " Time=" << duration << std::endl;
        // std::cout << threads << "," << duration << std::endl;
    }
    else if (command == "performance") {
        performance_test(input_file);
    }
    else if (command == "memory") {
        memory_budget_test(input_file);
    }
    else {
        std::cerr << "Unknown command: " << command << "\n";
        return 1;
    }

    return 0;
}

/* ## Example Commands ##

# Default sort (256MB, 4 threads)
./omp_main sort input.bin

# Custom memory (512MB) and threads (8)
./omp_main sort input.bin 512 8

# Performance test (1–16 threads)
./omp_main performance input.bin

# Memory test (64MB to 1GB)
./omp_main memory input.bin
*/
