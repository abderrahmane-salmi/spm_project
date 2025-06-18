#include <iostream>
#include <chrono>
#include <string>
#include <vector>
#include <filesystem>
#include <cstdlib>
#include <ctime>

#include "ff.hpp"
#include "../include/record.hpp"
#include "../filegen/filegen.hpp"

namespace fs = std::filesystem;

void print_usage(const std::string& exe_name) {
    std::cout << "==================================================\n";
    std::cout << " FastFlow External MergeSort - Command Line Tool\n";
    std::cout << "==================================================\n\n";
    std::cout << "Usage:\n";
    std::cout << "  " << exe_name << " sort <input_file> [memory_budget_MB] [num_workers]\n\n";
    std::cout << "Arguments:\n";
    std::cout << "  input_file        Path to the input binary file to sort.\n";
    std::cout << "  memory_budget_MB  Optional. Memory budget in megabytes (default: 256).\n";
    std::cout << "  num_workers       Optional. Number of FastFlow workers (default: 4).\n\n";
    std::cout << "Example:\n";
    std::cout << "  " << exe_name << " sort data.bin\n";
    std::cout << "  " << exe_name << " sort data.bin 512 8\n\n";
}

void performance_test(const std::string& input_file) {
    std::cout << "\n=== FastFlow Performance Test ===" << std::endl;
    std::cout << "Input file: " << input_file << std::endl;
    
    std::vector<int> worker_counts = {1, 2, 4, 8, 16};
    const size_t memory_budget = 256 * 1024 * 1024; // 256MB
    
    for (int workers : worker_counts) {
        std::string output_file = "output_ff_" + std::to_string(workers) + "_workers.bin";
        
        std::cout << "\nTesting with " << workers << " workers..." << std::endl;
        
        auto start = std::chrono::high_resolution_clock::now();
        
        FastFlowExternalMergeSort sorter(memory_budget, workers);
        sorter.sort_file(input_file, output_file);
        
        auto end = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
        
        // Verify correctness
        bool is_sorted = verify_sorted_output(output_file);
        
        std::cout << "Time: " << duration.count() << " ms" << std::endl;
        std::cout << "Correctness: " << (is_sorted ? "PASS" : "FAIL") << std::endl;
        
        // Clean up output file
        fs::remove(output_file);
    }
}

void memory_budget_test(const std::string& input_file) {
    std::cout << "\n=== FastFlow Memory Budget Test ===" << std::endl;
    std::cout << "Input file: " << input_file << std::endl;
    
    std::vector<size_t> memory_budgets = {
        64 * 1024 * 1024,   // 64MB
        128 * 1024 * 1024,  // 128MB
        256 * 1024 * 1024,  // 256MB
        512 * 1024 * 1024,  // 512MB
        1024 * 1024 * 1024  // 1GB
    };
    
    const int workers = 4;
    
    for (size_t budget : memory_budgets) {
        std::string output_file = "output_ff_" + std::to_string(budget / 1024 / 1024) + "MB.bin";
        
        std::cout << "\nTesting with " << (budget / 1024 / 1024) << " MB memory budget..." << std::endl;
        
        auto start = std::chrono::high_resolution_clock::now();
        
        FastFlowExternalMergeSort sorter(budget, workers);
        sorter.sort_file(input_file, output_file);
        
        auto end = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
        
        // Verify correctness
        bool is_sorted = verify_sorted_output(output_file);
        
        std::cout << "Time: " << duration.count() << " ms" << std::endl;
        std::cout << "Correctness: " << (is_sorted ? "PASS" : "FAIL") << std::endl;
        
        // Clean up output file
        fs::remove(output_file);
    }
}

int main(int argc, char* argv[]) {
    print_usage(argv[0]);

    // Check command-line arguments
    if (argc < 3) {
        std::cout << "Error usage command" << std::endl;
        return 1;
    }
    

    std::string command = argv[1];

    if (command == "sort") {
        std::string input_file = argv[2];
        if (!fs::exists(input_file)) {
            std::cerr << "Error: Input file does not exist.\n";
            return 1;
        }

        size_t memory_mb = (argc >= 4) ? std::stoul(argv[3]) : 256;
        size_t memory_bytes = memory_mb * 1024 * 1024;
        int workers = (argc >= 5) ? std::stoi(argv[4]) : 4;

        // Generate output file name based on input
        fs::path input_path(input_file);
        std::string output_file = input_path.stem().string() + "_ff_output" + input_path.extension().string();

        std::cout << "\n=== FastFlow External MergeSort ===\n";
        std::cout << "Input File      : " << input_file << "\n";
        std::cout << "Output File     : " << output_file << "\n";
        std::cout << "Memory Budget   : " << memory_mb << " MB\n";
        std::cout << "Worker Threads  : " << workers << "\n";

        FastFlowExternalMergeSort sorter(memory_bytes, workers);

        auto start = std::chrono::high_resolution_clock::now();
        sorter.sort_file(input_file, output_file);
        auto end = std::chrono::high_resolution_clock::now();
        
        auto duration = std::chrono::duration<double>(end - start).count();
        std::cout << "Sort completed in " << duration << " seconds.\n";
        
        bool sorted = verify_sorted_output(output_file);
        std::cout << "Verification: " << (sorted ? "PASS" : "FAIL") << std::endl;

        // this line deletes the output file, use it only when needed
        // if (std::filesystem::exists(output_file)) std::filesystem::remove(output_file);
    } else {
        std::cerr << "Unknown command: " << command << std::endl;
        print_usage(argv[0]);
        return 1;
    }
    
    return 0;
}