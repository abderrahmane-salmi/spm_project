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

int main(int argc, char* argv[]) {

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
    } else if (command == "benchmark") {
        std::string input_file = argv[2];
        if (!fs::exists(input_file)) {
            std::cerr << "Error: Input file does not exist.\n";
            return 1;
        }

        size_t memory_mb = (argc >= 4) ? std::stoul(argv[3]) : 256;
        size_t memory_bytes = memory_mb * 1024 * 1024;
        int workers = (argc >= 5) ? std::stoi(argv[4]) : 4;

        fs::path input_path(input_file);
        std::string output_file = input_path.stem().string() + "_ff_output" + input_path.extension().string();

        FastFlowExternalMergeSort sorter(memory_bytes, workers);

        auto start = std::chrono::high_resolution_clock::now();
        sorter.sort_file(input_file, output_file);
        auto end = std::chrono::high_resolution_clock::now();

        double duration = std::chrono::duration<double>(end - start).count();

        if (fs::exists(output_file)) fs::remove(output_file);

        // Output only performance data (compatible with bash parsing)
        std::cout << "[FF] Workers=" << workers
                << " Time=" << duration << std::endl;
    } 
    else {
        std::cerr << "Unknown command: " << command << std::endl;
        print_usage(argv[0]);
        return 1;
    }
    
    return 0;
}