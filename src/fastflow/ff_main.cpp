// src/fastflow/ff_main.cpp
#include <iostream>
#include <chrono>
#include <string>
#include <vector>
#include <filesystem>
#include <cstdlib>
#include <ctime>
#include "ff.hpp"
#include "../include/record.hpp"

namespace fs = std::filesystem;

// File Generator class (same as OpenMP version)
class FileGenerator {
private:
    static constexpr uint32_t PAYLOAD_MIN = 8;
    static constexpr uint32_t PAYLOAD_MAX = 1024;

public:
    static void generateFile(const std::string& filename, size_t num_records) {
        std::ofstream outfile(filename, std::ios::binary);
        if (!outfile.is_open()) {
            throw std::runtime_error("Cannot create file: " + filename);
        }

        std::srand(static_cast<unsigned>(std::time(nullptr)));
        
        for (size_t i = 0; i < num_records; i++) {
            Record record;
            record.key = std::rand();
            record.len = PAYLOAD_MIN + (std::rand() % (PAYLOAD_MAX - PAYLOAD_MIN + 1));
            
            record.payload.resize(record.len);
            for (uint32_t j = 0; j < record.len; j++) {
                record.payload[j] = 'A' + (std::rand() % 26);
            }
            
            record.write_to_stream(outfile);
        }
        
        outfile.close();
        std::cout << "Generated " << num_records << " records in " << filename << std::endl;
    }

    static void generateFileBySize(const std::string& filename, size_t target_size_bytes) {
        std::ofstream outfile(filename, std::ios::binary);
        if (!outfile.is_open()) {
            throw std::runtime_error("Cannot create file: " + filename);
        }

        std::srand(static_cast<unsigned>(std::time(nullptr)));
        size_t current_size = 0;
        size_t record_count = 0;
        
        while (current_size < target_size_bytes) {
            Record record;
            record.key = std::rand();
            record.len = PAYLOAD_MIN + (std::rand() % (PAYLOAD_MAX - PAYLOAD_MIN + 1));
            
            record.payload.resize(record.len);
            for (uint32_t j = 0; j < record.len; j++) {
                record.payload[j] = 'A' + (std::rand() % 26);
            }
            
            record.write_to_stream(outfile);
            current_size += record.total_size();
            record_count++;
        }
        
        outfile.close();
        std::cout << "Generated " << record_count << " records (" 
                  << (current_size / 1024 / 1024) << " MB) in " << filename << std::endl;
    }
};

// Utility functions (same as OpenMP version)
bool compare_files(const std::string& file1, const std::string& file2) {
    std::ifstream f1(file1, std::ios::binary);
    std::ifstream f2(file2, std::ios::binary);
    
    if (!f1.is_open() || !f2.is_open()) {
        return false;
    }
    
    Record r1, r2;
    while (true) {
        bool read1 = r1.read_from_stream(f1);
        bool read2 = r2.read_from_stream(f2);
        
        if (read1 != read2) return false; // Different number of records
        if (!read1) break; // Both reached EOF
        
        if (r1.key != r2.key || r1.len != r2.len || r1.payload != r2.payload) {
            return false;
        }
    }
    
    return true;
}

bool verify_sorted_output(const std::string& filename) {
    std::ifstream infile(filename, std::ios::binary);
    if (!infile.is_open()) {
        return false;
    }
    
    Record prev_record, current_record;
    bool first = true;
    
    while (current_record.read_from_stream(infile)) {
        if (!first && current_record.key < prev_record.key) {
            return false; // Not sorted
        }
        prev_record = current_record;
        first = false;
    }
    
    return true;
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

void print_usage() {
    std::cout << "FastFlow External MergeSort Usage:" << std::endl;
    std::cout << "  generate                    - Generate test files" << std::endl;
    std::cout << "  basic [input_file]          - Basic sort with default settings" << std::endl;
    std::cout << "  performance [input_file]    - Performance test across worker counts" << std::endl;
    std::cout << "  memory [input_file]         - Memory budget test" << std::endl;
    std::cout << "  large [input_file]          - Sort large file" << std::endl;
    std::cout << "  compare [file1] [file2]     - Compare two files for equality" << std::endl;
    std::cout << "  verify [file]               - Verify file is sorted" << std::endl;
}

int main(int argc, char* argv[]) {
    if (argc < 2) {
        print_usage();
        return 1;
    }
    
    std::string command = argv[1];
    
    try {
        if (command == "generate") {
            // Generate test files
            std::cout << "Generating test files..." << std::endl;
            
            FileGenerator::generateFile("small_ff.bin", 1000);
            FileGenerator::generateFile("medium_ff.bin", 100000);
            FileGenerator::generateFileBySize("large_ff.bin", 500 * 1024 * 1024); // ~500MB
            
        } else if (command == "basic") {
            if (argc < 3) {
                std::cout << "Usage: " << argv[0] << " basic [input_file]" << std::endl;
                return 1;
            }
            
            std::string input_file = argv[2];
            std::string output_file = "output_ff_basic.bin";
            
            std::cout << "FastFlow Basic Sort" << std::endl;
            std::cout << "Input: " << input_file << std::endl;
            std::cout << "Output: " << output_file << std::endl;
            
            auto start = std::chrono::high_resolution_clock::now();
            
            FastFlowExternalMergeSort sorter(256 * 1024 * 1024, 4); // 256MB, 4 workers
            sorter.sort_file(input_file, output_file);
            
            auto end = std::chrono::high_resolution_clock::now();
            auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
            
            std::cout << "Sorting completed in " << duration.count() << " ms" << std::endl;
            
            bool is_sorted = verify_sorted_output(output_file);
            std::cout << "Verification: " << (is_sorted ? "PASS" : "FAIL") << std::endl;
            
        } else if (command == "performance") {
            if (argc < 3) {
                std::cout << "Usage: " << argv[0] << " performance [input_file]" << std::endl;
                return 1;
            }
            performance_test(argv[2]);
            
        } else if (command == "memory") {
            if (argc < 3) {
                std::cout << "Usage: " << argv[0] << " memory [input_file]" << std::endl;
                return 1;
            }
            memory_budget_test(argv[2]);
            
        } else if (command == "large") {
            if (argc < 3) {
                std::cout << "Usage: " << argv[0] << " large [input_file]" << std::endl;
                return 1;
            }
            
            std::string input_file = argv[2];
            std::string output_file = "output_ff_large.bin";
            
            std::cout << "FastFlow Large File Sort" << std::endl;
            std::cout << "Input: " << input_file << std::endl;
            std::cout << "Output: " << output_file << std::endl;
            
            auto start = std::chrono::high_resolution_clock::now();
            
            FastFlowExternalMergeSort sorter(512 * 1024 * 1024, 8); // 512MB, 8 workers
            sorter.sort_file(input_file, output_file);
            
            auto end = std::chrono::high_resolution_clock::now();
            auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
            
            std::cout << "Large file sorting completed in " << duration.count() << " ms" << std::endl;
            
            bool is_sorted = verify_sorted_output(output_file);
            std::cout << "Verification: " << (is_sorted ? "PASS" : "FAIL") << std::endl;
            
        } else if (command == "compare") {
            if (argc < 4) {
                std::cout << "Usage: " << argv[0] << " compare [file1] [file2]" << std::endl;
                return 1;
            }
            
            bool identical = compare_files(argv[2], argv[3]);
            std::cout << "Files " << argv[2] << " and " << argv[3] 
                      << " are " << (identical ? "identical" : "different") << std::endl;
            
        } else if (command == "verify") {
            if (argc < 3) {
                std::cout << "Usage: " << argv[0] << " verify [file]" << std::endl;
                return 1;
            }
            
            bool is_sorted = verify_sorted_output(argv[2]);
            std::cout << "File " << argv[2] << " is " 
                      << (is_sorted ? "properly sorted" : "NOT sorted") << std::endl;
            
        } else {
            std::cout << "Unknown command: " << command << std::endl;
            print_usage();
            return 1;
        }
        
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }
    
    return 0;
}