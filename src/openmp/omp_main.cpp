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

// Custom inline generator
struct FileGenerator {
    void generateFile(const std::string& filename, size_t num_records) {
        std::ofstream file(filename, std::ios::binary);
        if (!file) {
            std::cerr << "Failed to create file: " << filename << std::endl;
            return;
        }

        for (size_t i = 0; i < num_records; ++i) {
            uint64_t key = rand(); // random key
            uint32_t len = 100;
            char* data = new char[len];
            for (uint32_t j = 0; j < len; ++j) {
                data[j] = 'A' + (rand() % 26);
            }

            Record rec(key, len, data);
            rec.write_to_stream(file);
            delete[] data;
        }
    }

    void generateFileBySize(const std::string& filename, size_t target_size_bytes) {
        std::ofstream file(filename, std::ios::binary);
        if (!file) {
            std::cerr << "Failed to create file: " << filename << std::endl;
            return;
        }

        size_t written = 0;
        while (written < target_size_bytes) {
            uint64_t key = rand();
            uint32_t len = 100;
            char* data = new char[len];
            for (uint32_t j = 0; j < len; ++j) {
                data[j] = 'A' + (rand() % 26);
            }

            Record rec(key, len, data);
            rec.write_to_stream(file);
            written += rec.total_size();
            delete[] data;
        }
    }
};

// Verify sorted output
bool verify_sorted_output(const std::string& filename) {
    std::ifstream file(filename, std::ios::binary);
    if (!file) {
        std::cerr << "Failed to open file: " << filename << std::endl;
        return false;
    }

    Record prev, curr;
    bool first = true;
    size_t count = 0;

    while (curr.read_from_stream(file)) {
        if (!first && prev.key > curr.key) {
            std::cerr << "Sort verification failed at record " << count << ": "
                      << prev.key << " > " << curr.key << std::endl;
            return false;
        }
        if (prev.payload) delete[] prev.payload;
        prev = curr;
        curr.payload = nullptr; // prevent double free
        first = false;
        ++count;
    }

    if (prev.payload) delete[] prev.payload;
    std::cout << "Verification PASSED! Total records: " << count << std::endl;
    return true;
}

// Compare two files
bool compare_files(const std::string& file1, const std::string& file2) {
    std::ifstream f1(file1, std::ios::binary);
    std::ifstream f2(file2, std::ios::binary);

    if (!f1 || !f2) {
        std::cerr << "Failed to open one of the files." << std::endl;
        return false;
    }

    Record r1, r2;
    size_t index = 0;

    while (true) {
        bool b1 = r1.read_from_stream(f1);
        bool b2 = r2.read_from_stream(f2);

        if (b1 != b2) return false;
        if (!b1 && !b2) break;

        if (r1.key != r2.key || r1.len != r2.len ||
            std::memcmp(r1.payload, r2.payload, r1.len) != 0) {
            std::cerr << "Mismatch at record " << index << std::endl;
            if (r1.payload) delete[] r1.payload;
            if (r2.payload) delete[] r2.payload;
            return false;
        }

        if (r1.payload) delete[] r1.payload;
        if (r2.payload) delete[] r2.payload;
        ++index;
    }

    std::cout << "File comparison PASSED! Records compared: " << index << std::endl;
    return true;
}

void performance_test(const std::string& input_file) {
    std::vector<size_t> thread_counts = {1, 2, 4, 8, 16};

    for (size_t threads : thread_counts) {
        if (threads > static_cast<size_t>(omp_get_max_threads())) continue;

        std::string out = "output_" + std::to_string(threads) + "_threads.bin";
        OpenMPExternalMergeSort sorter(1024 * 1024 * 1024, threads);
        auto start = std::chrono::high_resolution_clock::now();
        bool success = sorter.sort_file(input_file, out);
        auto end = std::chrono::high_resolution_clock::now();
        double duration = std::chrono::duration<double>(end - start).count();

        if (success) {
            std::cout << "Threads: " << threads << ", Time: " << duration << " sec" << std::endl;
            verify_sorted_output(out);
        } else {
            std::cout << "Failed with " << threads << " threads" << std::endl;
        }

        if (std::filesystem::exists(out)) std::filesystem::remove(out);
    }
}

void memory_budget_test(const std::string& input_file) {
    std::vector<size_t> budgets = {
        64 * 1024 * 1024,
        128 * 1024 * 1024,
        256 * 1024 * 1024,
        512 * 1024 * 1024,
        1024 * 1024 * 1024
    };

    for (size_t mem : budgets) {
        std::string out = "output_" + std::to_string(mem / (1024*1024)) + "MB.bin";
        OpenMPExternalMergeSort sorter(mem, 4);
        auto start = std::chrono::high_resolution_clock::now();
        bool success = sorter.sort_file(input_file, out);
        auto end = std::chrono::high_resolution_clock::now();
        double duration = std::chrono::duration<double>(end - start).count();

        std::cout << "Memory: " << (mem / (1024*1024)) << " MB, Time: " << duration << " sec" << std::endl;
        if (success) verify_sorted_output(out);
        if (std::filesystem::exists(out)) std::filesystem::remove(out);
    }
}

int main(int argc, char* argv[]) {
    std::cout << "=== OpenMP External MergeSort Tester ===" << std::endl;

    if (argc < 2) {
        std::cout << "Usage: " << argv[0] << " <test_type> [input_file]" << std::endl;
        std::cout << "Test types: generate, basic, performance, memory, large" << std::endl;
        return 1;
    }

    std::string test = argv[1];
    FileGenerator gen;

    if (test == "generate") {
        gen.generateFile("small_test_omp.bin", 1000);
        std::cout << "Generated small_test_omp.bin" << std::endl;

        gen.generateFile("medium_test_omp.bin", 100000);
        std::cout << "Generated medium_test_omp.bin" << std::endl;

        gen.generateFileBySize("large_test_omp.bin", 500 * 1024 * 1024);
        std::cout << "Generated large_test_omp.bin (~500MB)" << std::endl;
        return 0;
    }

    std::string input_file = argc >= 3 ? argv[2] : "medium_test_omp.bin";
    if (!std::filesystem::exists(input_file)) {
        std::cerr << "Input file not found. Use 'generate' to create one." << std::endl;
        return 1;
    }

    if (test == "basic") {
        OpenMPExternalMergeSort sorter(256 * 1024 * 1024, 4);
        std::string output_file = "output_basic_omp.bin";

        auto start = std::chrono::high_resolution_clock::now();
        bool success = sorter.sort_file(input_file, output_file);
        auto end = std::chrono::high_resolution_clock::now();
        double duration = std::chrono::duration<double>(end - start).count();

        if (success) {
            std::cout << "Sort complete in " << duration << " seconds" << std::endl;
            verify_sorted_output(output_file);
        } else {
            std::cout << "Sort failed!" << std::endl;
        }

        if (std::filesystem::exists(output_file)) std::filesystem::remove(output_file);

    } else if (test == "performance") {
        performance_test(input_file);

    } else if (test == "memory") {
        memory_budget_test(input_file);

    } else if (test == "large") {
        OpenMPExternalMergeSort sorter(512 * 1024 * 1024, 8);
        std::string output_file = "output_large_omp.bin";

        auto start = std::chrono::high_resolution_clock::now();
        bool success = sorter.sort_file(input_file, output_file);
        auto end = std::chrono::high_resolution_clock::now();
        double duration = std::chrono::duration<double>(end - start).count();

        if (success) {
            std::cout << "Large file sorted in " << duration << " seconds" << std::endl;
            verify_sorted_output(output_file);
        } else {
            std::cout << "Large file sort failed." << std::endl;
        }

        if (std::filesystem::exists(output_file)) std::filesystem::remove(output_file);

    } else {
        std::cerr << "Unknown test type: " << test << std::endl;
        return 1;
    }

    return 0;
}
