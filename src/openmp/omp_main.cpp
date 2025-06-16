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

// === Helper struct to generate binary test files with records ===
struct FileGenerator {
    /**
     * Generate a file with a specific number of records
     *
     * Writes a binary file with @p num_records random records, each with a random
     * 64-bit key, a fixed length of 100 bytes and a payload consisting of random
     * characters in the range 'A' to 'Z'.
     *
     * @param filename The file to generate
     * @param num_records The number of records to generate
     */
    void generateFile(const std::string& filename, size_t num_records) {
        std::ofstream file(filename, std::ios::binary);
        if (!file) {
            std::cerr << "Failed to create file: " << filename << std::endl;
            return;
        }

        for (size_t i = 0; i < num_records; ++i) {
            uint64_t key = rand(); // Random key for sorting
            uint32_t len = 100; // Fixed payload size
            std::vector<char> data(len);
            
            // Fill payload with random letters A-Z
            for (uint32_t j = 0; j < len; ++j) {
                data[j] = 'A' + (rand() % 26);
            }

            Record rec(key, data); // Create a record
            rec.write_to_stream(file); // Write to file
        }
    }

    /**
     * Generate a file with records until it reaches a certain byte size
     *
     * This function creates a binary file containing random records 
     * until the total file size reaches approximately @p target_size_bytes. Each record 
     * consists of a random 64-bit key and a fixed-length payload of 100 bytes filled 
     * with random uppercase letters from 'A' to 'Z'.
     *
     * @param filename The name of the file to be generated
     * @param target_size_bytes The approximate target size of the generated file in bytes
     */

    void generateFileBySize(const std::string& filename, size_t target_size_bytes) {
        // Open the file in binary mode for writing
        std::ofstream file(filename, std::ios::binary);
        if (!file) {
            std::cerr << "Failed to create file: " << filename << std::endl;
            return;
        }

        // Initialize a counter to track the total size written to the file
        size_t written = 0;

        // Continue writing records to the file until the target size is reached
        while (written < target_size_bytes) {
            uint64_t key = rand();
            uint32_t len = 100;
            std::vector<char> data(len);
            
            // Fill the payload with random uppercase letters
            for (uint32_t j = 0; j < len; ++j) {
                data[j] = 'A' + (rand() % 26);
            }

            Record rec(key, data);
            rec.write_to_stream(file);
            written += rec.total_size(); // Update the total size written counter
        }
    }
};


/**
 * Validate that output file is sorted correctly
 *
 * Reads the file from start to finish and checks that each record's key is
 * less than or equal to the previous record's key. If the check fails, output
 * an error message and return false. If the check passes, output a success
 * message and return true.
 */
bool verify_sorted_output(const std::string& filename) {
    // open the file in binary mode
    std::ifstream file(filename, std::ios::binary);
    if (!file) {
        std::cerr << "Failed to open file: " << filename << std::endl;
        return false;
    }

    
    Record prev, curr; // Previous and current records
    bool first = true; // Flag to track if this is the first record
    size_t count = 0; // Counter for the number of records

    // Read records from the file until the end is reached
    while (curr.read_from_stream(file)) {
        // If this is not the first record and the previous record's key is greater than the current record's key, output an error message and return false
        if (!first && prev.key > curr.key) {
            std::cerr << "Sort verification failed at record " << count << ": "
                      << prev.key << " > " << curr.key << std::endl;
            return false;
        }

        // Update the previous record
        prev = curr;
        first = false; // This is no longer the first record
        ++count;
    }

    // all records were read successfully
    std::cout << "Verification PASSED! Total records: " << count << std::endl;
    return true;
}


/**
 * Compare two binary files to check if they are identical
 *
 * This function reads records from two binary files and compares them
 * record by record. Each record's key, length, and payload are compared
 * for equality. The function returns false if the files differ in size
 * or content, and true if they are identical. If the files differ, an
 * error message indicating the index of the mismatched record is output.
 */

bool compare_files(const std::string& file1, const std::string& file2) {
    // Open the two files in binary mode
    std::ifstream f1(file1, std::ios::binary);
    std::ifstream f2(file2, std::ios::binary);

    if (!f1 || !f2) {
        std::cerr << "Failed to open one of the files." << std::endl;
        return false;
    }

    // Initialize Record objects to hold data from each file
    Record r1, r2;
    size_t index = 0; // Keep track of the current record index

    // Loop indefinitely until we reach the end of one of the files
    while (true) {
        // Read a record from each file
        bool b1 = r1.read_from_stream(f1);
        bool b2 = r2.read_from_stream(f2);

        if (b1 != b2) return false; // If the read operations didn't both succeed, return false
        if (!b1 && !b2) break; // If we reached the end of both files, exit the loop

        // Compare the records, check if key, length, and payload match
        if (r1.key != r2.key || r1.len != r2.len || r1.payload != r2.payload) {
            std::cerr << "Mismatch at record " << index << std::endl;
            return false;
        }

        ++index; // Increment the record index
    }

    // If we made it through the entire loop without returning, the files match
    std::cout << "File comparison PASSED! Records compared: " << index << std::endl;
    return true;
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
    std::cout << "=== OpenMP External MergeSort Tester ===" << std::endl;

    // Check command-line arguments
    if (argc < 2) {
        std::cout << "Usage: " << argv[0] << " <test_type> [input_file]" << std::endl;
        std::cout << "Test types: generate, basic, performance, memory, large" << std::endl;
        return 1;
    }

    // Determine test type
    std::string test = argv[1];
    FileGenerator gen;

    // Generate test files
    if (test == "generate") {
        gen.generateFile("small_test_omp.bin", 1000);
        std::cout << "Generated small_test_omp.bin" << std::endl;

        gen.generateFile("medium_test_omp.bin", 100000);
        std::cout << "Generated medium_test_omp.bin" << std::endl;

        gen.generateFileBySize("large_test_omp.bin", 500 * 1024 * 1024);
        std::cout << "Generated large_test_omp.bin (~500MB)" << std::endl;
        return 0;
    }

    // Set input file
    std::string input_file = argc >= 3 ? argv[2] : "medium_test_omp.bin";
    if (!std::filesystem::exists(input_file)) {
        std::cerr << "Input file not found. Use 'generate' to create one." << std::endl;
        return 1;
    }

    // Perform test
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

/*
==================================================
 OpenMP External MergeSort Tester - Command Guide
==================================================

Usage:
    ./omp_main <test_type> [input_file]

Test Types:
--------------------------------------------
1. generate
   - Creates test input files:
     - small_test_omp.bin    (1,000 records)
     - medium_test_omp.bin   (100,000 records)
     - large_test_omp.bin    (~500MB in size)

   Example:
     ./omp_main generate

2. basic
   - Performs a basic sort using:
     - 256 MB memory
     - 4 threads

   Example:
     ./omp_main basic medium_test_omp.bin

3. performance
   - Runs sorting with varying thread counts:
     - 1, 2, 4, 8, 16 (up to your CPU’s max)

   Example:
     ./omp_main performance medium_test_omp.bin

4. memory
   - Runs sorting with different memory budgets:
     - 64MB, 128MB, 256MB, 512MB, 1024MB

   Example:
     ./omp_main memory medium_test_omp.bin

5. large
   - Sorts a large file (~500MB) using:
     - 512 MB memory
     - 8 threads

   Example:
     ./omp_main large large_test_omp.bin

Note:
- If [input_file] is not provided, defaults to: medium_test_omp.bin
- Use 'generate' first if test files don't exist.
*/
