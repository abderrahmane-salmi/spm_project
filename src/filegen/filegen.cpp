#include "filegen.hpp"
#include "../include/record.hpp"
#include <fstream>
#include <iostream>
#include <vector>
#include <cstdlib>
#include <ctime>
#include <cstdio>  // for std::remove

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
void FileGenerator::generateFile(const std::string& filename, size_t num_records, uint32_t payload_len) {
    if (payload_len < 8 || payload_len > PAYLOAD_MAX) {
        std::cerr << "Invalid payload size: must be between 8 and " << PAYLOAD_MAX << "\n";
        return;
    }

    std::ofstream file(filename, std::ios::binary);
    if (!file) {
        std::cerr << "Failed to create file: " << filename << std::endl;
        return;
    }

    for (size_t i = 0; i < num_records; ++i) {
        uint64_t key = rand(); // Random key for sorting
        std::vector<char> data(payload_len);
        
        // Fill payload with random letters A-Z
        for (uint32_t j = 0; j < payload_len; ++j) {
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
void FileGenerator::generateFileBySize(const std::string& filename, size_t target_size_bytes, uint32_t payload_len) {
    if (payload_len < 8 || payload_len > PAYLOAD_MAX) {
        std::cerr << "Invalid payload size: must be between 8 and " << PAYLOAD_MAX << "\n";
        return;
    }
    
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
        std::vector<char> data(payload_len);
        
        // Fill the payload with random uppercase letters
        for (uint32_t j = 0; j < payload_len; ++j) {
            data[j] = 'A' + (rand() % 26);
        }

        Record rec(key, data);
        rec.write_to_stream(file);
        written += rec.total_size(); // Update the total size written counter
    }
}

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
 * Delete a file
 *
 * Tries to delete the file with the given name. If successful, outputs a success message and returns true.
 * If the deletion fails, outputs an error message and returns false.
 *
 * @param filename The name of the file to delete
 * @return true if the file was deleted successfully, false otherwise
 */
bool delete_file(const std::string& filename) {
    if (std::remove(filename.c_str()) == 0) {
        std::cout << "Deleted file: " << filename << std::endl;
        return true;
    } else {
        std::cerr << "Failed to delete file: " << filename << std::endl;
        return false;
    }
}