#ifndef OPENMP_EXTERNAL_MERGESORT_H
#define OPENMP_EXTERNAL_MERGESORT_H

#include <iostream>
#include <fstream>
#include <vector>
#include <queue>
#include <algorithm>
#include <memory>
#include <string>
#include <cstdint>
#include <filesystem>
#include <chrono>
#include <omp.h>

#include "../include/record.hpp"
#include "../include/record_io.hpp"
#include "../chunking/chunking.hpp"
#include "../merging/merging.hpp"

class OpenMPExternalMergeSort {
private:
    size_t memory_budget_;          // Maximum memory to use (bytes)
    size_t num_threads_;            // Number of OpenMP threads
    std::string temp_dir_;          // Directory for temporary files
    std::vector<std::string> temp_files_; // List of temporary files created
    
    // Statistics
    size_t total_records_processed_;
    
public:
    OpenMPExternalMergeSort(size_t memory_budget = size_t(30720) * 1024 * 1024, // default: 30GB
                           size_t num_threads = 0,
                           const std::string& temp_dir = "./temp_omp") 
        : memory_budget_(memory_budget), 
          temp_dir_(temp_dir),
          total_records_processed_(0)
        {
        
        num_threads_ = (num_threads == 0) ? omp_get_max_threads() : num_threads;
        omp_set_num_threads(num_threads_);

        // confirm
        int max_threads = omp_get_max_threads();
        #pragma omp parallel
        {
            #pragma omp master
            {
                std::cout << "[OpenMP] omp_get_max_threads(): " << max_threads << std::endl;
                std::cout << "[OpenMP] omp_get_num_threads() inside parallel region: " << omp_get_num_threads() << std::endl;
            }
        }


        std::filesystem::create_directories(temp_dir_);
        
        std::cout << "OpenMP MergeSort initialized with " << num_threads_ 
                  << " threads, memory budget: " << (memory_budget_ / (1024*1024)) 
                  << " MB" << std::endl;
    }
    
    // === Destructor ===
    ~OpenMPExternalMergeSort() {
        cleanup_temp_files(); // Clean up all generated temp files
    }
    
    
    
    /**
     * Main entry point to sort an input file and save the sorted result.
     *
     * This function coordinates the external merge sort process using OpenMP.
     * It performs the following steps:
     * 1. Splits the input file into smaller chunks.
     * 2. Sorts the chunks in parallel using OpenMP.
     * 3. Merges the sorted chunks into a single sorted output file.
     * 4. Cleans up temporary files created during the process.
     *
     * @param input_file The path to the input file to be sorted.
     * @param output_file The path to the file where the sorted result will be saved.
     * @return true if the sorting was successful, false otherwise.
     */
    bool sort_file(const std::string& input_file, const std::string& output_file) {
        auto start_time = std::chrono::high_resolution_clock::now();
        
        std::cout << "Starting OpenMP external merge sort..." << std::endl;
        std::cout << "Input: " << input_file << std::endl;
        std::cout << "Output: " << output_file << std::endl;

        // PHASE 1: Divide file into chunks
        auto chunk_files = generate_chunk_files(input_file, memory_budget_ * 0.8, temp_dir_, num_threads_);
        std::cout << "Phase 1: Created " << chunk_files.size() << " chunk files." << std::endl;
        
        // PHASE 2: Sort chunks in parallel, and write to temp files
        if (!parallel_sort_chunks(chunk_files)) {
            std::cerr << "Failed to create sorted runs" << std::endl;
            return false;
        }
        
        // PHASE 3: Merge all sorted chunks back into one sorted output
        if (!parallel_merge_sorted_files(temp_files_, output_file)) {
            std::cerr << "Failed to merge sorted runs" << std::endl;
            return false;
        }
        
        auto end_time = std::chrono::high_resolution_clock::now();
        double total_time = std::chrono::duration<double>(end_time - start_time).count();
        
        print_statistics(total_time);
        cleanup_temp_files();
        
        return true;
    }
    
private:
    /**
     * Phase 1: creates sorted runs in parallel using OpenMP.
     * 
     * This function divides the input file into chunks, sorts each chunk in parallel using OpenMP,
     * and writes the sorted chunks to temporary files.
     * 
     * @param input_file The file to sort.
     * @return true if the sorted runs were created successfully, false otherwise.
     */
    bool parallel_sort_chunks(const std::vector<std::string>& chunk_files) {
        // Prepare temp_files_ to store sorted chunk file paths
        temp_files_.resize(chunk_files.size());
        for (size_t i = 0; i < chunk_files.size(); ++i) {
            temp_files_[i] = temp_dir_ + "/run_" + std::to_string(i) + ".tmp";
        }
        
        // bool success = true;
        
        // Process each chunk in parallel
        // Use OpenMP to parallelize the loop
        // #pragma omp parallel for schedule(dynamic, 1) shared(success)
        #pragma omp parallel for schedule(dynamic)
        for (int i = 0; i < static_cast<int>(chunk_files.size()); ++i) {
            // If any thread has already failed, skip the current iteration
            // if (!success) continue;
            
            // Get the current thread ID
            int thread_id = omp_get_thread_num();

            // Process the current chunk in parallel
            process_chunk_parallel(chunk_files[i], temp_files_[i], thread_id);

            // if (!process_chunk_parallel(chunk_files[i], temp_files_[i], thread_id)) {
            //     // If processing fails, set success to false and print an error message
            //     // Used a critical section to ensure only one thread can execute this at a time
            //     #pragma omp critical
            //     {
            //         success = false;
            //         std::cerr << "Thread " << thread_id << " failed to process chunk " << i << std::endl;
            //     }
            // }
        }
        // return success;
        return true;
    }
    
    /**
     * Sort and write a single chunk to a temp file
     * 
     * This function reads a specific chunk of the input file, sorts the records within it,
     * and writes the sorted records to a temporary file. It ensures that the memory usage
     * per thread does not exceed the specified memory budget.
     * 
     * @param chunk_file Path to the chunk file (already extracted from the input).
     * @param temp_file The path to the temporary file where sorted records will be written.
     * @param thread_id The ID of the thread processing the chunk, used for logging.
     * @return true if the chunk was processed successfully, false otherwise.
     */

    bool process_chunk_parallel(const std::string& chunk_file, 
                               const std::string& temp_file,
                               int thread_id) {
        #pragma omp critical
        {
            std::cout << "Thread " << thread_id << " starting chunk " << chunk_file << std::endl;
        }


        std::vector<Record> records;
        
        // Open the chunk file
        std::ifstream input(chunk_file, std::ios::binary);
        if (!input.is_open()) {
            std::cerr << "Thread " << thread_id << ": Failed to open chunk file: " << chunk_file << std::endl;
            return false;
        }

        // Initialize variables to track the number of bytes read and the memory usage
        size_t bytes_read = 0;
        size_t memory_limit_per_thread = memory_budget_ / num_threads_;
        
        // Read all records from the chunk file
        while (input.good()) {
            Record record;
            // If a record cannot be read, break out of the loop
            if (!record.read_from_stream(input)) break;
            
            // Update the number of bytes read and the memory usage
            bytes_read += record.total_size();
            
            // Check if the memory usage exceeds the limit per thread
            if (bytes_read > memory_limit_per_thread) {
                static bool warned = false;
                if (!warned) {
                    std::cerr << "Warning: Thread " << thread_id << " exceeded memory budget (" 
                            << bytes_read << " > " << memory_limit_per_thread << " bytes)" << std::endl;
                    warned = true;
                }
                // Do NOT break — continue reading the full chunk
            }
            
            // Add the record to the vector of records
            records.push_back(std::move(record));
        }

        // Close the input file
        input.close();
        
        // Check if any records were read
        if (records.empty()) {
            std::cerr << "Thread " << thread_id << ": No records read from chunk" << std::endl;
            return false;
        }
        
        // Sort the records by key
        std::sort(records.begin(), records.end(), 
                 [](const Record& a, const Record& b) {
                     return a.key < b.key;
                 });
        
        // Open the temporary file in binary mode
        std::ofstream output(temp_file, std::ios::binary);
        if (!output) {
            std::cerr << "Thread " << thread_id << ": Failed to create temp file: " << temp_file << std::endl;
            return false;
        }
        
        // Write the sorted records to the temporary file
        for (const auto& record : records) {
            if (!record.write_to_stream(output)) {
                std::cerr << "Thread " << thread_id << ": Failed to write record to temp file" << std::endl;
                return false;
            }
        }

        // Close the output file
        output.close();
        
        // Update the total number of records processed in a critical section
        #pragma omp critical
        {
            total_records_processed_ += records.size();
            std::cout << "Thread " << thread_id << ": Processed " << records.size() 
                      << " records, temp file: " << temp_file << std::endl;
        }
        
        return true;
    }
    
    /**
     * Deletes temporary files created during processing.
     * Iterates through the list of temporary files and attempts to remove each one.
     * If a file cannot be deleted, a warning message is logged.
     * Clears the list of temporary files after attempting to delete them.
     */

    void cleanup_temp_files() {
        for (const auto& file : temp_files_) {
            std::error_code ec;
            std::filesystem::remove(file, ec);
            if (ec) {
                std::cerr << "Warning: Failed to delete temp file " << file << ": " << ec.message() << std::endl;
            }
        }
        temp_files_.clear();
    }
    
    void print_statistics(double total_time) {
        std::cout << "OpenMP External Merge Sort statistics:" << std::endl;
        std::cout << "Total records processed: " << total_records_processed_ << std::endl;
        std::cout << "Total elapsed time: " << total_time << " seconds" << std::endl;
    }


    bool parallel_merge_sorted_files(const std::vector<std::string>& sorted_files, const std::string& output_file) {
    std::vector<std::string> current_files = sorted_files;
    std::vector<std::string> next_files;

    size_t merge_round = 0;

    while (current_files.size() > 1) {
        next_files.clear();
        size_t num_merges = current_files.size() / 2;

        #pragma omp parallel for schedule(dynamic)
        for (int i = 0; i < static_cast<int>(num_merges); ++i) {
            std::string file1 = current_files[2 * i];
            std::string file2 = current_files[2 * i + 1];
            std::string merged_file = temp_dir_ + "/merge_" + std::to_string(merge_round) + "_" + std::to_string(i) + ".tmp";

            std::ifstream in1(file1, std::ios::binary);
            std::ifstream in2(file2, std::ios::binary);
            std::ofstream out(merged_file, std::ios::binary);

            if (!in1 || !in2 || !out) {
                #pragma omp critical
                std::cerr << "Failed to open one of the merge files: " << file1 << ", " << file2 << std::endl;
                continue;
            }

            Record r1, r2;
            bool has_r1 = r1.read_from_stream(in1);
            bool has_r2 = r2.read_from_stream(in2);

            while (has_r1 && has_r2) {
                if (r1.key < r2.key) {
                    r1.write_to_stream(out);
                    has_r1 = r1.read_from_stream(in1);
                } else {
                    r2.write_to_stream(out);
                    has_r2 = r2.read_from_stream(in2);
                }
            }
            while (has_r1) {
                r1.write_to_stream(out);
                has_r1 = r1.read_from_stream(in1);
            }
            while (has_r2) {
                r2.write_to_stream(out);
                has_r2 = r2.read_from_stream(in2);
            }

            #pragma omp critical
            {
                next_files.push_back(merged_file);
            }
        }

        // Handle odd leftover file (if any)
        if (current_files.size() % 2 == 1) {
            next_files.push_back(current_files.back());
        }

        current_files = next_files;
        merge_round++;
    }

    // Final merge result
    if (current_files.size() == 1) {
        std::filesystem::rename(current_files[0], output_file);
        return true;
    }

    return false;
}
};

#endif // OPENMP_EXTERNAL_MERGESORT_H