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

class OpenMPExternalMergeSort {
private:
    size_t memory_budget_;          // Maximum memory to use (bytes)
    size_t num_threads_;            // Number of OpenMP threads
    std::string temp_dir_;          // Directory for temporary files
    std::vector<std::string> temp_files_; // List of temporary files created
    
    // Statistics
    size_t total_records_processed_;
    double phase1_time_;
    double phase2_time_;
    
public:
    // Struct to store file chunk information
    struct ChunkInfo {
        size_t start_offset; // Start offset in the file
        size_t size; // Byte size of chunk
        size_t estimated_records; // Estimate of how many records are in chunk
    };

    OpenMPExternalMergeSort(size_t memory_budget = 1024 * 1024 * 1024, // default: 1GB
                           size_t num_threads = 0,
                           const std::string& temp_dir = "./temp_omp") 
        : memory_budget_(memory_budget), 
          temp_dir_(temp_dir),
          total_records_processed_(0),
          phase1_time_(0.0),
          phase2_time_(0.0) {
        
        num_threads_ = (num_threads == 0) ? omp_get_max_threads() : num_threads;
        omp_set_num_threads(num_threads_);
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
     * === Main entry point to sort an input file and save sorted result ===
     * 
     * This function performs the following steps:
     * 1. Creates sorted runs of the input file in parallel using OpenMP.
     * 2. Merges the sorted runs into a single sorted file in parallel using OpenMP.
     * 3. Prints statistics about the sort.
     * 4. Deletes all temporary files created during the sort.
     * 
     * @param input_file The file to sort.
     * @param output_file The file to write the sorted result to.
     * @return true if the sort was successful, false otherwise.
     */
    bool sort_file(const std::string& input_file, const std::string& output_file) {
        auto start_time = std::chrono::high_resolution_clock::now();
        
        std::cout << "Starting OpenMP external merge sort..." << std::endl;
        std::cout << "Input: " << input_file << std::endl;
        std::cout << "Output: " << output_file << std::endl;
        
        // PHASE 1: Divide file into chunks, sort them in parallel, write to temp files
        if (!create_sorted_runs_parallel(input_file)) {
            std::cerr << "Failed to create sorted runs" << std::endl;
            return false;
        }
        
        // PHASE 2: Merge all sorted chunks back into one sorted output
        if (!merge_sorted_runs_parallel(output_file)) {
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
    bool create_sorted_runs_parallel(const std::string& input_file) {
        auto start_time = std::chrono::high_resolution_clock::now();
        
        std::cout << "Phase 1: Creating sorted runs (parallel)..." << std::endl;
        
        // Step 1: Analyze file into chunks based on size and memory budget
        std::vector<ChunkInfo> chunks = analyze_file_for_chunks(input_file);
        if (chunks.empty()) {
            std::cerr << "Failed to analyze input file" << std::endl;
            return false;
        }
        
        std::cout << "File analysis complete. Creating " << chunks.size() << " chunks." << std::endl;
        
        // Step 2: Generate file paths for temporary sorted files
        temp_files_.resize(chunks.size());
        for (size_t i = 0; i < chunks.size(); ++i) {
            temp_files_[i] = temp_dir_ + "/run_" + std::to_string(i) + ".tmp";
        }
        
        bool success = true;
        
        // Step 3: Process each chunk in parallel
        // Use OpenMP to parallelize the loop
        #pragma omp parallel for schedule(dynamic) shared(success)
        for (int i = 0; i < static_cast<int>(chunks.size()); ++i) {
            // If any thread has already failed, skip the current iteration
            if (!success) continue;
            
            // Get the current thread ID
            int thread_id = omp_get_thread_num();

            // Process the current chunk in parallel
            if (!process_chunk_parallel(input_file, chunks[i], temp_files_[i], thread_id)) {
                // If processing fails, set success to false and print an error message
                // Used a critical section to ensure only one thread can execute this at a time
                #pragma omp critical
                {
                    success = false;
                    std::cerr << "Thread " << thread_id << " failed to process chunk " << i << std::endl;
                }
            }
        }
        
        auto end_time = std::chrono::high_resolution_clock::now();
        phase1_time_ = std::chrono::duration<double>(end_time - start_time).count();
        
        std::cout << "Phase 1 completed in " << phase1_time_ << " seconds" << std::endl;
        return success;
    }
    
    /**
     * Sort and write a single chunk to a temp file
     * 
     * This function reads a specific chunk of the input file, sorts the records within it,
     * and writes the sorted records to a temporary file. It ensures that the memory usage
     * per thread does not exceed the specified memory budget.
     * 
     * @param input_file The path to the input file to read from.
     * @param chunk The ChunkInfo structure containing details about the chunk to process.
     * @param temp_file The path to the temporary file where sorted records will be written.
     * @param thread_id The ID of the thread processing the chunk, used for logging.
     * @return true if the chunk was processed successfully, false otherwise.
     */

    bool process_chunk_parallel(const std::string& input_file, 
                               const ChunkInfo& chunk, 
                               const std::string& temp_file,
                               int thread_id) {
        // Preallocate memory for the records to avoid reallocations
        std::vector<Record> records;
        records.reserve(chunk.estimated_records);
        
        // Open the input file in binary mode
        std::ifstream input(input_file, std::ios::binary);
        if (!input) {
            std::cerr << "Thread " << thread_id << ": Failed to open input file" << std::endl;
            return false;
        }
        
        // Seek to the start of the chunk in the input file
        input.seekg(chunk.start_offset);

        // Initialize variables to track the number of bytes read and the memory usage
        size_t bytes_read = 0;
        size_t memory_limit_per_thread = memory_budget_ / num_threads_;
        
        // Read records from the input file until the end of the chunk is reached
        while (bytes_read < chunk.size && input.good()) {
            Record record;
            // If a record cannot be read, break out of the loop
            if (!record.read_from_stream(input)) break;
            
            // Update the number of bytes read and the memory usage
            bytes_read += record.total_size();
            // Approximate memory usage: size of record + payload size
            size_t approx_mem_usage = bytes_read; // rough but sufficient
            
            // Check if the memory usage exceeds the limit per thread
            if (approx_mem_usage > memory_limit_per_thread) {
                std::cerr << "Thread " << thread_id << ": Memory budget exceeded" << std::endl;
                break;
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
     * Phase 2: Merge sorted temporary files into a single sorted output file
     * 
     * This function takes a vector of temporary files produced by process_chunks_parallel
     * and merges them into a single sorted output file. If there's only one temporary file,
     * it simply copies it to the output file.
     * 
     * @param output_file The path to the output file where the sorted records will be written.
     * @return true if the merge was successful, false otherwise.
     */
    bool merge_sorted_runs_parallel(const std::string& output_file) {
        auto start_time = std::chrono::high_resolution_clock::now();
        
        std::cout << "Phase 2: Merging sorted runs (parallel)..." << std::endl;
        
        // Check if there are any temporary files to merge
        if (temp_files_.empty()) {
            std::cerr << "No temp files to merge" << std::endl;
            return false;
        }
        
        // If there's only one temporary file, simply copy it to the output file
        if (temp_files_.size() == 1) {
            std::filesystem::copy_file(temp_files_[0], output_file, std::filesystem::copy_options::overwrite_existing);
            auto end_time = std::chrono::high_resolution_clock::now();
            phase2_time_ = std::chrono::duration<double>(end_time - start_time).count();
            return true;
        }
        
         // If multiple files, perform a parallel merge using the k-way merge algorithm
        bool success = k_way_merge_parallel(temp_files_, output_file);
        
        auto end_time = std::chrono::high_resolution_clock::now();
        phase2_time_ = std::chrono::duration<double>(end_time - start_time).count();
        
        std::cout << "Phase 2 completed in " << phase2_time_ << " seconds" << std::endl;
        return success;
    }
    
    /**
     * K-way merge from multiple sorted temp files into one output
     * 
     * This function takes a vector of temporary files produced by process_chunks_parallel
     * and merges them into a single sorted output file. It uses a priority queue to keep track of the
     * current smallest record from each file and writes the merged records to the output file.
     * 
     * @param temp_files The vector of temporary files to merge.
     * @param output_file The path to the output file where the sorted records will be written.
     * @return true if the merge was successful, false otherwise.
     */
    bool k_way_merge_parallel(const std::vector<std::string>& temp_files, 
                             const std::string& output_file) {
        // Define a struct to hold a record and its corresponding file index
        struct MergeElement {
            Record record; // The record to be merged
            size_t file_index; // The index of the file this record comes from
            
            // Custom comparison operator for the priority queue
            bool operator>(const MergeElement& other) const {
                return record.key > other.record.key;
            }
        };
        
        // Create a priority queue to hold the records to be merged
        std::priority_queue<MergeElement, std::vector<MergeElement>, std::greater<MergeElement>> pq;
        
        // Create a vector to hold the input streams for each temporary file
        std::vector<std::unique_ptr<std::ifstream>> input_streams(temp_files.size());
        
        // Step 1: Open all temp files and insert first record of each into priority queue
        for (size_t i = 0; i < temp_files.size(); ++i) {
            // Open the current temporary file
            input_streams[i] = std::make_unique<std::ifstream>(temp_files[i], std::ios::binary);
            if (!input_streams[i]->is_open()) {
                std::cerr << "Failed to open temp file for merging: " << temp_files[i] << std::endl;
                return false;
            }
            Record rec;
            if (rec.read_from_stream(*input_streams[i])) {
                // Add the first record from the current file to the priority queue
                pq.push(MergeElement{std::move(rec), i});
            }
        }
        
        // Open the output file
        std::ofstream output(output_file, std::ios::binary);
        if (!output) {
            std::cerr << "Failed to open output file for final merge" << std::endl;
            return false;
        }
        
        // Initialize a counter for the number of merged records
        size_t merged_count = 0;

        // Step 2: Continuously write smallest record and refill from the same file
        while (!pq.empty()) {
            // Extract the smallest record from the priority queue
            MergeElement smallest = std::move(const_cast<MergeElement&>(pq.top()));
            pq.pop();
            
            // Write the smallest record to the output file
            if (!smallest.record.write_to_stream(output)) {
                std::cerr << "Failed to write record during merge" << std::endl;
                return false;
            }
            ++merged_count;
            
            // Read the next record from the same file
            Record next_rec;
            if (next_rec.read_from_stream(*input_streams[smallest.file_index])) {
                // Add the next record to the priority queue
                pq.push(MergeElement{std::move(next_rec), smallest.file_index});
            }
        }
        
        std::cout << "Merged " << merged_count << " records into output file" << std::endl;
        output.close();
        
        return true;
    }
    
    /**
     * Analyzes the input file and divides it into manageable chunks for processing.
     *
     * This function reads the specified input file and partitions it into chunks
     * based on the available memory budget and number of threads configured. Each
     * chunk is represented by an offset, its size in bytes, and the number of records
     * it contains. The function returns a vector of ChunkInfo structures, each
     * detailing the characteristics of a file chunk.
     *
     * @param input_file The path to the binary input file to be analyzed.
     * @return A vector of ChunkInfo objects, each representing a chunk of the file.
     *         If the file cannot be opened, an empty vector is returned.
     */

    std::vector<ChunkInfo> analyze_file_for_chunks(const std::string& input_file) {
        // Initialize an empty vector to store the chunk information
        std::vector<ChunkInfo> chunks;
        
        // Open the input file in binary mode and check if it was successful
        std::ifstream input(input_file, std::ios::binary | std::ios::ate);
        if (!input) {
            std::cerr << "Failed to open input file for analysis" << std::endl;
            return chunks;
        }
        
        // Get the size of the input file
        size_t file_size = static_cast<size_t>(input.tellg());
        // Reset the file pointer to the beginning of the file
        input.seekg(0);
        // Initialize the offset to the beginning of the file
        size_t offset = 0;
        
        // Loop through the file until all bytes have been processed
        while (offset < file_size) {
            // Calculate the chunk size based on the available memory budget and number of threads
            // we want to ensure it doesn't exceed the available memory budget per thread or the remaining file size
            size_t chunk_size = std::min(memory_budget_ / num_threads_, file_size - offset);
            
            // Seek to the current offset in the file
            input.seekg(offset);
            
            // Initialize variables to track the number of bytes read and records processed
            size_t bytes_read = 0;
            size_t record_count = 0;
            
            // Loop through the chunk until all bytes have been processed or an error occurs
            while (bytes_read < chunk_size && input.good()) {
                // Create a new Record object to read from the stream
                Record record;
                // Get the current file position before reading the record
                std::streampos before = input.tellg();
                // Attempt to read the record from the stream
                if (!record.read_from_stream(input)) break;
                // Get the current file position after reading the record
                std::streampos after = input.tellg();
                // Check if the file position is valid
                if (before == -1 || after == -1) break;

                // Calculate the size of the record
                size_t record_size = static_cast<size_t>(after - before);
                // Increment the bytes read and record count
                bytes_read += record_size;
                ++record_count;
            }
            
            // Create a new ChunkInfo object to store the chunk information
            chunks.push_back(ChunkInfo{offset, bytes_read, record_count});
            // Increment the offset to the next chunk
            offset += bytes_read;
        }
        
        // Close the input file and return the vector of chunk information
        input.close();
        return chunks;
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
        std::cout << "Phase 1 time: " << phase1_time_ << " seconds" << std::endl;
        std::cout << "Phase 2 time: " << phase2_time_ << " seconds" << std::endl;
        std::cout << "Total elapsed time: " << total_time << " seconds" << std::endl;
    }
};

#endif // OPENMP_EXTERNAL_MERGESORT_H
