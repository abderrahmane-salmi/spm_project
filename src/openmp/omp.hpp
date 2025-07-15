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

#include <mutex>

#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>

#include "../include/record.hpp"
#include "../include/record_io.hpp"
#include "../chunking/chunking.hpp"
#include "../merging/merging.hpp"

class OpenMPExternalMergeSort {
private:
    size_t memory_budget_;          // Maximum memory to use (bytes)
    size_t num_threads_;            // Number of OpenMP threads
    size_t max_memory_per_thread_;  // Maximum memory per thread
    std::string temp_dir_;          // Directory for temporary files
    std::vector<std::string> temp_files_; // List of temporary files created
    
public:
    OpenMPExternalMergeSort(size_t memory_budget = 28 * 1024 * 1024 * 1024, // default: 1GB
                           size_t num_threads = 0,
                           const std::string& temp_dir = "./temp_omp") 
        : memory_budget_(memory_budget), 
          temp_dir_(temp_dir)
        //   total_records_processed_(0)
        {
        
        // set number of threads
        num_threads_ = (num_threads == 0) ? omp_get_max_threads() : num_threads;
        omp_set_num_threads(num_threads_);

        max_memory_per_thread_ = memory_budget_ / num_threads_;

        // confirm
        // int max_threads = omp_get_max_threads();
        // #pragma omp parallel
        // {
        //     #pragma omp master
        //     {
        //         std::cout << "[OpenMP] omp_get_max_threads(): " << max_threads << std::endl;
        //         std::cout << "[OpenMP] omp_get_num_threads() inside parallel region: " << omp_get_num_threads() << std::endl;
        //     }
        // }


        std::filesystem::create_directories(temp_dir_);
        
        std::cout << "OpenMP MergeSort initialized with " << num_threads_ 
                  << " threads, memory budget: " << (memory_budget_ / (1024*1024)) 
                  << " MB" << std::endl;
    }
    
    // === Destructor ===
    ~OpenMPExternalMergeSort() {
        cleanup_temp_files(); // Clean up all generated temp files
    }

    bool sort_file(const std::string& input_file, const std::string& output_file) {
        using Clock = std::chrono::high_resolution_clock;

        auto file_size_bytes = std::filesystem::file_size(input_file);
        double file_size_mb = static_cast<double>(file_size_bytes) / (1024 * 1024);
        std::cout << "Starting OpenMP external merge sort..." << std::endl;
        std::cout << "Input: " << input_file << " (" << file_size_mb << " MB)" << std::endl;
        std::cout << "Output: " << output_file << std::endl;

        auto t1 = Clock::now();
        auto chunk_files = generate_chunk_files_(input_file);
        auto t2 = Clock::now();
        std::cout << "Done: Created " << chunk_files.size() << " chunk files." << std::endl;
        std::chrono::duration<double> chunking_time = t2 - t1;
        std::cout << "[TIMING] Chunking time: " << chunking_time.count() << " s" << std::endl;

        t1 = Clock::now();
        if (!parallel_sort_chunks(chunk_files)) {
            std::cerr << "Failed to create sorted runs" << std::endl;
            return false;
        }
        t2 = Clock::now();
        std::chrono::duration<double> sorting_time = t2 - t1;
        std::cout << "[TIMING] Sorting time: " << sorting_time.count() << " s" << std::endl;

        t1 = Clock::now();
        if (!merge_sorted_files(temp_files_, output_file)) {
            std::cerr << "Failed to merge sorted runs" << std::endl;
            return false;
        }
        t2 = Clock::now();
        std::chrono::duration<double> merging_time = t2 - t1;
        std::cout << "[TIMING] Merging time: " << merging_time.count() << " s" << std::endl;

        double total_time = chunking_time.count() + sorting_time.count() + merging_time.count();
        std::cout << "[TIMING] Total time: " << total_time << " s" << std::endl;

        // print_statistics(total_time);
        
        t1 = Clock::now();
        cleanup_temp_files();
        t2 = Clock::now();
        std::chrono::duration<double> cleanup_time = t2 - t1;
        std::cout << "[TIMING] Cleanup temp files time: " << cleanup_time.count() << " s" << std::endl;
        
        return true;
    }
    
private:

    int input_fd_;
    size_t file_size_;
    uint8_t* mapped_data_;

    void map_input_file(const std::string& input_path) {
        input_fd_ = open(input_path.c_str(), O_RDONLY);
        if (input_fd_ < 0) throw std::runtime_error("Failed to open input file");

        struct stat st;
        if (fstat(input_fd_, &st) < 0) throw std::runtime_error("fstat failed");
        file_size_ = st.st_size;

        mapped_data_ = static_cast<uint8_t*>(
            mmap(NULL, file_size_, PROT_READ, MAP_PRIVATE, input_fd_, 0)
        );
        if (mapped_data_ == MAP_FAILED) throw std::runtime_error("mmap failed");
    }

    void unmap_input_file() {
        munmap(mapped_data_, file_size_);
        close(input_fd_);
    }

struct ChunkMeta {
    uint64_t start_offset;
    uint64_t end_offset;
    size_t size;
    int chunk_id;
};

std::vector<ChunkMeta> compute_logical_chunks(const std::string& input_path) {
    map_input_file(input_path); // Memory-map the input file for fast access

    std::vector<ChunkMeta> logical_chunks;
    size_t estimated_chunk_size = compute_est_chunk_size(input_path);

    // std::cout << "Estimated chunk size: " << (estimated_chunk_size /= (1024*1024)) << " MB" << std::endl;

    uint64_t curr_offset = 0;
    uint64_t chunk_start = 0;
    size_t curr_chunk_size = 0;

    // Loop through file while we have enough bytes to read a full record header (key + length)
    while (curr_offset + sizeof(uint64_t) + sizeof(uint32_t) <= file_size_) {
        // Read key (8 bytes)
        uint64_t key = *reinterpret_cast<uint64_t*>(mapped_data_ + curr_offset);
        curr_offset += sizeof(key);

        // Read length (4 bytes)
        uint32_t len = *reinterpret_cast<uint32_t*>(mapped_data_ + curr_offset);
        curr_offset += sizeof(len);

        // Validate payload size
        if (len < 8 || len > PAYLOAD_MAX) {
            std::cerr << "Invalid payload length at offset " << curr_offset - sizeof(len)
                      << ": " << len << "\n";
            break; // Stop parsing here to avoid undefined behavior
        }

        // Make sure we don't read past file
        if (curr_offset + len > file_size_) {
            std::cerr << "Error record at offset " << curr_offset << " (len = " << len << ")\n";
            break;
        }

        // Compute record size
        size_t rec_size = sizeof(key) + sizeof(len) + len;

        // If estimated chunk size is exceeded, save the current chunk to the list and start a new chunk
        // chunk_acc > 0 to avoid saving an empty chunk
        if (curr_chunk_size + rec_size > estimated_chunk_size && curr_chunk_size > 0) {
            uint64_t chunk_end = curr_offset - sizeof(key) - sizeof(len); // go back to record start (ignore last record)
            
            logical_chunks.push_back({
                .start_offset = chunk_start,
                .end_offset   = chunk_end,
                .size         = chunk_end - chunk_start,
                .chunk_id     = static_cast<int>(logical_chunks.size())
            });

            // start new chunk
            chunk_start = chunk_end;  // new chunk starts at end of previous one
            curr_chunk_size = 0; 
        }

         // Advance curr_offset to skip payload
        curr_offset += len;
        curr_chunk_size += rec_size;
    }

    // Handle final chunk if it exists (put remaining data in final chunk)
    if (curr_chunk_size > 0 && chunk_start < file_size_) {
        logical_chunks.push_back({
            .start_offset = chunk_start,
            .end_offset   = file_size_,
            .size         = file_size_ - chunk_start,
            .chunk_id     = static_cast<int>(logical_chunks.size())
        });
    }

    unmap_input_file();
    return logical_chunks;
}

inline size_t compute_est_chunk_size(const std::string& input_path) {
    size_t input_size = std::filesystem::file_size(input_path);
    // Apply 80% safety margin on max memory per thread
    size_t safe_max_memory_per_thread = static_cast<size_t>(max_memory_per_thread_ * 0.8);
    // min chunks needed to divide the input file so no chunk exceeds max_chunk_size
    // ps: this is just ceiling division, but manual because we want to round up instead of down (default fun)
    size_t min_chunks_needed = (input_size + safe_max_memory_per_thread - 1) / safe_max_memory_per_thread;
    // ensure at least one chunk per thread
    size_t actual_chunk_count = std::max(min_chunks_needed, static_cast<size_t>(num_threads_));
    // final chunk size = total size divided by number of chunks
    size_t estimated_chunk_size = input_size / actual_chunk_count;
    return estimated_chunk_size;
}

std::vector<std::string> generate_chunk_files_(const std::string& input_file) {
        // get logical chunks
        auto logical_chunks = compute_logical_chunks(input_file);
        if (logical_chunks.size() == 1) return {input_file};

        // Map file again for chunk extraction
        map_input_file(input_file);
        std::vector<std::string> chunk_files(logical_chunks.size());

        // Parallel write using OpenMP
        #pragma omp parallel for schedule(static)
        for (int i = 0; i < (int)logical_chunks.size(); ++i) {
            const auto& curr_chunk = logical_chunks[i];
            std::string output_file = temp_dir_ + "/chunk_" + std::to_string(i) + ".bin";
            std::ofstream out{output_file, std::ios::binary};
            // Write chunk data directly from mmap buffer
            // Write 'curr_chunk.size' bytes starting at 'mapped_data_ + curr_chunk.start_offset' (raw pointer offset) to output file (we avoid record parsing)
            out.write(reinterpret_cast<char*>(mapped_data_ + curr_chunk.start_offset), curr_chunk.size);
            chunk_files[i] = output_file;

            // Thread logging (debug)
            // #pragma omp critical
            // std::cout << "[THREAD " << omp_get_thread_num()
            //           << "] chunk " << i
            //           << " (offset=" << curr_chunk.start_offset
            //           << ", size=" << curr_chunk.size << ")\n";
        }

        unmap_input_file();
        return chunk_files;
    }


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
        #pragma omp parallel 
        {
            #pragma omp for schedule(dynamic)
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
        // #pragma omp critical
        // {
        //     std::cout << "Thread " << thread_id << " starting chunk " << chunk_file << std::endl;
        // }

        std::vector<Record> records;
        
        // Open the chunk file
        std::ifstream input(chunk_file, std::ios::binary);
        if (!input.is_open()) {
            std::cerr << "Thread " << thread_id << ": Failed to open chunk file: " << chunk_file << std::endl;
            return false;
        }

        // Initialize variables to track the number of bytes read and the memory usage
        size_t bytes_read = 0;
        
        // Read all records from the chunk file
        while (input.good()) {
            Record record;
            // If a record cannot be read, break out of the loop
            if (!record.read_from_stream(input)) break;
            
            // Update the number of bytes read and the memory usage
            bytes_read += record.total_size();
            
            // Check if the memory usage exceeds the limit per thread
            if (bytes_read > max_memory_per_thread_) {
                static bool warned = false;
                if (!warned) {
                    std::cerr << "Warning: Thread " << thread_id << " exceeded memory budget (" 
                            << bytes_read << " > " << max_memory_per_thread_ << " bytes)" << std::endl;
                    warned = true;
                }
                break;
                // for debug: Do NOT break — continue reading the full chunk
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
        // #pragma omp critical
        // {
        //     total_records_processed_ += records.size();
        //     std::cout << "Thread " << thread_id << ": Processed " << records.size() 
        //               << " records, temp file: " << temp_file << std::endl;
        // }
        
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
};

#endif // OPENMP_EXTERNAL_MERGESORT_H