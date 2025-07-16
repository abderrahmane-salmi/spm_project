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
    OpenMPExternalMergeSort(size_t memory_budget = 28 * 1024 * 1024 * 1024, // default
                           size_t num_threads = 0,
                           const std::string& temp_dir = "./temp_omp") 
        : memory_budget_(memory_budget), 
          temp_dir_(temp_dir)
        {
        
        // set number of threads
        num_threads_ = (num_threads == 0) ? omp_get_max_threads() : num_threads;
        omp_set_num_threads(num_threads_);

        max_memory_per_thread_ = memory_budget_ / num_threads_;

        // confirm we are using the specified the num of threads
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
    }
    
    // === Destructor ===
    ~OpenMPExternalMergeSort() {
        cleanup_temp_files(); // Clean up all generated temp files
    }

    double sort_file(const std::string& input_file, const std::string& output_file) {
        auto file_size_bytes = std::filesystem::file_size(input_file);
        double file_size_mb = static_cast<double>(file_size_bytes) / (1024 * 1024);

        std::cout << "Starting OpenMP external merge sort...\n"
                << "Memory budget: " << (memory_budget_ / (1024 * 1024)) << " MB\n"
                << "Num threads: " << num_threads_ << "\n"
                << "Input: " << input_file << " (" << file_size_mb << " MB)\n"
                << "Output: " << output_file << std::endl;

        double t1, t2;

        // Chunking
        t1 = omp_get_wtime();
        auto chunk_files = generate_chunk_files_(input_file);
        t2 = omp_get_wtime();
        std::cout << "Done: Created " << chunk_files.size() << " chunk files." << std::endl;
        std::cout << "[TIMING] Chunking time: " << (t2 - t1) << " s" << std::endl;

        // Sorting
        t1 = omp_get_wtime();
        if (!parallel_sort_chunks(chunk_files)) {
            std::cerr << "Failed to create sorted runs" << std::endl;
            return false;
        }
        t2 = omp_get_wtime();
        std::cout << "[TIMING] Sorting time: " << (t2 - t1) << " s" << std::endl;

        // Merging
        t1 = omp_get_wtime();
        if (!merge_sorted_files(temp_files_, output_file)) {
            std::cerr << "Failed to merge sorted runs" << std::endl;
            return false;
        }
        t2 = omp_get_wtime();
        std::cout << "[TIMING] Merging time: " << (t2 - t1) << " s" << std::endl;

        double total_time = (t2 - omp_get_wtime()) + (t2 - t1); 
        std::cout << "[TIMING] Total time: " << total_time << " s" << std::endl;

        // Cleanup
        t1 = omp_get_wtime();
        cleanup_temp_files();
        t2 = omp_get_wtime();
        std::cout << "[TIMING] Cleanup temp files time: " << (t2 - t1) << " s" << std::endl;

        return total_time;
    }
    
private:

    int input_fd_;
    size_t file_size_;
    uint8_t* mapped_data_;

    // use mmap() to map the input file into memory, which allows very fast access without copying the whole file into a buffer
    void map_input_file(const std::string& input_path) {
        // Open the input file in read-only mode
        input_fd_ = open(input_path.c_str(), O_RDONLY);
        if (input_fd_ < 0) throw std::runtime_error("Failed to open input file");

        // Retrieve the file's metadata (ex: size) using fstat
        struct stat st;
        if (fstat(input_fd_, &st) < 0) throw std::runtime_error("fstat failed");
        file_size_ = st.st_size;

        // Memory-map the entire file into the process's address space
        // mmap returns a pointer to the mapped memory region
        // - NULL: Let the OS choose the address
        // - file_size_: Map the entire file
        // - PROT_READ: Pages are read-only
        // - MAP_PRIVATE: Changes are private (copy-on-write, not visible to other processes)
        // - input_fd_: File descriptor
        // - 0: Offset in file (start at beginning)
        mapped_data_ = static_cast<uint8_t*>(
            mmap(NULL, file_size_, PROT_READ, MAP_PRIVATE, input_fd_, 0)
        );
        if (mapped_data_ == MAP_FAILED) throw std::runtime_error("mmap failed");
    }

    // unmap the input file from memory and close the file descriptor
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
    uint64_t curr_offset = 0;
    uint64_t chunk_start = 0;
    size_t curr_chunk_size = 0;

    // Loop through file while we have enough bytes to read a full record header (key + length)
    while (curr_offset + sizeof(uint64_t) + sizeof(uint32_t) <= file_size_) {
        // Read key (8 bytes) (via pointer casting and dereferencing)
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
        
        // Process each chunk in parallel
        #pragma omp parallel 
        {
            #pragma omp for schedule(dynamic)
            for (int i = 0; i < static_cast<int>(chunk_files.size()); ++i) {
                // Process the current chunk in parallel
                int thread_id = omp_get_thread_num();
                process_chunk_parallel(chunk_files[i], temp_files_[i], thread_id);
            }
        }
        
        return true;
    }
    
    // Sort the records within a single chunk and write the sorted records to a temp file
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
            if (!record.read_from_stream(input)) break;
            
            // Check if the memory usage exceeds the memory limit per thread
            bytes_read += record.total_size();
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
        
        // Open the temporary file
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

        output.close();
        return true;
    }
    
    // Deletes temporary files created during processing.
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

#endif