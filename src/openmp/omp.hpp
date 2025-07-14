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

        std::cout << "Starting OpenMP external merge sort..." << std::endl;
        std::cout << "Input: " << input_file << std::endl;
        std::cout << "Output: " << output_file << std::endl;

        auto t1 = Clock::now();
        auto chunk_files = generate_chunk_files2(input_file);
        auto t2 = Clock::now();
        std::cout << "Phase 1: Created " << chunk_files.size() << " chunk files." << std::endl;
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

std::vector<ChunkMeta> analyze_file_for_chunks2(const std::string& input_path) {
    map_input_file(input_path);

    std::vector<ChunkMeta> chunks;
    size_t est_size = compute_optimal_chunk_size2(input_path);

    uint64_t offset = 0;
    uint64_t chunk_start = 0;
    size_t chunk_acc = 0;

    while (offset + sizeof(uint64_t) + sizeof(uint32_t) <= file_size_) {
        // Read key
        uint64_t key = *reinterpret_cast<uint64_t*>(mapped_data_ + offset);
        offset += sizeof(key);

        // Read length
        uint32_t len = *reinterpret_cast<uint32_t*>(mapped_data_ + offset);
        offset += sizeof(len);

        // Sanity check on length
        if (len < 8 || len > PAYLOAD_MAX) {
            std::cerr << "Invalid payload length at offset " << offset - sizeof(len)
                      << ": " << len << "\n";
            break; // Stop parsing here to avoid undefined behavior
        }

        // Make sure we don't read past file
        if (offset + len > file_size_) {
            std::cerr << "Truncated record at offset " << offset << " (len = " << len << ")\n";
            break;
        }

        size_t rec_size = sizeof(key) + sizeof(len) + len;

        // If estimated chunk size exceeded, flush the current chunk
        if (chunk_acc + rec_size > est_size && chunk_acc > 0) {
            chunks.push_back({
                .start_offset = chunk_start,
                .end_offset   = offset - sizeof(key) - sizeof(len), // go back to record start
                .size         = (offset - sizeof(key) - sizeof(len)) - chunk_start,
                .chunk_id     = static_cast<int>(chunks.size())
            });

            chunk_start = offset - sizeof(key) - sizeof(len);  // new chunk starts here
            chunk_acc = 0;
        }

        offset += len;
        chunk_acc += rec_size;
    }

    // Handle final chunk if any
    if (chunk_acc > 0 && chunk_start < file_size_) {
        chunks.push_back({
            .start_offset = chunk_start,
            .end_offset   = file_size_,
            .size         = file_size_ - chunk_start,
            .chunk_id     = static_cast<int>(chunks.size())
        });
    }

    std::cout << "[INFO] Generated " << chunks.size() << " chunk"
              << (chunks.size() == 1 ? "" : "s") << " using record-aware mmap.\n";

    unmap_input_file();
    return chunks;
}

inline size_t compute_optimal_chunk_size2(const std::string& input_path) {
    size_t input_size = std::filesystem::file_size(input_path);
    size_t max_chunk_size = memory_budget_ / num_threads_;
    size_t min_chunks_needed = (input_size + max_chunk_size - 1) / max_chunk_size;
    size_t K = std::max(min_chunks_needed, static_cast<size_t>(num_threads_));
    size_t estimated_chunk_size = input_size / K;
    return estimated_chunk_size;
}

std::vector<std::string> generate_chunk_files2(const std::string& input_file) {
        auto chunks = analyze_file_for_chunks2(input_file);
        if (chunks.size() == 1) return {input_file};

        map_input_file(input_file);
        std::vector<std::string> chunk_files(chunks.size());

        #pragma omp parallel for schedule(static)
        for (int i = 0; i < (int)chunks.size(); ++i) {
            const auto& m = chunks[i];
            std::string out = temp_dir_ + "/chunk_" + std::to_string(i) + ".bin";
            std::ofstream of{out, std::ios::binary};
            of.write(reinterpret_cast<char*>(mapped_data_ + m.start_offset), m.size);
            chunk_files[i] = out;

            #pragma omp critical
            std::cout << "[THREAD " << omp_get_thread_num()
                      << "] chunk " << i
                      << " (offset=" << m.start_offset
                      << ", size=" << m.size << ")\n";
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
        // size_t bytes_read = 0;
        // size_t memory_limit_per_thread = memory_budget_ / num_threads_;
        
        // Read all records from the chunk file
        while (input.good()) {
            Record record;
            // If a record cannot be read, break out of the loop
            if (!record.read_from_stream(input)) break;
            
            // Update the number of bytes read and the memory usage
            // bytes_read += record.total_size();
            
            // Check if the memory usage exceeds the limit per thread
            // if (bytes_read > memory_limit_per_thread) {
            //     static bool warned = false;
            //     if (!warned) {
            //         std::cerr << "Warning: Thread " << thread_id << " exceeded memory budget (" 
            //                 << bytes_read << " > " << memory_limit_per_thread << " bytes)" << std::endl;
            //         warned = true;
            //     }
            //     // Do NOT break — continue reading the full chunk
            // }
            
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