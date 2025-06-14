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

// Include your existing Record definition and FileGenerator
#include "../include/record.hpp"
#include "../include/record_io.hpp"
// #include "file_generator.h"

/**
 * OpenMP-based parallel external merge sort implementation
 * This class provides parallel sorting for files larger than available RAM
 * using OpenMP pragmas for shared-memory parallelization
 */
class OpenMPExternalMergeSort {
private:
    size_t memory_budget_;          // Maximum memory to use (bytes)
    size_t num_threads_;           // Number of OpenMP threads
    std::string temp_dir_;         // Directory for temporary files
    std::vector<std::string> temp_files_; // List of temporary files created
    
    // Statistics
    size_t total_records_processed_;
    double phase1_time_;
    double phase2_time_;
    
public:
    /**
     * Structure to hold chunk information
     */
    struct ChunkInfo {
        size_t start_offset;
        size_t size;
        size_t estimated_records;
    };

    /**
     * Constructor
     * @param memory_budget Maximum memory to use in bytes (default: 1GB)
     * @param num_threads Number of OpenMP threads (default: hardware concurrency)
     * @param temp_dir Directory for temporary files (default: "./temp")
     */
    OpenMPExternalMergeSort(size_t memory_budget = 1024 * 1024 * 1024, // 1GB
                           size_t num_threads = 0,
                           const std::string& temp_dir = "./temp") 
        : memory_budget_(memory_budget), 
          temp_dir_(temp_dir),
          total_records_processed_(0),
          phase1_time_(0.0),
          phase2_time_(0.0) {
        
        // Set number of threads (0 means use all available cores)
        if (num_threads == 0) {
            num_threads_ = omp_get_max_threads();
        } else {
            num_threads_ = num_threads;
        }
        
        // Set OpenMP thread count
        omp_set_num_threads(num_threads_);
        
        // Create temp directory if it doesn't exist
        std::filesystem::create_directories(temp_dir_);
        
        std::cout << "OpenMP MergeSort initialized with " << num_threads_ 
                  << " threads, memory budget: " << (memory_budget_ / (1024*1024)) 
                  << " MB" << std::endl;
    }
    
    /**
     * Destructor - cleanup temporary files
     */
    ~OpenMPExternalMergeSort() {
        cleanup_temp_files();
    }
    
    /**
     * Main sorting function
     * @param input_file Path to input file to sort
     * @param output_file Path to output sorted file
     * @return true if sorting successful, false otherwise
     */
    bool sort_file(const std::string& input_file, const std::string& output_file) {
        auto start_time = std::chrono::high_resolution_clock::now();
        
        std::cout << "Starting OpenMP external merge sort..." << std::endl;
        std::cout << "Input: " << input_file << std::endl;
        std::cout << "Output: " << output_file << std::endl;
        
        // Phase 1: Create sorted runs (parallel)
        if (!create_sorted_runs_parallel(input_file)) {
            std::cerr << "Failed to create sorted runs" << std::endl;
            return false;
        }
        
        // Phase 2: Merge sorted runs (parallel merge)
        if (!merge_sorted_runs_parallel(output_file)) {
            std::cerr << "Failed to merge sorted runs" << std::endl;
            return false;
        }
        
        auto end_time = std::chrono::high_resolution_clock::now();
        auto total_time = std::chrono::duration<double>(end_time - start_time).count();
        
        print_statistics(total_time);
        cleanup_temp_files();
        
        return true;
    }
    
private:
    /**
     * Phase 1: Create sorted runs using OpenMP parallel for
     * Each thread processes different chunks of the input file
     */
    bool create_sorted_runs_parallel(const std::string& input_file) {
        auto start_time = std::chrono::high_resolution_clock::now();
        
        std::cout << "Phase 1: Creating sorted runs (parallel)..." << std::endl;
        
        // First, analyze file to determine chunk boundaries
        std::vector<ChunkInfo> chunks = analyze_file_for_chunks(input_file);
        
        if (chunks.empty()) {
            std::cerr << "Failed to analyze input file" << std::endl;
            return false;
        }
        
        std::cout << "File analysis complete. Creating " << chunks.size() << " chunks." << std::endl;
        
        // Prepare temp file names (thread-safe)
        temp_files_.resize(chunks.size());
        for (size_t i = 0; i < chunks.size(); ++i) {
            temp_files_[i] = temp_dir_ + "/run_" + std::to_string(i) + ".tmp";
        }
        
        // Parallel processing of chunks
        bool success = true;
        
        // OpenMP parallel for - each thread processes different chunks
        #pragma omp parallel for schedule(dynamic) shared(success)
        for (int i = 0; i < static_cast<int>(chunks.size()); ++i) {
            if (!success) continue; // Skip if another thread failed
            
            int thread_id = omp_get_thread_num();
            
            // Process chunk i
            if (!process_chunk_parallel(input_file, chunks[i], temp_files_[i], thread_id)) {
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
     * Process a single chunk (called by each thread)
     */
    bool process_chunk_parallel(const std::string& input_file, 
                               const ChunkInfo& chunk, 
                               const std::string& temp_file,
                               int thread_id) {
        
        std::vector<Record> records;
        records.reserve(chunk.estimated_records);
        
        // Read chunk data
        std::ifstream input(input_file, std::ios::binary);
        if (!input) {
            std::cerr << "Thread " << thread_id << ": Failed to open input file" << std::endl;
            return false;
        }
        
        // Seek to chunk start
        input.seekg(chunk.start_offset);
        
        size_t bytes_read = 0;
        while (bytes_read < chunk.size && input.good()) {
            Record record;
            if (!record.read_from_stream(input)) {
                break; // End of file or error
            }
            
            records.push_back(std::move(record));
            bytes_read += record.total_size();
            
            // Memory budget check
            if (records.size() * sizeof(Record) > memory_budget_ / num_threads_) {
                std::cerr << "Thread " << thread_id << ": Memory budget exceeded" << std::endl;
                break;
            }
        }
        
        input.close();
        
        if (records.empty()) {
            std::cerr << "Thread " << thread_id << ": No records read from chunk" << std::endl;
            return false;
        }
        
        // Sort records by key
        std::sort(records.begin(), records.end(), 
                 [](const Record& a, const Record& b) {
                     return a.key < b.key;
                 });
        
        // Write sorted records to temp file
        std::ofstream output(temp_file, std::ios::binary);
        if (!output) {
            std::cerr << "Thread " << thread_id << ": Failed to create temp file: " << temp_file << std::endl;
            return false;
        }
        
        for (const auto& record : records) {
            if (!record.write_to_stream(output)) {
                std::cerr << "Thread " << thread_id << ": Failed to write record to temp file" << std::endl;
                return false;
            }
        }
        
        output.close();
        
        #pragma omp critical
        {
            total_records_processed_ += records.size();
            std::cout << "Thread " << thread_id << ": Processed " << records.size() 
                      << " records, temp file: " << temp_file << std::endl;
        }
        
        return true;
    }
    
    /**
     * Phase 2: Merge sorted runs using parallel I/O and task-based merging
     */
    bool merge_sorted_runs_parallel(const std::string& output_file) {
        auto start_time = std::chrono::high_resolution_clock::now();
        
        std::cout << "Phase 2: Merging sorted runs (parallel)..." << std::endl;
        
        if (temp_files_.empty()) {
            std::cerr << "No temp files to merge" << std::endl;
            return false;
        }
        
        // If only one temp file, just copy it
        if (temp_files_.size() == 1) {
            std::filesystem::copy_file(temp_files_[0], output_file);
            auto end_time = std::chrono::high_resolution_clock::now();
            phase2_time_ = std::chrono::duration<double>(end_time - start_time).count();
            return true;
        }
        
        // Multi-way merge using priority queue
        bool success = k_way_merge_parallel(temp_files_, output_file);
        
        auto end_time = std::chrono::high_resolution_clock::now();
        phase2_time_ = std::chrono::duration<double>(end_time - start_time).count();
        
        std::cout << "Phase 2 completed in " << phase2_time_ << " seconds" << std::endl;
        return success;
    }
    
    /**
     * K-way merge using priority queue with parallel I/O buffering
     */
    bool k_way_merge_parallel(const std::vector<std::string>& temp_files, 
                             const std::string& output_file) {
        
        // Priority queue element
        struct MergeElement {
            Record record;
            size_t file_index;
            
            // Min-heap comparison (smallest key first)
            bool operator>(const MergeElement& other) const {
                return record.key > other.record.key;
            }
        };
        
        // Priority queue for k-way merge
        std::priority_queue<MergeElement, std::vector<MergeElement>, std::greater<MergeElement>> pq;
        
        // Open all temp files for reading
        std::vector<std::unique_ptr<std::ifstream>> input_streams;
        input_streams.reserve(temp_files.size());
        
        for (size_t i = 0; i < temp_files.size(); ++i) {
            auto stream = std::make_unique<std::ifstream>(temp_files[i], std::ios::binary);
            if (!stream->good()) {
                std::cerr << "Failed to open temp file: " << temp_files[i] << std::endl;
                return false;
            }
            
            // Read first record from each file
            Record record;
            if (record.read_from_stream(*stream)) {
                pq.push({std::move(record), i});
            }
            
            input_streams.push_back(std::move(stream));
        }
        
        // Open output file
        std::ofstream output(output_file, std::ios::binary);
        if (!output) {
            std::cerr << "Failed to create output file: " << output_file << std::endl;
            return false;
        }
        
        // Merge process
        size_t merged_records = 0;
        while (!pq.empty()) {
            // Get minimum record
            MergeElement min_element = pq.top();
            pq.pop();
            
            // Write to output
            if (!min_element.record.write_to_stream(output)) {
                std::cerr << "Failed to write merged record" << std::endl;
                return false;
            }
            
            merged_records++;
            
            // Read next record from the same file
            Record next_record;
            if (next_record.read_from_stream(*input_streams[min_element.file_index])) {
                pq.push({std::move(next_record), min_element.file_index});
            }
            
            // Progress reporting
            if (merged_records % 10000 == 0) {
                std::cout << "Merged " << merged_records << " records..." << std::endl;
            }
        }
        
        output.close();
        
        std::cout << "Merge completed. Total records merged: " << merged_records << std::endl;
        return true;
    }
    
    /**
     * Analyze file to determine optimal chunk boundaries
     */
    std::vector<ChunkInfo> analyze_file_for_chunks(const std::string& input_file) {
        std::vector<ChunkInfo> chunks;
        
        std::ifstream file(input_file, std::ios::binary);
        if (!file) {
            std::cerr << "Failed to open file for analysis: " << input_file << std::endl;
            return chunks;
        }
        
        // Get file size
        file.seekg(0, std::ios::end);
        size_t file_size = file.tellg();
        file.seekg(0, std::ios::beg);
        
        std::cout << "File size: " << (file_size / (1024*1024)) << " MB" << std::endl;
        
        // Calculate chunk size based on memory budget and number of threads
        size_t chunk_memory = memory_budget_ / num_threads_;
        size_t target_chunk_size = chunk_memory * 0.8; // Leave some buffer
        
        std::cout << "Target chunk size: " << (target_chunk_size / (1024*1024)) << " MB" << std::endl;
        
        size_t current_offset = 0;
        size_t chunk_index = 0;
        
        while (current_offset < file_size) {
            ChunkInfo chunk;
            chunk.start_offset = current_offset;
            
            // Calculate chunk size
            size_t remaining = file_size - current_offset;
            chunk.size = std::min(target_chunk_size, remaining);
            
            // Estimate number of records (rough estimate)
            chunk.estimated_records = chunk.size / 64; // Assume average record size
            
            chunks.push_back(chunk);
            current_offset += chunk.size;
            chunk_index++;
        }
        
        std::cout << "Created " << chunks.size() << " chunks for parallel processing" << std::endl;
        return chunks;
    }
    
    /**
     * Clean up temporary files
     */
    void cleanup_temp_files() {
        for (const auto& temp_file : temp_files_) {
            if (std::filesystem::exists(temp_file)) {
                std::filesystem::remove(temp_file);
            }
        }
        temp_files_.clear();
        
        // Remove temp directory if empty
        if (std::filesystem::exists(temp_dir_) && std::filesystem::is_empty(temp_dir_)) {
            std::filesystem::remove(temp_dir_);
        }
    }
    
    /**
     * Print performance statistics
     */
    void print_statistics(double total_time) {
        std::cout << "\n=== OpenMP MergeSort Performance Statistics ===" << std::endl;
        std::cout << "Total execution time: " << total_time << " seconds" << std::endl;
        std::cout << "Phase 1 (Create runs): " << phase1_time_ << " seconds (" 
                  << (phase1_time_/total_time*100) << "%)" << std::endl;
        std::cout << "Phase 2 (Merge runs): " << phase2_time_ << " seconds (" 
                  << (phase2_time_/total_time*100) << "%)" << std::endl;
        std::cout << "Total records processed: " << total_records_processed_ << std::endl;
        std::cout << "Records per second: " << (total_records_processed_/total_time) << std::endl;
        std::cout << "Number of threads used: " << num_threads_ << std::endl;
        std::cout << "Memory budget: " << (memory_budget_/(1024*1024)) << " MB" << std::endl;
        std::cout << "Temporary files created: " << temp_files_.size() << std::endl;
        std::cout << "===============================================" << std::endl;
    }
};

#endif // OPENMP_EXTERNAL_MERGESORT_H