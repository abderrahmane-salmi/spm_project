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
    struct ChunkInfo {
        size_t start_offset;
        size_t size;
        size_t estimated_records;
    };

    OpenMPExternalMergeSort(size_t memory_budget = 1024 * 1024 * 1024, // 1GB
                           size_t num_threads = 0,
                           const std::string& temp_dir = "./temp") 
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
    
    ~OpenMPExternalMergeSort() {
        cleanup_temp_files();
    }
    
    bool sort_file(const std::string& input_file, const std::string& output_file) {
        auto start_time = std::chrono::high_resolution_clock::now();
        
        std::cout << "Starting OpenMP external merge sort..." << std::endl;
        std::cout << "Input: " << input_file << std::endl;
        std::cout << "Output: " << output_file << std::endl;
        
        if (!create_sorted_runs_parallel(input_file)) {
            std::cerr << "Failed to create sorted runs" << std::endl;
            return false;
        }
        
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
    bool create_sorted_runs_parallel(const std::string& input_file) {
        auto start_time = std::chrono::high_resolution_clock::now();
        
        std::cout << "Phase 1: Creating sorted runs (parallel)..." << std::endl;
        
        std::vector<ChunkInfo> chunks = analyze_file_for_chunks(input_file);
        if (chunks.empty()) {
            std::cerr << "Failed to analyze input file" << std::endl;
            return false;
        }
        
        std::cout << "File analysis complete. Creating " << chunks.size() << " chunks." << std::endl;
        
        temp_files_.resize(chunks.size());
        for (size_t i = 0; i < chunks.size(); ++i) {
            temp_files_[i] = temp_dir_ + "/run_" + std::to_string(i) + ".tmp";
        }
        
        bool success = true;
        
        #pragma omp parallel for schedule(dynamic) shared(success)
        for (int i = 0; i < static_cast<int>(chunks.size()); ++i) {
            if (!success) continue;
            
            int thread_id = omp_get_thread_num();
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
    
    bool process_chunk_parallel(const std::string& input_file, 
                               const ChunkInfo& chunk, 
                               const std::string& temp_file,
                               int thread_id) {
        
        std::vector<Record> records;
        records.reserve(chunk.estimated_records);
        
        std::ifstream input(input_file, std::ios::binary);
        if (!input) {
            std::cerr << "Thread " << thread_id << ": Failed to open input file" << std::endl;
            return false;
        }
        
        input.seekg(chunk.start_offset);
        size_t bytes_read = 0;
        size_t memory_limit_per_thread = memory_budget_ / num_threads_;
        
        while (bytes_read < chunk.size && input.good()) {
            Record record;
            if (!record.read_from_stream(input)) break;
            
            bytes_read += record.total_size();
            // Approximate memory usage: size of record + payload size
            size_t approx_mem_usage = bytes_read; // rough but sufficient
            
            if (approx_mem_usage > memory_limit_per_thread) {
                std::cerr << "Thread " << thread_id << ": Memory budget exceeded" << std::endl;
                break;
            }
            
            records.push_back(std::move(record));
        }
        input.close();
        
        if (records.empty()) {
            std::cerr << "Thread " << thread_id << ": No records read from chunk" << std::endl;
            return false;
        }
        
        std::sort(records.begin(), records.end(), 
                 [](const Record& a, const Record& b) {
                     return a.key < b.key;
                 });
        
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
    
    bool merge_sorted_runs_parallel(const std::string& output_file) {
        auto start_time = std::chrono::high_resolution_clock::now();
        
        std::cout << "Phase 2: Merging sorted runs (parallel)..." << std::endl;
        
        if (temp_files_.empty()) {
            std::cerr << "No temp files to merge" << std::endl;
            return false;
        }
        
        if (temp_files_.size() == 1) {
            std::filesystem::copy_file(temp_files_[0], output_file, std::filesystem::copy_options::overwrite_existing);
            auto end_time = std::chrono::high_resolution_clock::now();
            phase2_time_ = std::chrono::duration<double>(end_time - start_time).count();
            return true;
        }
        
        bool success = k_way_merge_parallel(temp_files_, output_file);
        
        auto end_time = std::chrono::high_resolution_clock::now();
        phase2_time_ = std::chrono::duration<double>(end_time - start_time).count();
        
        std::cout << "Phase 2 completed in " << phase2_time_ << " seconds" << std::endl;
        return success;
    }
    
    bool k_way_merge_parallel(const std::vector<std::string>& temp_files, 
                             const std::string& output_file) {
        struct MergeElement {
            Record record;
            size_t file_index;
            bool operator>(const MergeElement& other) const {
                return record.key > other.record.key;
            }
        };
        
        std::priority_queue<MergeElement, std::vector<MergeElement>, std::greater<MergeElement>> pq;
        std::vector<std::unique_ptr<std::ifstream>> input_streams(temp_files.size());
        
        for (size_t i = 0; i < temp_files.size(); ++i) {
            input_streams[i] = std::make_unique<std::ifstream>(temp_files[i], std::ios::binary);
            if (!input_streams[i]->is_open()) {
                std::cerr << "Failed to open temp file for merging: " << temp_files[i] << std::endl;
                return false;
            }
            Record rec;
            if (rec.read_from_stream(*input_streams[i])) {
                pq.push(MergeElement{std::move(rec), i});
            }
        }
        
        std::ofstream output(output_file, std::ios::binary);
        if (!output) {
            std::cerr << "Failed to open output file for final merge" << std::endl;
            return false;
        }
        
        size_t merged_count = 0;
        while (!pq.empty()) {
            MergeElement smallest = std::move(const_cast<MergeElement&>(pq.top()));
            pq.pop();
            
            if (!smallest.record.write_to_stream(output)) {
                std::cerr << "Failed to write record during merge" << std::endl;
                return false;
            }
            ++merged_count;
            
            Record next_rec;
            if (next_rec.read_from_stream(*input_streams[smallest.file_index])) {
                pq.push(MergeElement{std::move(next_rec), smallest.file_index});
            }
        }
        
        std::cout << "Merged " << merged_count << " records into output file" << std::endl;
        output.close();
        
        return true;
    }
    
    std::vector<ChunkInfo> analyze_file_for_chunks(const std::string& input_file) {
        std::vector<ChunkInfo> chunks;
        
        std::ifstream input(input_file, std::ios::binary | std::ios::ate);
        if (!input) {
            std::cerr << "Failed to open input file for analysis" << std::endl;
            return chunks;
        }
        
        size_t file_size = static_cast<size_t>(input.tellg());
        input.seekg(0);
        
        size_t offset = 0;
        
        while (offset < file_size) {
            size_t chunk_size = std::min(memory_budget_ / num_threads_, file_size - offset);
            
            input.seekg(offset);
            size_t bytes_read = 0;
            size_t record_count = 0;
            
            while (bytes_read < chunk_size && input.good()) {
                Record record;
                std::streampos before = input.tellg();
                if (!record.read_from_stream(input)) break;
                std::streampos after = input.tellg();
                if (before == -1 || after == -1) break;
                
                size_t record_size = static_cast<size_t>(after - before);
                bytes_read += record_size;
                ++record_count;
            }
            
            chunks.push_back(ChunkInfo{offset, bytes_read, record_count});
            offset += bytes_read;
        }
        
        input.close();
        return chunks;
    }
    
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
