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
    // size_t total_records_processed_;
    
public:
    OpenMPExternalMergeSort(size_t memory_budget = 1024 * 1024 * 1024, // default: 1GB
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
        std::vector<std::string> chunk_files;
        chunk_and_sort(input_file, num_threads_, chunk_files);
        auto t2 = Clock::now();
        std::cout << "Created " << chunk_files.size() << " chunk files." << std::endl;
        std::chrono::duration<double> chunking_time = t2 - t1;
        std::cout << "[TIMING] Chunking and Sorting time: " << chunking_time.count() << " s" << std::endl;

        // t1 = Clock::now();
        // if (!parallel_sort_chunks(chunk_files)) {
        //     std::cerr << "Failed to create sorted runs" << std::endl;
        //     return false;
        // }
        // t2 = Clock::now();
        // std::chrono::duration<double> sorting_time = t2 - t1;
        // std::cout << "[TIMING] Sorting time: " << sorting_time.count() << " s" << std::endl;

        t1 = Clock::now();
        multistage_merge(chunk_files, num_threads_, output_file);

        // if (!merge_sorted_files(temp_files_, output_file)) {
        //     std::cerr << "Failed to merge sorted runs" << std::endl;
        //     return false;
        // }
        t2 = Clock::now();
        std::chrono::duration<double> merging_time = t2 - t1;
        std::cout << "[TIMING] Merging time: " << merging_time.count() << " s" << std::endl;

        double total_time = chunking_time.count() + merging_time.count();
        // double total_time = chunking_time.count() + sorting_time.count() + merging_time.count();
        std::cout << "[TIMING] Total time: " << total_time << " s" << std::endl;

        // print_statistics(total_time);
        
        t1 = Clock::now();
        cleanup_temp_files();
        t2 = Clock::now();
        std::chrono::duration<double> cleanup_time = t2 - t1;
        std::cout << "[TIMING] Cleanup temp files time: " << cleanup_time.count() << " s" << std::endl;
        
        return true;
    }

    // bool sort_file(const std::string& input_file, const std::string& output_file) {
    //     using Clock = std::chrono::high_resolution_clock;

    //     std::cout << "Starting OpenMP external merge sort..." << std::endl;
    //     std::cout << "Input: " << input_file << std::endl;
    //     std::cout << "Output: " << output_file << std::endl;

    //     auto t1 = Clock::now();
    //     auto chunk_files = generate_chunk_files(input_file, memory_budget_ * 0.8, temp_dir_);
    //     auto t2 = Clock::now();
    //     std::cout << "Phase 1: Created " << chunk_files.size() << " chunk files." << std::endl;
    //     std::chrono::duration<double> chunking_time = t2 - t1;
    //     std::cout << "[TIMING] Chunking time: " << chunking_time.count() << " s" << std::endl;

    //     t1 = Clock::now();
    //     if (!parallel_sort_chunks(chunk_files)) {
    //         std::cerr << "Failed to create sorted runs" << std::endl;
    //         return false;
    //     }
    //     t2 = Clock::now();
    //     std::chrono::duration<double> sorting_time = t2 - t1;
    //     std::cout << "[TIMING] Sorting time: " << sorting_time.count() << " s" << std::endl;

    //     t1 = Clock::now();
    //     if (!merge_sorted_files(temp_files_, output_file)) {
    //         std::cerr << "Failed to merge sorted runs" << std::endl;
    //         return false;
    //     }
    //     t2 = Clock::now();
    //     std::chrono::duration<double> merging_time = t2 - t1;
    //     std::cout << "[TIMING] Merging time: " << merging_time.count() << " s" << std::endl;

    //     double total_time = chunking_time.count() + sorting_time.count() + merging_time.count();
    //     std::cout << "[TIMING] Total time: " << total_time << " s" << std::endl;

    //     // print_statistics(total_time);
        
    //     t1 = Clock::now();
    //     cleanup_temp_files();
    //     t2 = Clock::now();
    //     std::chrono::duration<double> cleanup_time = t2 - t1;
    //     std::cout << "[TIMING] Cleanup temp files time: " << cleanup_time.count() << " s" << std::endl;
        
    //     return true;
    // }
    
private:

    void chunk_and_sort(const std::string& input_file, int num_threads, std::vector<std::string>& out_chunk_files) {
        std::ifstream in(input_file, std::ios::binary);
        if (!in) {
            std::cerr << "Cannot open input file.\n";
            return;
        }

        size_t input_size = std::filesystem::file_size(input_file);
        size_t max_chunk_size = memory_budget_ / num_threads_;
        size_t min_chunks_needed = (input_size + max_chunk_size - 1) / max_chunk_size;
        size_t K = std::max(min_chunks_needed, static_cast<size_t>(num_threads_));
        size_t estimated_chunk_size = input_size / K;

        int chunk_id = 0;

        #pragma omp parallel num_threads(num_threads)
        {
            #pragma omp single
            {
                while (true) {
                    std::vector<Record> chunk;
                    int my_id;

                    #pragma omp critical
                    {
                        if (!in.eof()) {
                            chunk = read_chunk(in, estimated_chunk_size);
                            my_id = chunk_id++;
                        }
                    }

                    if (chunk.empty()) break;

                    std::string filename = chunk_filename(my_id);
                    #pragma omp task firstprivate(chunk, filename)
                    {
                        std::vector<Record> local_chunk = chunk;
                        std::sort(local_chunk.begin(), local_chunk.end(), [](const Record& a, const Record& b) {
                            return a.key < b.key;
                        });
                        write_chunk_to_file(local_chunk, filename);

                        #pragma omp critical
                        {
                            out_chunk_files.push_back(filename);
                        }
                    }
                }

                #pragma omp taskwait
            }
        }
    }

    std::vector<Record> read_chunk(std::ifstream& in, size_t max_chunk_size) {
        std::vector<Record> chunk;
        size_t current_chunk_size = 0;
        Record record;

        while (record.read_from_stream(in)) {
            size_t record_size = RECORD_HEADER_SIZE + record.len;
            if (current_chunk_size + record_size > max_chunk_size) {
                // Don't include this record, leave it for the next chunk
                in.seekg(-static_cast<std::streamoff>(record_size), std::ios::cur);
                break;
            }
            current_chunk_size += record_size;
            chunk.emplace_back(std::move(record));
        }

        return chunk;
    }

    void write_chunk_to_file(const std::vector<Record>& chunk, const std::string& filename) {
        std::ofstream out(filename, std::ios::binary);
        for (const auto& record : chunk) {
            record.write_to_stream(out);
        }
    }

    std::string chunk_filename(int id) {
        std::ostringstream oss;
        oss << "chunk_" << std::setw(4) << std::setfill('0') << id << ".bin";
        return oss.str();
    }

    std::string merge_filename(int pass, int id) {
        std::ostringstream oss;
        oss << "merge_pass_" << pass << "_" << id << ".bin";
        return oss.str();
    }

    void kway_merge(const std::vector<std::string>& input_files, const std::string& output_file) {
        struct HeapItem {
            Record record;
            int stream_index;

            bool operator<(const HeapItem& a) const {
                return record.key > a.record.key;  // min-heap
            }
        };

        int k = input_files.size();
        std::vector<std::ifstream> streams(k);
        std::priority_queue<HeapItem> min_heap;

        for (int i = 0; i < k; ++i) {
            streams[i].open(input_files[i], std::ios::binary);
            Record rec;
            if (rec.read_from_stream(streams[i])) {
                min_heap.push({rec, i});
            }
        }

        std::ofstream out(output_file, std::ios::binary);

        while (!min_heap.empty()) {
            auto [record, idx] = min_heap.top();
            min_heap.pop();
            record.write_to_stream(out);

            Record next;
            if (next.read_from_stream(streams[idx])) {
                min_heap.push({next, idx});
            }
        }

        for (auto& s : streams) s.close();
    }

    void multistage_merge(std::vector<std::string>& files, int num_threads, std::string final_output) {
        int pass = 0;
        int fanin = std::min(8, num_threads);

        while (files.size() > 1) {
            std::vector<std::string> next_pass_files;
            int total_groups = (files.size() + fanin - 1) / fanin;

            #pragma omp parallel for num_threads(num_threads) schedule(dynamic)
            for (int i = 0; i < total_groups; ++i) {
                int start = i * fanin;
                int end = std::min(start + fanin, static_cast<int>(files.size()));
                std::vector<std::string> group(files.begin() + start, files.begin() + end);

                std::string out_file = merge_filename(pass, i);
                kway_merge(group, out_file);

                #pragma omp critical
                {
                    next_pass_files.push_back(out_file);
                }
            }

            files = next_pass_files;
            pass++;
        }

        std::filesystem::rename(files.front(), final_output);
    }







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
    
    void print_statistics(double total_time) {
        std::cout << "OpenMP External Merge Sort statistics:" << std::endl;
        // std::cout << "Total records processed: " << total_records_processed_ << std::endl;
        std::cout << "Total elapsed time: " << total_time << " seconds" << std::endl;
    }
};

#endif // OPENMP_EXTERNAL_MERGESORT_H