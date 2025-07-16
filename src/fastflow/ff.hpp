#ifndef FASTFLOW_MERGESORT_HPP
#define FASTFLOW_MERGESORT_HPP

#include <ff/ff.hpp>
#include <ff/farm.hpp>
#include <ff/utils.hpp>
#include <vector>
#include <string>
#include <fstream>
#include <algorithm>
#include <queue>
#include <filesystem>
#include <memory>
#include <chrono>

#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>

#include "../include/record.hpp"
#include "../chunking/chunking.hpp"
#include "../merging/merging.hpp"

using namespace ff;

namespace fs = std::filesystem;

struct ChunkTask {
    int chunk_id;
    uint8_t* data;
    size_t size;
    std::string output_path;
};

// ------------------------
// FastFlow Node: Chunk Emitter
// ------------------------
// Divide the input file into logical chunks and send each chunk to a worker to process it
class ChunkEmitter : public ff_node {
private:
    const std::string input_file_;
    std::string temp_dir_;
    uint8_t* mmap_ptr_{nullptr};
    size_t file_size_{0};
    size_t num_workers_;
    size_t per_worker_budget_;
    size_t safe_budget_;

public:
    ChunkEmitter(const std::string& infile, const std::string& temp_dir, size_t num_workers, size_t memory_budget)
        : input_file_(infile), temp_dir_(temp_dir), num_workers_(num_workers) {
        safe_budget_ = static_cast<size_t>(memory_budget * 0.8);
        per_worker_budget_ = safe_budget_ / num_workers_;
        map_input_file(infile);
    }

    ~ChunkEmitter() {
        munmap(mmap_ptr_, file_size_);
    }

    void map_input_file(const std::string& input_path) {
        // Open the input file in read-only mode
        int file_descriptor = open(input_file_.c_str(), O_RDONLY);
        if (file_descriptor < 0) throw std::runtime_error("open input failed");

        // Retrieve the file's metadata (ex: size) using fstat
        struct stat st;
        fstat(file_descriptor, &st);
        file_size_ = st.st_size;

        // Memory-map the entire file into the process's address space
        // mmap returns a pointer to the mapped memory region
        // - NULL: Let the OS choose the address
        // - file_size_: Map the entire file
        // - PROT_READ: Pages are read-only
        // - MAP_PRIVATE: Changes are private (copy-on-write, not visible to other processes)
        // - file_descriptor: File descriptor
        // - 0: Offset in file (start at beginning)
        mmap_ptr_ = static_cast<uint8_t*>(
            mmap(nullptr, file_size_, PROT_READ, MAP_PRIVATE, file_descriptor, 0)
        );
        close(file_descriptor);
        if (mmap_ptr_ == MAP_FAILED) throw std::runtime_error("mmap failed");
    }

    void* svc(void*) override {
        // Estimate the number of chunks needed
        size_t estimated_chunk_count = std::max<size_t>(
            (file_size_ + per_worker_budget_ - 1) / per_worker_budget_, // ceiling division because we want to round up not down (so we don't miss any chunks)
            num_workers_
        );
        size_t target_chunk_size = file_size_ / estimated_chunk_count;

        size_t current_offset = 0; 
        size_t curr_chunk_start_offset = 0;   
        size_t curr_chunk_accumulated_size = 0; 
        int curr_chunk_id = 0;

        // Loop through file while we have enough bytes to read a full record header (key + length)
        while (current_offset + sizeof(uint64_t) + sizeof(uint32_t) <= file_size_) {
            // read key (8 bytes)
            uint64_t key = *reinterpret_cast<uint64_t*>(mmap_ptr_ + current_offset);
            current_offset += sizeof(key);

            // read payload length (4 bytes)
            uint32_t payload_len = *reinterpret_cast<uint32_t*>(mmap_ptr_ + current_offset);
            current_offset += sizeof(payload_len);

            // Validate payload size
            if (payload_len < 8 || payload_len > PAYLOAD_MAX) {
                std::cerr << "Invalid payload length at offset " << current_offset - sizeof(payload_len)
                      << ": " << payload_len << "\n";
                break;
            }
            
            // Make sure we don't read past file
            if (current_offset + payload_len > file_size_) {
                std::cerr << "Error record at offset " << current_offset << " (len = " << payload_len << ")\n";
                break;
            }

            // Compute record size
            size_t record_size = sizeof(key) + sizeof(payload_len) + payload_len;

            // If estimated chunk size is exceeded, save the current chunk to the list and start a new chunk
            // chunk_acc > 0 to avoid saving an empty chunk
            if (curr_chunk_accumulated_size + record_size > target_chunk_size && curr_chunk_accumulated_size > 0) {
                auto chunk_output_path = fs::path(temp_dir_) / ("chunk_" + std::to_string(curr_chunk_id) + ".bin");
                auto* task = new ChunkTask{
                    curr_chunk_id,
                    mmap_ptr_ + curr_chunk_start_offset,
                    current_offset - sizeof(key) - sizeof(payload_len) - curr_chunk_start_offset, // size of current chunk
                    chunk_output_path.string()
                };

                // Send this task to one of the available workers for processing
                ff_send_out(task); 
                
                // Start a new chunk
                curr_chunk_id++;
                curr_chunk_start_offset = current_offset - sizeof(key) - sizeof(payload_len); // restart chunk
                curr_chunk_accumulated_size = 0;
            }

            // Move to the next record (advance offset to skip payload)
            current_offset += payload_len;
            curr_chunk_accumulated_size += record_size;
        }

        // Handle final chunk if it exists (put remaining data in final chunk)
        if (curr_chunk_accumulated_size > 0 && curr_chunk_start_offset < file_size_) {
            auto chunk_output_path = fs::path(temp_dir_) / ("chunk_" + std::to_string(curr_chunk_id) + ".bin");
            auto* task = new ChunkTask{
                curr_chunk_id,
                mmap_ptr_ + curr_chunk_start_offset,
                file_size_ - curr_chunk_start_offset,
                chunk_output_path.string()
            };

            // Send this task to one of the available workers for processing
            ff_send_out(task);
        }

        return EOS; // End of stream
    }
};

// ------------------------
// FastFlow Node: Chunk Worker
// ------------------------
// Parse records from the chunk, sort them, write a sorted temp file, send it to the collector for merge
class ChunkWorker : public ff_node {
private:
    size_t memory_budget_per_worker_;

public:
    ChunkWorker(size_t memory_budget_per_worker)
        : memory_budget_per_worker_(memory_budget_per_worker) {}

    void* svc(void* task_ptr) override {
        auto* chunk_task = static_cast<ChunkTask*>(task_ptr);

        // --- Step 1: Parse records from the chunk ---
        std::vector<Record> parsed_records;
        size_t total_bytes_read = 0;
        size_t offset_in_chunk = 0;

        // Loop through the chunk buffer and extract records
        while (offset_in_chunk + sizeof(uint64_t) + sizeof(uint32_t) <= chunk_task->size) {
            // Read key (8 bytes) (via pointer casting and dereferencing)
            uint64_t key = *reinterpret_cast<uint64_t*>(chunk_task->data + offset_in_chunk);
            offset_in_chunk += sizeof(key);

            // Read payload length (4 bytes)
            uint32_t payload_len = *reinterpret_cast<uint32_t*>(chunk_task->data + offset_in_chunk);
            offset_in_chunk += sizeof(payload_len);

            // Validate payload length
            if (payload_len < 8 || payload_len > PAYLOAD_MAX || offset_in_chunk + payload_len > chunk_task->size)
                break;

            // Create a Record object from parsed data
            Record record(key, payload_len, reinterpret_cast<char*>(chunk_task->data + offset_in_chunk));
            offset_in_chunk += payload_len;

            total_bytes_read += record.total_size();

            // Stop if this chunk exceeds the per-worker memory budget
            if (total_bytes_read > memory_budget_per_worker_) {
                std::cerr << "[warning] Worker for chunk " << chunk_task->chunk_id
                          << " exceeded its memory budget\n";
                break;
            }

            parsed_records.push_back(std::move(record));
        }

        // --- Step 2: Sort parsed records by their key ---
        std::sort(parsed_records.begin(), parsed_records.end(),
                  [](const Record& a, const Record& b) {
                      return a.key < b.key;
                  });

        // --- Step 3: Write sorted records to output file ---
        std::ofstream output_file(chunk_task->output_path, std::ios::binary);
        for (const auto& record : parsed_records) {
            record.write_to_stream(output_file);
        }

        return chunk_task;  // Return to collector
    }
};

// ------------------------
// FastFlow Node: Chunk Collector
// ------------------------
// Collect sorted chunk files and merge them
class ChunkCollector : public ff_node {
private:
    std::vector<std::string> sorted_chunk_paths_;
    std::string final_output_path_;

public:
    explicit ChunkCollector(const std::string& output_path)
        : final_output_path_(output_path) {}

    /**
     * Called each time a worker sends output
     * It collects paths to the sorted chunk files to merge them later
     */
    void* svc(void* task_ptr) override {
        auto* chunk_task = static_cast<ChunkTask*>(task_ptr);
        
        // Save the path to this chunk’s sorted file
        sorted_chunk_paths_.push_back(chunk_task->output_path);
        
        // Clean up the memory used by the task
        delete chunk_task;

        // Continue processing more tasks
        return GO_ON;
    }

    /**
     * Called after all chunk tasks have been processed by workers
     * Launches the final merge of all sorted chunk files
     */
    void svc_end() override {
        if (!sorted_chunk_paths_.empty()) {
            merge_sorted_files(sorted_chunk_paths_, final_output_path_);
        }
    }
};

// ------------------------
// Main class: FastFlowExternalMergeSort
// ------------------------
// Coordinates the whole external merge sort: chunking, sorting, merging.
class FastFlowExternalMergeSort {
private:
    size_t memory_budget;
    std::string temp_dir; // Directory for temporary files
    int num_workers;

public:
    FastFlowExternalMergeSort(size_t memory_budget_bytes = 256 * 1024 * 1024, int workers = 4)
        : memory_budget(memory_budget_bytes), num_workers(workers) {
        temp_dir = "temp_ff_" + std::to_string(getpid());
        fs::create_directory(temp_dir);
    }

    ~FastFlowExternalMergeSort() {
        cleanup_temp_files();
    }

    /**
     * Main function to sort the input file.
     * Steps:
     * 1. Chunk the input file (ChunkEmitter)
     * 2. Sort chunks in parallel (ChunkWorker)
     * 3. Merge sorted chunks into a final output file (ChunkCollector)
     */
    double sort_file(const std::string& input_file, const std::string& output_file) {
        auto file_size_bytes = std::filesystem::file_size(input_file);
        double file_size_mb = static_cast<double>(file_size_bytes) / (1024 * 1024);

        std::cout << "FastFlow External MergeSort starting..." << std::endl;
        std::cout << "Input: " << input_file << " (" << file_size_mb << " MB)" << std::endl;
        std::cout << "Output: " << output_file << std::endl;
        std::cout << "Memory budget: " << (memory_budget / 1024 / 1024) << " MB" << std::endl;
        std::cout << "Workers: " << num_workers << std::endl;

        ff::ffTime(ff::START_TIME);

        // Create emitter and collector
        ChunkEmitter emitter(input_file, temp_dir, num_workers, memory_budget);
        ChunkCollector collector(output_file);

        // Create Workers
        std::vector<ff_node*> workers_v;
        for (int i = 0; i < num_workers; ++i) {
            workers_v.push_back(new ChunkWorker(memory_budget / num_workers));
        }

        // Set up FastFlow farm
        ff_farm farm;
        farm.add_emitter(&emitter);
        farm.add_workers(workers_v);
        farm.add_collector(&collector);

        // Run farm
        if (farm.run_and_wait_end() < 0) {
            std::cerr << "FastFlow farm execution failed!" << std::endl;
            return;
        }

        double elapsed_ms = ff::ffTime(ff::STOP_TIME);
        std::cout << "[TIMING] Total sorting + merging time: " << (elapsed_ms / 1000.0) << " s" << std::endl;

        cleanup_temp_files();

        return elapsed_ms;
    }


private:
    void cleanup_temp_files() {
        if (fs::exists(temp_dir)) {
            fs::remove_all(temp_dir);
        }
    }
};

#endif