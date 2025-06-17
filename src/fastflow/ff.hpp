#ifndef FASTFLOW_MERGESORT_HPP
#define FASTFLOW_MERGESORT_HPP

#include <ff/ff.hpp>
#include <ff/farm.hpp>
#include <vector>
#include <string>
#include <fstream>
#include <algorithm>
#include <queue>
#include <filesystem>
#include <memory>
#include "../include/record.hpp"

using namespace ff;

namespace fs = std::filesystem;

// Task structure for communication between FastFlow nodes
struct SortTask {
    std::string input_file;
    std::string output_file;
    size_t chunk_id;
    size_t memory_budget;
    
    SortTask(const std::string& in, const std::string& out, size_t id, size_t budget)
        : input_file(in), output_file(out), chunk_id(id), memory_budget(budget) {}
};

// Emitter: Distributes chunk sorting tasks to workers
class ChunkEmitter : public ff_node_t<int, SortTask> {
private:
    std::vector<std::string> chunk_files;
    std::string temp_dir;
    size_t memory_budget;
    size_t current_chunk;

public:
    ChunkEmitter(const std::vector<std::string>& chunks, const std::string& temp_dir, size_t budget)
        : chunk_files(chunks), temp_dir(temp_dir), memory_budget(budget), current_chunk(0) {}

    SortTask* svc(int*) override {
        if (current_chunk >= chunk_files.size()) {
            return EOS; // End of stream
        }
        
        std::string output_file = temp_dir + "/sorted_chunk_" + std::to_string(current_chunk) + ".bin";
        auto task = new SortTask(chunk_files[current_chunk], output_file, current_chunk, memory_budget);
        current_chunk++;
        return task;
    }
};

// Worker: Sorts individual chunks
class ChunkSorter : public ff_node_t<SortTask, SortTask> {
public:
    SortTask* svc(SortTask* task) override {
        try {
            // Read all records from the chunk file
            std::ifstream infile(task->input_file, std::ios::binary);
            if (!infile.is_open()) {
                std::cerr << "Error: Cannot open chunk file " << task->input_file << std::endl;
                return task; // Return task to indicate completion (with error)
            }

            std::vector<Record> records;
            size_t memory_used = 0;
            const size_t memory_limit = task->memory_budget;

            // Read records until memory limit or EOF
            while (infile.good() && memory_used < memory_limit) {
                Record record;
                if (record.read_from_stream(infile)) {
                    memory_used += record.total_size();
                    records.push_back(std::move(record));
                } else {
                    break; // EOF or read error
                }
            }
            infile.close();

            // Sort records by key
            std::sort(records.begin(), records.end(), [](const Record& a, const Record& b) {
                return a.key < b.key;
            });

            // Write sorted records to output file
            std::ofstream outfile(task->output_file, std::ios::binary);
            if (!outfile.is_open()) {
                std::cerr << "Error: Cannot create sorted chunk file " << task->output_file << std::endl;
                return task;
            }

            for (const auto& record : records) {
                record.write_to_stream(outfile);
            }
            outfile.close();

            std::cout << "FastFlow Worker: Sorted chunk " << task->chunk_id 
                      << " (" << records.size() << " records) -> " << task->output_file << std::endl;

        } catch (const std::exception& e) {
            std::cerr << "Error in ChunkSorter: " << e.what() << std::endl;
        }
        
        return task;
    }
};

// Collector: Collects completion notifications from workers
class ChunkCollector : public ff_node_t<SortTask, std::string> {
private:
    std::vector<std::string> sorted_files;
    size_t expected_chunks;
    size_t completed_chunks;

public:
    ChunkCollector(size_t expected) : expected_chunks(expected), completed_chunks(0) {}

    std::string* svc(SortTask* task) override {
        sorted_files.push_back(task->output_file);
        completed_chunks++;
        
        delete task; // Clean up the task
        
        // When all chunks are processed, return the list of sorted files
        if (completed_chunks == expected_chunks) {
            // Create a result containing all sorted file names
            auto result = new std::string();
            for (size_t i = 0; i < sorted_files.size(); i++) {
                if (i > 0) *result += ",";
                *result += sorted_files[i];
            }
            return result;
        }
        
        return GO_ON; // Continue processing
    }
    
    const std::vector<std::string>& get_sorted_files() const {
        return sorted_files;
    }
};

class FastFlowExternalMergeSort {
private:
    size_t memory_budget;
    std::string temp_dir;
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

    void sort_file(const std::string& input_file, const std::string& output_file) {
        std::cout << "FastFlow External MergeSort starting..." << std::endl;
        std::cout << "Memory budget: " << (memory_budget / 1024 / 1024) << " MB" << std::endl;
        std::cout << "Workers: " << num_workers << std::endl;

        // Phase 1: Split input file into chunks
        auto chunk_files = create_chunks(input_file);
        std::cout << "Created " << chunk_files.size() << " chunks" << std::endl;

        // Phase 2: Parallel sorting using FastFlow Farm
        auto sorted_files = parallel_sort_chunks(chunk_files);
        std::cout << "Sorted " << sorted_files.size() << " chunks in parallel" << std::endl;

        // Phase 3: Merge sorted chunks
        merge_sorted_chunks(sorted_files, output_file);
        std::cout << "Merged chunks into final output: " << output_file << std::endl;

        cleanup_temp_files();
    }

private:
    std::vector<std::string> create_chunks(const std::string& input_file) {
        std::ifstream infile(input_file, std::ios::binary);
        if (!infile.is_open()) {
            throw std::runtime_error("Cannot open input file: " + input_file);
        }

        std::vector<std::string> chunk_files;
        size_t chunk_id = 0;
        const size_t chunk_memory_limit = memory_budget * 0.8; // Reserve some memory for operations

        while (infile.good()) {
            std::string chunk_file = temp_dir + "/chunk_" + std::to_string(chunk_id) + ".bin";
            std::ofstream chunk_out(chunk_file, std::ios::binary);
            
            size_t memory_used = 0;
            bool chunk_created = false;

            while (infile.good() && memory_used < chunk_memory_limit) {
                Record record;
                if (record.read_from_stream(infile)) {
                    record.write_to_stream(chunk_out);
                    memory_used += record.total_size();
                    chunk_created = true;
                } else {
                    break;
                }
            }

            chunk_out.close();

            if (chunk_created) {
                chunk_files.push_back(chunk_file);
                chunk_id++;
            } else {
                fs::remove(chunk_file); // Remove empty chunk file
                break;
            }
        }

        infile.close();
        return chunk_files;
    }

    std::vector<std::string> parallel_sort_chunks(const std::vector<std::string>& chunk_files) {
        // Create FastFlow farm for parallel chunk sorting
        ChunkEmitter emitter(chunk_files, temp_dir, memory_budget);
        ChunkCollector collector(chunk_files.size());
        
        std::vector<std::unique_ptr<ff_node>> workers;
        for (int i = 0; i < num_workers; i++) {
            workers.push_back(std::make_unique<ChunkSorter>());
        }

        ff_farm farm;
        farm.add_emitter(&emitter);
        farm.add_collector(&collector);
        
        // Add workers to farm
        std::vector<ff_node*> worker_ptrs;
        for (auto& worker : workers) {
            worker_ptrs.push_back(worker.get());
        }
        farm.add_workers(worker_ptrs);

        // Run the farm
        if (farm.run_and_wait_end() < 0) {
            throw std::runtime_error("FastFlow farm execution failed");
        }

        return collector.get_sorted_files();
    }

    void merge_sorted_chunks(const std::vector<std::string>& sorted_files, const std::string& output_file) {
        if (sorted_files.empty()) {
            throw std::runtime_error("No sorted files to merge");
        }

        if (sorted_files.size() == 1) {
            // Only one file, just rename it
            fs::rename(sorted_files[0], output_file);
            return;
        }

        // K-way merge using priority queue (same as OpenMP version)
        struct FileRecord {
            Record record;
            size_t file_index;
            
            bool operator>(const FileRecord& other) const {
                return record.key > other.record.key;
            }
        };

        // Open all sorted files
        std::vector<std::ifstream> input_files(sorted_files.size());
        for (size_t i = 0; i < sorted_files.size(); i++) {
            input_files[i].open(sorted_files[i], std::ios::binary);
            if (!input_files[i].is_open()) {
                throw std::runtime_error("Cannot open sorted file: " + sorted_files[i]);
            }
        }

        // Initialize priority queue with first record from each file
        std::priority_queue<FileRecord, std::vector<FileRecord>, std::greater<FileRecord>> pq;
        
        for (size_t i = 0; i < input_files.size(); i++) {
            Record record;
            if (record.read_from_stream(input_files[i])) {
                pq.push({std::move(record), i});
            }
        }

        // Open output file
        std::ofstream output(output_file, std::ios::binary);
        if (!output.is_open()) {
            throw std::runtime_error("Cannot create output file: " + output_file);
        }

        // Merge records
        while (!pq.empty()) {
            FileRecord file_record = pq.top();
            pq.pop();

            // Write the smallest record to output
            file_record.record.write_to_stream(output);

            // Read next record from the same file
            Record next_record;
            if (next_record.read_from_stream(input_files[file_record.file_index])) {
                pq.push({std::move(next_record), file_record.file_index});
            }
        }

        // Close all files
        output.close();
        for (auto& file : input_files) {
            file.close();
        }
    }

    void cleanup_temp_files() {
        if (fs::exists(temp_dir)) {
            fs::remove_all(temp_dir);
        }
    }
};

#endif // FASTFLOW_MERGESORT_HPP