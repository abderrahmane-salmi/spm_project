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
#include <chrono>

#include "../include/record.hpp"
#include "../chunking/chunking.hpp"
#include "../merging/merging.hpp"

using namespace ff;

namespace fs = std::filesystem;

// This task struct is the message sent between FastFlow components
struct SortTask {
    // Each SortTask tells a worker:
    std::string input_file; // Which file to read
    std::string output_file; // Where to write the sorted result
    size_t chunk_id; // Which chunk it is 
    size_t memory_budget; // How much memory it's allowed to use
    
    SortTask(const std::string& in, const std::string& out, size_t id, size_t budget)
        : input_file(in), output_file(out), chunk_id(id), memory_budget(budget) {}
};

// ------------------------
// FastFlow Node: Chunk Emitter
// ------------------------
// Distributes chunk sorting tasks to workers
class ChunkEmitter : public ff_node_t<int, SortTask> {
private:
    std::vector<std::string> chunk_files; // Paths to all chunk files
    std::string temp_dir; // Directory for temp files
    size_t memory_budget; // Max memory each worker can use
    size_t current_chunk; // Index of the next chunk to emit

public:
    ChunkEmitter(const std::vector<std::string>& chunks, const std::string& temp_dir, size_t budget)
        : chunk_files(chunks), temp_dir(temp_dir), memory_budget(budget), current_chunk(0) {}

    /**
     * This service function is called by FastFlow for each item in the input stream.
     * In this case, it emits a SortTask for each chunk file in the input list.
     * On each svc() call, it sends a SortTask to a worker.
     * 
     * @return A SortTask for the next chunk file, or EOS to signal end of stream.
     */
    SortTask* svc(int*) override {
        if (current_chunk >= chunk_files.size()) {
            return EOS; // End of stream (no more chunks)
        }
        
        // Create output file path for the sorted chunk
        std::string output_file = temp_dir + "/sorted_chunk_" + std::to_string(current_chunk) + ".bin";
        
        // Create and send a SortTask to a worker
        auto task = new SortTask(chunk_files[current_chunk], output_file, current_chunk, memory_budget);
        current_chunk++;
        return task;
    }
};

// ------------------------
// FastFlow Node: ChunkSorter (Worker)
// ------------------------
// Receives a chunk, loads it, sorts it in memory, writes sorted data to disk
class ChunkSorter : public ff_node_t<SortTask, SortTask> {
public:

    /**
     * Processes a SortTask by reading records from the input chunk file, sorting them in memory,
     * and writing the sorted records to the specified output file. Handles file I/O and memory
     * management up to the specified memory budget. Logs errors if any file operations fail.
     * 
     * @param task The SortTask.
     * @return The same SortTask to indicate completion, even if an error occurs.
    */
    SortTask* svc(SortTask* task) override {
        try {
            // Open the input chunk file for reading in binary mode
            std::ifstream infile(task->input_file, std::ios::binary);
            if (!infile.is_open()) {
                std::cerr << "Error: Cannot open chunk file " << task->input_file << std::endl;
                return task;
            }

            // Create a vector to store the records in memory
            std::vector<Record> records;
            
            // Initialize variables to track memory usage
            size_t memory_used = 0;
            const size_t memory_limit = task->memory_budget;

            // Read records from the file until the memory limit is reached or EOF
            while (infile.good() && memory_used < memory_limit) {
                Record record;
                if (record.read_from_stream(infile)) {
                    // Update memory usage and add record to the vector
                    memory_used += record.total_size();
                    records.push_back(std::move(record));
                } else {
                    // Break out of the loop on EOF or read error
                    break;
                }
            }
            // Close the input file
            infile.close();

            // Sort the records in memory based on the key
            std::sort(records.begin(), records.end(), [](const Record& a, const Record& b) {
                return a.key < b.key;
            });

            // Open the output file for writing in binary mode
            std::ofstream outfile(task->output_file, std::ios::binary);
            if (!outfile.is_open()) {
                std::cerr << "Error: Cannot create sorted chunk file " << task->output_file << std::endl;
                return task;
            }

            // Write the sorted records to the output file
            for (const auto& record : records) {
                record.write_to_stream(outfile);
            }
            // Close the output file
            outfile.close();

            std::cout << "FastFlow Worker: Sorted chunk " << task->chunk_id 
                    << " (" << records.size() << " records) -> " << task->output_file << std::endl;

        } catch (const std::exception& e) {
            std::cerr << "Error in ChunkSorter: " << e.what() << std::endl;
        }
        
        // Return the original task to indicate completion
        return task;
    }
};

// ------------------------
// FastFlow Node: Chunk Collector
// ------------------------
// Collects sorted file paths from workers and returns a string of all paths
class ChunkCollector : public ff_node_t<SortTask, std::string> {
private:
    std::vector<std::string> sorted_files; // List of sorted output files
    size_t expected_chunks; // How many chunks we expect to receive
    size_t completed_chunks; // How many chunks have been processed

public:
    ChunkCollector(size_t expected) : expected_chunks(expected), completed_chunks(0) {}

    /**
     * This service function is called by FastFlow for each completed SortTask.
     * It collects the output file paths of the sorted chunks and keeps track 
     * of how many chunks have been processed.
     * 
     * @param task A pointer to the completed SortTask containing the output file path.
     * @return A string containing a comma-separated list of sorted file paths if all 
     * chunks have been processed, or GO_ON to signal that more chunks are expected.
     */
    std::string* svc(SortTask* task) override {
        // Add the output file path of the completed task to the list of sorted files
        sorted_files.push_back(task->output_file);
        completed_chunks++;
        
        delete task; // Clean up the task to prevent memory leaks
        
        // When all chunks are processed, return the list of sorted files
        if (completed_chunks == expected_chunks) {
            // Create a new string to store the result
            auto result = new std::string();

            // Iterate over the sorted files and append them to the result string
            for (size_t i = 0; i < sorted_files.size(); i++) {
                if (i > 0) *result += ",";
                *result += sorted_files[i];
            }
            return result;
        }
        
        // If not all chunks have been processed, return GO_ON to continue processing
        return GO_ON;
    }
    
    const std::vector<std::string>& get_sorted_files() const {
        return sorted_files;
    }
};

// ------------------------
// Main class: FastFlowExternalMergeSort
// ------------------------
// Coordinates the whole external merge sort: chunking, sorting, merging.
class FastFlowExternalMergeSort {
private:
    size_t memory_budget; // Total memory to use per chunk
    std::string temp_dir; // Directory for temporary files
    int num_workers; // Number of parallel workers

public:
    // Constructor: sets memory and workers, creates temp directory
    FastFlowExternalMergeSort(size_t memory_budget_bytes = 256 * 1024 * 1024, int workers = 4)
        : memory_budget(memory_budget_bytes), num_workers(workers) {
        temp_dir = "temp_ff_" + std::to_string(getpid());
        fs::create_directory(temp_dir);
    }

    // Destructor: removes temp files
    ~FastFlowExternalMergeSort() {
        cleanup_temp_files();
    }

    /**
     * Main entry point to sort an input file and save the sorted result.
     *
     * This function coordinates the external merge sort process using FastFlow.
     * It performs the following steps:
     * 1. Splits the input file into smaller chunks.
     * 2. Sorts the chunks in parallel using FastFlow Farm.
     * 3. Merges the sorted chunks into a single sorted output file.
     * 4. Cleans up temporary files created during the process.
     *
     * @param input_file The path to the input file to be sorted.
     * @param output_file The path to the file where the sorted result will be saved.
    */
    void sort_file(const std::string& input_file, const std::string& output_file) {
        using Clock = std::chrono::high_resolution_clock;

        std::cout << "FastFlow External MergeSort starting..." << std::endl;
        std::cout << "Memory budget: " << (memory_budget / 1024 / 1024) << " MB" << std::endl;
        std::cout << "Workers: " << num_workers << std::endl;

        // Phase 1: Divide file into chunks
        auto t1 = Clock::now();
        auto chunk_files = generate_chunk_files(input_file, memory_budget * 0.8, temp_dir);
        auto t2 = Clock::now();
        std::cout << "Created " << chunk_files.size() << " chunks" << std::endl;
        std::chrono::duration<double> chunking_time = t2 - t1;
        std::cout << "[TIMING] Chunking time: " << chunking_time.count() << " s" << std::endl;

        // Phase 2: Sort chunks in parallel using FastFlow farm
        t1 = Clock::now();
        auto sorted_files = parallel_sort_chunks(chunk_files);
        t2 = Clock::now();
        std::chrono::duration<double> sorting_time = t2 - t1;
        std::cout << "Sorted " << sorted_files.size() << " chunks in parallel" << std::endl;
        std::cout << "[TIMING] Sorting time: " << sorting_time.count() << " s" << std::endl;

        // Phase 3: Merge sorted files into a single sorted output file
        t1 = Clock::now();
        merge_sorted_files(sorted_files, output_file);
        t2 = Clock::now();
        std::chrono::duration<double> merging_time = t2 - t1;
        std::cout << "Merged chunks into final output: " << output_file << std::endl;
        std::cout << "[TIMING] Merging time: " << merging_time.count() << " s" << std::endl;

        t1 = Clock::now();
        cleanup_temp_files();
        t2 = Clock::now();
        std::chrono::duration<double> cleanup_time = t2 - t1;
        std::cout << "[TIMING] Cleanup temp files time: " << cleanup_time.count() << " s" << std::endl;
    }


private:
    /**
     * Phase 2: Sort chunks in parallel using FastFlow farm
     *
     * This method sets up a FastFlow farm with an emitter, a collector, and
     * multiple worker nodes. The emitter sends sorting tasks to worker nodes
     * for each chunk file, and each worker sorts its assigned chunk in memory
     * within the specified memory budget. The collector gathers the sorted
     * file paths and returns them as a vector.
     *
     * @param chunk_files A vector containing paths to the unsorted chunk files.
     * @return A vector of file paths to the sorted chunk files.
     */
    std::vector<std::string> parallel_sort_chunks(const std::vector<std::string>& chunk_files) {
        // Create FastFlow farm for parallel chunk sorting
        ChunkEmitter emitter(chunk_files, temp_dir, memory_budget);
        ChunkCollector collector(chunk_files.size());
        
        // Create a vector to hold a specified number of worker nodes
        std::vector<std::unique_ptr<ff_node>> workers;
        for (int i = 0; i < num_workers; i++) {
            // Each worker is a ChunkSorter object
            workers.push_back(std::make_unique<ChunkSorter>());
        }

        // Create a FastFlow farm object and add the emitter and collector 
        ff_farm farm;
        farm.add_emitter(&emitter);
        farm.add_collector(&collector);
        
        // Add workers to farm
        std::vector<ff_node*> worker_ptrs;
        for (auto& worker : workers) {
            // Get a raw pointer to each worker
            worker_ptrs.push_back(worker.get());
        }
        farm.add_workers(worker_ptrs);

        // Run the farm and wait for it to finish
        if (farm.run_and_wait_end() < 0) {
            // If the farm execution fails, throw an exception
            throw std::runtime_error("FastFlow farm execution failed");
        }

        // Return the sorted file paths gathered by the collector
        return collector.get_sorted_files();
    }

    void cleanup_temp_files() {
        if (fs::exists(temp_dir)) {
            fs::remove_all(temp_dir);
        }
    }
};

#endif // FASTFLOW_MERGESORT_HPP