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

        // Clean up after sorting
        cleanup_temp_files();
    }

private:
    /**
     * Phase 1: Read input file and split it into chunks
     * 
     * Creates chunks from the input file by reading records until the memory limit
     * is reached, and writing them to a new file in the temporary directory.
     * The method creates as many chunks as necessary to process the whole input file.
     * The method returns a vector of file paths to the chunk files.
     *
     * @param input_file The path to the input file to be split into chunks.
     * @return A vector of file paths to the created chunk files.
     */
    std::vector<std::string> create_chunks(const std::string& input_file) {
        // Open the input file in binary mode
        std::ifstream infile(input_file, std::ios::binary);
        if (!infile.is_open()) {
            throw std::runtime_error("Cannot open input file: " + input_file);
        }

        // Initialize a vector to store the paths of the chunk files
        std::vector<std::string> chunk_files;
        
        // Initialize a counter for the chunk files
        size_t chunk_id = 0;
        
        // Set the memory limit for each chunk (80% of the total memory budget) (leave room for overhead)
        const size_t chunk_memory_limit = memory_budget * 0.8;

        // Loop until the end of the input file is reached
        while (infile.good()) {
            // Create a file path for the current chunk file
            std::string chunk_file = temp_dir + "/chunk_" + std::to_string(chunk_id) + ".bin";
            // Open the chunk file in binary mode
            std::ofstream chunk_out(chunk_file, std::ios::binary);
            
            // Initialize variables to track the memory used and whether a chunk was created
            size_t memory_used = 0;
            bool chunk_created = false;

            // Keep writing records until chunk is full or the end of the input file is reached
            while (infile.good() && memory_used < chunk_memory_limit) {
                // Read a record from the input file
                Record record;
                if (record.read_from_stream(infile)) {
                    // Write the record to the chunk file
                    record.write_to_stream(chunk_out);
                    // Update the memory used
                    memory_used += record.total_size();
                    // Mark that a chunk was created
                    chunk_created = true;
                } else {
                    // Break out of the loop if the end of the input file is reached
                    break;
                }
            }

            // Close the chunk file
            chunk_out.close();

            // If a chunk was created, add its path to the vector and increment the chunk ID
            if (chunk_created) {
                chunk_files.push_back(chunk_file);
                chunk_id++;
            } else {
                // If no chunk was created, remove the empty chunk file and break out of the loop
                fs::remove(chunk_file);
                break;
            }
        }

        // Close the input file and return the vector of chunk file paths
        infile.close();
        return chunk_files;
    }

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

    /**
     * Phase 3: Merge sorted files into a single sorted output file
     * 
     * This function takes a vector of sorted file paths and performs a k-way 
     * merge using a priority queue to merge records from these files into 
     * a single output file. If there is only one sorted file, it simply renames 
     * it to the output file. It throws an exception if no files are provided 
     * or if any file operation fails.
     * 
     * @param sorted_files A vector containing paths to the sorted input files.
     * @param output_file The path to the output file where the merged result will be written.
     */
    void merge_sorted_chunks(const std::vector<std::string>& sorted_files, const std::string& output_file) {
        // Check if there are any files to merge
        if (sorted_files.empty()) {
            throw std::runtime_error("No sorted files to merge");
        }

        // Optimization: If there's only one file, simply rename it to the output file
        if (sorted_files.size() == 1) {
            fs::rename(sorted_files[0], output_file);
            return;
        }

        // Define a struct to hold a record and its file index
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
            // Read the first record from each file and add it to the priority queue
            if (record.read_from_stream(input_files[i])) {
                pq.push({std::move(record), i});
            }
        }

        // Open output file
        std::ofstream output(output_file, std::ios::binary);
        if (!output.is_open()) {
            throw std::runtime_error("Cannot create output file: " + output_file);
        }

        // Merge records from the priority queue
        while (!pq.empty()) {
            // Get the smallest record from the priority queue
            FileRecord file_record = pq.top();
            pq.pop();

            // Write the smallest record to output
            file_record.record.write_to_stream(output);

            // Read next record from the same file
            Record next_record;
            if (next_record.read_from_stream(input_files[file_record.file_index])) {
                // Add the next record to the priority queue
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