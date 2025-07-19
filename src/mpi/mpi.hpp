#ifndef MPI_MERGE_SORT_HPP
#define MPI_MERGE_SORT_HPP

#include <mpi.h>
#include <iostream>
#include <fstream>
#include <algorithm>
#include <chrono>
#include <filesystem>
#include <cstring>
#include <vector>
#include <string>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <memory>
#include <sstream>

#include "../include/record.hpp"
#include "../chunking/chunking.hpp"
#include "../merging/merging.hpp"
#include "../openmp/omp.hpp"
#include "../fastflow/ff.hpp"

struct ChunkMeta {
    uint64_t start_offset;
    uint64_t end_offset;
    size_t size;
    int chunk_id;
};

class MPIMergeSort {
private:
    int rank_;
    int size_;
    int num_workers;
    size_t memory_budget;
    std::string temp_dir_;
    
    // Memory mapping variables
    int input_fd_;
    size_t file_size_;
    uint8_t* mapped_data_;

public:
    MPIMergeSort(int rank, int size, int num_workers, size_t memory_budget, const std::string& temp_dir = "/mpi_tmp") 
        : rank_(rank), size_(size), num_workers(num_workers), memory_budget(memory_budget), temp_dir_(temp_dir), input_fd_(-1), mapped_data_(nullptr) {
        
        // Create temp directory if it doesn't exist
        std::filesystem::create_directories(temp_dir_);
    }

    ~MPIMergeSort() {
        if (mapped_data_) {
            unmap_input_file();
        }
    }

    // Main sort function
    void sort_file(const std::string& input_file, const std::string& output_file) {
        if (size_ == 1) {
            // Single-node mode — just use FastFlow directly
            std::cout << "[LOG] Running single-node sort\n";
            FastFlowExternalMergeSort sorter(memory_budget, num_workers);
            sorter.sort_file(input_file, output_file);
            std::cout << "[LOG] FastFlow sort completed\n";
        } else {
            // Multi-node sort
            if (rank_ == 0) {
                run_coordinator(input_file, output_file);
            } else {
                run_worker();
            }
        }
    }

private:
    // Memory map the input file for fast access
    void map_input_file(const std::string& input_path) {
        // Open the input file in read-only mode
        input_fd_ = open(input_path.c_str(), O_RDONLY);
        if (input_fd_ < 0) {
            throw std::runtime_error("Failed to open input file: " + input_path);
        }

        // Retrieve the file's metadata (ex: size) using fstat
        struct stat st;
        if (fstat(input_fd_, &st) < 0) {
            close(input_fd_);
            throw std::runtime_error("fstat failed");
        }
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
        if (mapped_data_ == MAP_FAILED) {
            close(input_fd_);
            throw std::runtime_error("mmap failed");
        }
    }

    // Unmap the input file and close file descriptor
    void unmap_input_file() {
        if (mapped_data_) {
            munmap(mapped_data_, file_size_);
            mapped_data_ = nullptr;
        }
        if (input_fd_ >= 0) {
            close(input_fd_);
            input_fd_ = -1;
        }
    }

    // Compute logical chunks based on memory constraints
    std::vector<ChunkMeta> compute_logical_chunks(const std::string& input_path) {
        //std::cout << "[LOG] Computing logical chunks for " << input_path << "\n";
        map_input_file(input_path);

        std::vector<ChunkMeta> logical_chunks;
        size_t estimated_chunk_size = compute_estimated_chunk_size(input_path);
        uint64_t curr_offset = 0;
        uint64_t chunk_start = 0;
        size_t curr_chunk_size = 0;

        //std::cout << "[LOG] Estimated chunk size: " << estimated_chunk_size << " bytes\n";

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
                break;
            }

            // Make sure we don't read past file
            if (curr_offset + len > file_size_) {
                std::cerr << "Error: record at offset " << curr_offset << " (len = " << len << ")\n";
                break;
            }

            // Compute record size
            size_t rec_size = sizeof(key) + sizeof(len) + len;

            // If estimated chunk size is exceeded, save the current chunk to the list and start a new chunk
            // curr_chunk_size > 0 to avoid saving an empty chunk
            if (curr_chunk_size + rec_size > estimated_chunk_size && curr_chunk_size > 0) {
                uint64_t chunk_end = curr_offset - sizeof(key) - sizeof(len); // go back to record start (ignore last record)
                
                logical_chunks.push_back({
                    .start_offset = chunk_start,
                    .end_offset = chunk_end,
                    .size = chunk_end - chunk_start,
                    .chunk_id = static_cast<int>(logical_chunks.size())
                });

                // start new chunk
                chunk_start = chunk_end; // new chunk starts at end of previous one
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
                .end_offset = file_size_,
                .size = file_size_ - chunk_start,
                .chunk_id = static_cast<int>(logical_chunks.size())
            });
        }

        unmap_input_file();
        return logical_chunks;
    }

    // Estimate chunk size
    size_t compute_estimated_chunk_size(const std::string& input_path) {
        size_t safe_memory_budget = static_cast<size_t>(memory_budget * 0.8); // 80%
        size_t file_size = std::filesystem::file_size(input_path);
        int num_ranks;
        MPI_Comm_size(MPI_COMM_WORLD, &num_ranks);

        // If all workers had 1 chunk and that fits
        size_t ideal_chunk_size = file_size / (num_ranks - 1);
        if (ideal_chunk_size <= safe_memory_budget) {
            return ideal_chunk_size;
        }

        // Else: estimate how many total chunks we need to stay within memory budget
        size_t min_chunks = (file_size + safe_memory_budget - 1) / safe_memory_budget; // ceiling division (to round up)
        return (file_size + min_chunks - 1) / min_chunks;  // ceiling division (to round up)
    }

    // Generate physical chunk files from logical chunks
    std::vector<std::string> generate_chunk_files(const std::string& input_file) {
        // std::cout << "[LOG] Generating chunk files from input file " << input_file << "\n";
        auto logical_chunks = compute_logical_chunks(input_file);
        
        if (logical_chunks.size() == 1) {
            return {input_file};
        }

        // Map file for chunk extraction
        map_input_file(input_file);
        std::vector<std::string> chunk_files(logical_chunks.size());

        // Create chunk files
        for (size_t i = 0; i < logical_chunks.size(); ++i) {
            const auto& curr_chunk = logical_chunks[i];
            std::string output_file = temp_dir_ + "/chunk_" + std::to_string(curr_chunk.chunk_id) + ".bin";
            
            std::ofstream out{output_file, std::ios::binary};
            if (!out) {
                throw std::runtime_error("Failed to create chunk file: " + output_file);
            }
            
            // Write chunk data directly from mmap buffer
            // Write 'curr_chunk.size' bytes starting at 'mapped_data_ + curr_chunk.start_offset' (raw pointer offset) to output file (we avoid record parsing)
            out.write(reinterpret_cast<char*>(mapped_data_ + curr_chunk.start_offset), curr_chunk.size);
            chunk_files[i] = output_file;
            // std::cout << "[LOG] Created chunk file: " << output_file 
            //           << " (" << curr_chunk.size << " bytes)" << std::endl;
        }

        unmap_input_file();
        return chunk_files;
    }

    // Coordinator (Rank 0) logic
    void run_coordinator(const std::string& input_file, const std::string& output_file) {
        if (rank_ != 0) return; // only rank 0 is coordinator

        std::cout << "Coordinator: Starting distributed merge sort\n";
        
        // Step 1: Generate chunk files
        std::vector<std::string> chunk_files = generate_chunk_files(input_file);
        std::cout << "Coordinator: Created " << chunk_files.size() << " chunks\n";
        
        // Step 2: Distribute chunks to workers in round-robin
        std::vector<std::vector<std::string>> worker_chunk_map(size_); // worker chunk map

        // Assign chunks in round-robin (excluding rank 0)
        for (size_t i = 0; i < chunk_files.size(); ++i) {
            int target_worker = 1 + (i % (size_ - 1));
            worker_chunk_map[target_worker].push_back(chunk_files[i]);
            std::cout << "[LOG] Coordinator: Assigning chunk " << chunk_files[i]
                    << " to worker " << target_worker << "\n";
        }

        // Step 3: Send assigned chunk file paths to each worker
        for (int worker = 1; worker < size_; ++worker) {
            // get chunks assigned to this worker
            const auto& worker_chunks = worker_chunk_map[worker]; 

            // Send number of chunks first
            int num_chunks = static_cast<int>(worker_chunks.size());
            MPI_Send(&num_chunks, 1, MPI_INT, worker, 0, MPI_COMM_WORLD);

            // Send each chunk file path
            for (const auto& chunk_file : worker_chunks) {
                int path_len = chunk_file.length();
                MPI_Send(&path_len, 1, MPI_INT, worker, 0, MPI_COMM_WORLD); // Send string length
                MPI_Send(chunk_file.c_str(), path_len, MPI_CHAR, worker, 0, MPI_COMM_WORLD); // Send string data
            }
        }

        // Step 4: Collect (receive) sorted file paths from workers
        std::vector<std::string> sorted_files;

        for (int worker = 1; worker < size_; ++worker) {
            // Receive how many sorted files the worker produced
            int num_sorted_files;
            MPI_Recv(&num_sorted_files, 1, MPI_INT, worker, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            //  std::cout << "[LOG] Coordinator: Receiving " << num_sorted_files << " sorted files from worker "
            //           << worker << std::endl;
            
            for (int i = 0; i < num_sorted_files; ++i) {
                // Receive length of file path
                int path_len;
                MPI_Recv(&path_len, 1, MPI_INT, worker, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                
                // Receive the file path itself
                std::vector<char> path_buffer(path_len + 1);
                MPI_Recv(path_buffer.data(), path_len, MPI_CHAR, worker, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                path_buffer[path_len] = '\0'; // null-terminate the string (mark the end of the string)

                // ps: we are recieving file path as raw characters not a string
                // that's why we used a buffer (char array), then we convert the buffer to a string below
                //std::cout << "[LOG] Coordinator: Received sorted file path: " << path_buffer.data() << std::endl;
                
                // Store the received path
                sorted_files.push_back(std::string(path_buffer.data()));
            }
        }

        std::cout << "Coordinator: Received " << sorted_files.size() << " sorted files\n";

        // Step 5: Merge all the sorted chunk files into a final output file
        std::cout << "[LOG] Coordinator: Merging " << sorted_files.size() << " sorted files into " << output_file << std::endl;
        merge_sorted_files(sorted_files, output_file);
        
        // Step 6: Clean up temporary chunk and sorted files
        cleanup_temp_files(chunk_files);
        cleanup_temp_files(sorted_files);
        
        std::cout << "Coordinator: Merge sort completed successfully\n";
    }

    // Worker logic
    void run_worker() {
        // make sure only workers run this code (not rank 0)
        if (rank_ == 0) return;

        std::cout << "Worker " << rank_ << ": Starting work\n";
        
        // Step 1: Receive the number of chunk files this worker should process
        int num_chunks;
        MPI_Recv(&num_chunks, 1, MPI_INT, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        
        // Allocate space for file paths of chunks
        std::vector<std::string> chunk_files(num_chunks);
        
        // Step 2: Receive each chunk file path from the coordinator
        for (int i = 0; i < num_chunks; ++i) {
            // Receive length of chunk file path
            int path_len;
            MPI_Recv(&path_len, 1, MPI_INT, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            
            // Allocate buffer and receive actual path
            std::vector<char> path_buffer(path_len + 1);
            MPI_Recv(path_buffer.data(), path_len, MPI_CHAR, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            path_buffer[path_len] = '\0'; // Null-terminate the string
            
            // Store path in vector
            chunk_files[i] = std::string(path_buffer.data());
            // std::cout << "[LOG] Worker " << rank_ << ": Received chunk file path: " << chunk_files[i] << std::endl;
        }

        std::cout << "Worker " << rank_ << ": Received " << num_chunks << " chunks to process\n";

        // Step 3: Sort each chunk using FastFlow and store the sorted file paths
        std::vector<std::string> sorted_files;
        for (const auto& chunk_file : chunk_files) {
            //std::cout << "[LOG] Worker " << rank_ << ": Processing chunk file: " << chunk_file << std::endl;
            std::string sorted_file = process_chunk_with_fastflow(chunk_file);
            //std::cout << "[LOG] Worker " << rank_ << ": Finished sorting chunk, produced sorted file: " << sorted_file << std::endl;
            sorted_files.push_back(sorted_file);
        }

        // Step 4: Send the number of sorted files back to the coordinator
        int num_sorted_files = sorted_files.size();
        MPI_Send(&num_sorted_files, 1, MPI_INT, 0, 0, MPI_COMM_WORLD);
        
        // Step 5: Send each sorted file path back to the coordinator
        for (const auto& sorted_file : sorted_files) {
            int path_len = sorted_file.length();
            MPI_Send(&path_len, 1, MPI_INT, 0, 0, MPI_COMM_WORLD); // Send path length
            MPI_Send(sorted_file.c_str(), path_len, MPI_CHAR, 0, 0, MPI_COMM_WORLD); // Send path data
            //std::cout << "[LOG] Worker " << rank_ << ": Sent sorted file path: " << sorted_file << std::endl;
        }

        std::cout << "Worker " << rank_ << ": Completed processing\n";
    }

    // Process a single chunk using FastFlow
    std::string process_chunk_with_fastflow(const std::string& chunk_file) {
        
        // Generate sorted output file (chunk_1.bin -> chunk_1_ff_sorted.bin)
        std::filesystem::path input_path(chunk_file);
        std::string sorted_file = input_path.parent_path().string() + "/" + input_path.stem().string() + "_ff_sorted.bin";

        // std::cout << "[LOG] Worker " << rank_ << ": Starting FastFlow sort for " << chunk_file
        //           << ", output: " << sorted_file << std::endl;
        
        FastFlowExternalMergeSort sorter(memory_budget, num_workers);
        sorter.sort_file(chunk_file, sorted_file);

        // std::cout << "[LOG] Worker " << rank_ << ": Completed FastFlow sort for " << chunk_file << std::endl;
        return sorted_file;
    }

    // Cleanup temporary files
    void cleanup_temp_files(const std::vector<std::string>& files) {
        for (const auto& file : files) {
            std::filesystem::remove(file);
        }
    }
};

#endif // MPI_MERGE_SORT_HPP