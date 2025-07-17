#ifndef MPI_MERGE_SORT_HPP
#define MPI_MERGE_SORT_HPP

#include <iostream>
#include <fstream>
#include <algorithm>
#include <chrono>
#include <filesystem>
#include <cstring>
#include <mpi.h>

#include "../include/record.hpp"
#include "../chunking/chunking.hpp"
#include "../merging/merging.hpp"
#include "../openmp/omp.hpp"
#include "../fastflow/ff.hpp"

#include <mpi.h>
#include <iostream>
#include <vector>
#include <string>
#include <fstream>
#include <filesystem>
#include <algorithm>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <cstring>
#include <memory>
#include <sstream>

const double MEMORY_SAFETY_MARGIN = 0.8; // Use 80% of memory budget just to be safe

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
        if (rank_ == 0) {
            run_coordinator(input_file, output_file);
        } else {
            run_worker();
        }
    }

private:
    // Memory map the input file for fast access
    void map_input_file(const std::string& input_path) {
        input_fd_ = open(input_path.c_str(), O_RDONLY);
        if (input_fd_ < 0) {
            throw std::runtime_error("Failed to open input file: " + input_path);
        }

        struct stat st;
        if (fstat(input_fd_, &st) < 0) {
            close(input_fd_);
            throw std::runtime_error("fstat failed");
        }
        file_size_ = st.st_size;

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
        map_input_file(input_path);

        std::vector<ChunkMeta> logical_chunks;
        size_t estimated_chunk_size = compute_estimated_chunk_size(input_path);
        uint64_t curr_offset = 0;
        uint64_t chunk_start = 0;
        size_t curr_chunk_size = 0;

        // Parse records and create chunks
        while (curr_offset + sizeof(uint64_t) + sizeof(uint32_t) <= file_size_) {
            // Read key (8 bytes)
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

            // Check if we don't read past file
            if (curr_offset + len > file_size_) {
                std::cerr << "Error: record at offset " << curr_offset << " (len = " << len << ")\n";
                break;
            }

            // Compute record size
            size_t rec_size = sizeof(key) + sizeof(len) + len;

            // If estimated chunk size is exceeded, save current chunk and start new one
            if (curr_chunk_size + rec_size > estimated_chunk_size && curr_chunk_size > 0) {
                uint64_t chunk_end = curr_offset - sizeof(key) - sizeof(len);
                
                logical_chunks.push_back({
                    .start_offset = chunk_start,
                    .end_offset = chunk_end,
                    .size = chunk_end - chunk_start,
                    .chunk_id = static_cast<int>(logical_chunks.size())
                });

                // Start new chunk
                chunk_start = chunk_end;
                curr_chunk_size = 0;
            }

            // Advance to skip payload
            curr_offset += len;
            curr_chunk_size += rec_size;
        }

        // Handle final chunk
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

    // Estimate chunk size based on memory constraints
    size_t compute_estimated_chunk_size(const std::string& input_path) {
        size_t safe_budget = static_cast<size_t>(memory_budget * 0.8);
        size_t file_size = std::filesystem::file_size(input_path);
        int num_ranks;
        MPI_Comm_size(MPI_COMM_WORLD, &num_ranks);

        // If all workers had 1 chunk and that fits
        size_t ideal_chunk_size = file_size / (num_ranks - 1);
        if (ideal_chunk_size <= safe_budget) {
            return ideal_chunk_size;
        }

        // Else: estimate how many total chunks we need to stay within memory budget
        size_t min_chunks = (file_size + safe_budget - 1) / safe_budget; // ceiling division (to round up)
        return (file_size + min_chunks - 1) / min_chunks;  // ceiling division (to round up)
    }

    // Generate physical chunk files from logical chunks
    std::vector<std::string> generate_chunk_files(const std::string& input_file) {
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
            out.write(reinterpret_cast<char*>(mapped_data_ + curr_chunk.start_offset), curr_chunk.size);
            chunk_files[i] = output_file;
        }

        unmap_input_file();
        return chunk_files;
    }

    // Coordinator (Rank 0) logic
    void run_coordinator(const std::string& input_file, const std::string& output_file) {
        if (rank_ != 0) return;

        std::cout << "Coordinator: Starting distributed merge sort\n";
        
        // Generate chunk files
        std::vector<std::string> chunk_files = generate_chunk_files(input_file);
        std::cout << "Coordinator: Created " << chunk_files.size() << " chunks\n";

        // Distribute chunks to workers
        std::vector<std::string> sorted_files;
        size_t chunk_idx = 0;
        
        // Send chunks to workers in round-robin fashion
        for (int worker = 1; worker < size_; ++worker) {
            std::vector<std::string> worker_chunks;
            
            // Assign chunks to this worker
            while (chunk_idx < chunk_files.size()) {
                worker_chunks.push_back(chunk_files[chunk_idx]);
                chunk_idx++;
                
                // Move to next worker if we want to distribute evenly
                if (chunk_idx % (size_ - 1) == 0) break;
            }
            
            // Send number of chunks to worker
            int num_chunks = worker_chunks.size();
            MPI_Send(&num_chunks, 1, MPI_INT, worker, 0, MPI_COMM_WORLD);
            
            // Send each chunk file path
            for (const auto& chunk_file : worker_chunks) {
                int path_len = chunk_file.length();
                MPI_Send(&path_len, 1, MPI_INT, worker, 0, MPI_COMM_WORLD);
                MPI_Send(chunk_file.c_str(), path_len, MPI_CHAR, worker, 0, MPI_COMM_WORLD);
            }
        }

        // Collect sorted files from workers
        for (int worker = 1; worker < size_; ++worker) {
            int num_sorted_files;
            MPI_Recv(&num_sorted_files, 1, MPI_INT, worker, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            
            for (int i = 0; i < num_sorted_files; ++i) {
                int path_len;
                MPI_Recv(&path_len, 1, MPI_INT, worker, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                
                std::vector<char> path_buffer(path_len + 1);
                MPI_Recv(path_buffer.data(), path_len, MPI_CHAR, worker, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                path_buffer[path_len] = '\0';
                
                sorted_files.push_back(std::string(path_buffer.data()));
            }
        }

        std::cout << "Coordinator: Received " << sorted_files.size() << " sorted files\n";

        // Merge all sorted files into final output
        merge_sorted_files(sorted_files, output_file);
        
        // Cleanup temporary files
        cleanup_temp_files(chunk_files);
        cleanup_temp_files(sorted_files);
        
        std::cout << "Coordinator: Merge sort completed successfully\n";
    }

    // Worker logic
    void run_worker() {
        if (rank_ == 0) return;

        std::cout << "Worker " << rank_ << ": Starting work\n";
        
        // Receive number of chunks to process
        int num_chunks;
        MPI_Recv(&num_chunks, 1, MPI_INT, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        
        std::vector<std::string> chunk_files(num_chunks);
        
        // Receive chunk file paths
        for (int i = 0; i < num_chunks; ++i) {
            int path_len;
            MPI_Recv(&path_len, 1, MPI_INT, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            
            std::vector<char> path_buffer(path_len + 1);
            MPI_Recv(path_buffer.data(), path_len, MPI_CHAR, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            path_buffer[path_len] = '\0';
            
            chunk_files[i] = std::string(path_buffer.data());
        }

        std::cout << "Worker " << rank_ << ": Received " << num_chunks << " chunks to process\n";

        // Process each chunk with FastFlow
        std::vector<std::string> sorted_files;
        for (const auto& chunk_file : chunk_files) {
            std::string sorted_file = process_chunk_with_fastflow(chunk_file);
            sorted_files.push_back(sorted_file);
        }

        // Send back the number of sorted files
        int num_sorted_files = sorted_files.size();
        MPI_Send(&num_sorted_files, 1, MPI_INT, 0, 0, MPI_COMM_WORLD);
        
        // Send sorted file paths back to coordinator
        for (const auto& sorted_file : sorted_files) {
            int path_len = sorted_file.length();
            MPI_Send(&path_len, 1, MPI_INT, 0, 0, MPI_COMM_WORLD);
            MPI_Send(sorted_file.c_str(), path_len, MPI_CHAR, 0, 0, MPI_COMM_WORLD);
        }

        std::cout << "Worker " << rank_ << ": Completed processing\n";
    }

    // Process a single chunk using FastFlow
    std::string process_chunk_with_fastflow(const std::string& chunk_file) {
        std::string sorted_file = temp_dir_ + "/sorted_" + std::to_string(rank_) + "_" + 
                                  std::to_string(std::hash<std::string>{}(chunk_file)) + ".bin";
        
        // Here you would call your FastFlow sorter
        // For now, I'll implement a simple sort as placeholder
        // Replace this with: fastflow_sorter.sort(chunk_file, sorted_file);

        FastFlowExternalMergeSort sorter(memory_budget, num_workers);
        std::string sorted_out = chunk_file + "_sorted.bin";
        sorter.sort_file(chunk_file, sorted_out);
        
        return sorted_file;
    }

    // Cleanup temporary files
    void cleanup_temp_files(const std::vector<std::string>& files) {
        for (const auto& file : files) {
            std::filesystem::remove(file);
        }
    }
};

// // Main function
// int main(int argc, char* argv[]) {
//     MPI_Init(&argc, &argv);
    
//     int rank, size;
//     MPI_Comm_rank(MPI_COMM_WORLD, &rank);
//     MPI_Comm_size(MPI_COMM_WORLD, &size);
    
//     if (size < 2) {
//         if (rank == 0) {
//             std::cerr << "Error: Need at least 2 MPI processes (1 coordinator + 1 worker)\n";
//         }
//         MPI_Finalize();
//         return 1;
//     }
    
//     if (argc != 3) {
//         if (rank == 0) {
//             std::cerr << "Usage: " << argv[0] << " <input_file> <output_file>\n";
//         }
//         MPI_Finalize();
//         return 1;
//     }
    
//     std::string input_file = argv[1];
//     std::string output_file = argv[2];
    
//     try {
//         MPIMergeSort sorter(rank, size);
//         sorter.sort(input_file, output_file);
//     } catch (const std::exception& e) {
//         std::cerr << "Error in rank " << rank << ": " << e.what() << "\n";
//         MPI_Abort(MPI_COMM_WORLD, 1);
//     }
    
//     MPI_Finalize();
//     return 0;
// }

#endif // MPI_MERGE_SORT_HPP