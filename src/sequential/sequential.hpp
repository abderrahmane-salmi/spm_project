#ifndef SEQUENTIAL_EXTERNAL_MERGESORT_H
#define SEQUENTIAL_EXTERNAL_MERGESORT_H

#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <algorithm>
#include <filesystem>
#include <chrono>

#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>

#include "../include/record.hpp"
#include "../include/record_io.hpp"
#include "../chunking/chunking.hpp"
#include "../merging/merging.hpp"

class SequentialExternalMergeSort {
private:
    size_t memory_budget_;
    std::string temp_dir_;
    std::vector<std::string> temp_files_;

public:
    SequentialExternalMergeSort(size_t memory_budget = 512 * 1024 * 1024, // 512 MB
                                 const std::string& temp_dir = "./temp_seq")
        : memory_budget_(memory_budget), temp_dir_(temp_dir) {
        std::filesystem::create_directories(temp_dir_);
    }

    ~SequentialExternalMergeSort() {
        cleanup_temp_files();
    }

    bool sort_file(const std::string& input_file, const std::string& output_file) {
        using Clock = std::chrono::high_resolution_clock;

        auto file_size_bytes = std::filesystem::file_size(input_file);
        double file_size_mb = static_cast<double>(file_size_bytes) / (1024 * 1024);
        std::cout << "Sequential External MergeSort: input=" << input_file
            << " (" << file_size_mb << " MB), output=" << output_file << std::endl;

        auto t1 = Clock::now();
        auto chunk_files = generate_chunk_files_(input_file);
        auto t2 = Clock::now();
        std::cout << "Phase 1: Created " << chunk_files.size() << " chunk files." << std::endl;
        std::chrono::duration<double> chunking_time = t2 - t1;
        std::cout << "[TIMING] Chunking time: " << chunking_time.count() << " s" << std::endl;

        t1 = Clock::now();
        temp_files_.resize(chunk_files.size());
        for (size_t i = 0; i < chunk_files.size(); ++i) {
            temp_files_[i] = temp_dir_ + "/run_" + std::to_string(i) + ".tmp";
            if (!process_chunk(chunk_files[i], temp_files_[i])) {
                std::cerr << "Failed to process chunk " << i << std::endl;
                return false;
            }
        }
        t2 = Clock::now();
        std::chrono::duration<double> sorting_time = t2 - t1;
        std::cout << "[TIMING] Sorting time: " << sorting_time.count() << " s" << std::endl;

        t1 = Clock::now();
        if (!merge_sorted_files(temp_files_, output_file)) {
            std::cerr << "Merging failed!" << std::endl;
            return false;
        }
        t2 = Clock::now();
        std::chrono::duration<double> merging_time = t2 - t1;
        std::cout << "[TIMING] Merging time: " << merging_time.count() << " s" << std::endl;

        double total_time = chunking_time.count() + sorting_time.count() + merging_time.count();
        std::cout << "[TIMING] Total time: " << total_time << " s" << std::endl;

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

    // use mmap() to map the input file into memory, which allows very fast access without copying the whole file into a buffer
    void map_input_file(const std::string& input_path) {
        // Open the input file in read-only mode
        input_fd_ = open(input_path.c_str(), O_RDONLY);
        if (input_fd_ < 0) throw std::runtime_error("Failed to open input file");

        // Retrieve the file's metadata (ex: size) using fstat
        struct stat st;
        if (fstat(input_fd_, &st) < 0) throw std::runtime_error("fstat failed");
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
        if (mapped_data_ == MAP_FAILED) throw std::runtime_error("mmap failed");
    }

    // unmap the input file from memory and close the file descriptor
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

    // Compute record-aligned chunk boundaries (logical chunks)
    std::vector<ChunkMeta> compute_logical_chunks(const std::string& input_path) {
        map_input_file(input_path); // Memory-map the input file for fast access

        std::vector<ChunkMeta> logical_chunks;
        size_t estimated_chunk_size = memory_budget_ * 0.8;
        uint64_t curr_offset = 0;
        uint64_t chunk_start = 0;
        size_t curr_chunk_size = 0;

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
                break; // Stop parsing here to avoid undefined behavior
            }

            // Make sure we don't read past file
            if (curr_offset + len > file_size_) {
                std::cerr << "Error record at offset " << curr_offset << " (len = " << len << ")\n";
                break;
            }

            // Compute record size
            size_t rec_size = sizeof(key) + sizeof(len) + len;

            // If estimated chunk size is exceeded, save the current chunk to the list and start a new chunk
            // chunk_acc > 0 to avoid saving an empty chunk
            if (curr_chunk_size + rec_size > estimated_chunk_size && curr_chunk_size > 0) {
                uint64_t chunk_end = curr_offset - sizeof(key) - sizeof(len); // go back to record start (ignore last record)
                
                logical_chunks.push_back({
                    .start_offset = chunk_start,
                    .end_offset   = chunk_end,
                    .size         = chunk_end - chunk_start,
                    .chunk_id     = static_cast<int>(logical_chunks.size())
                });

                // start new chunk
                chunk_start = chunk_end;  // new chunk starts at end of previous one
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
                .end_offset   = file_size_,
                .size         = file_size_ - chunk_start,
                .chunk_id     = static_cast<int>(logical_chunks.size())
            });
        }

        unmap_input_file();
        return logical_chunks;
    }

    // Generate chunk files based on logical chunks
    std::vector<std::string> generate_chunk_files_(const std::string& input_file) {
        // get logical chunks
        auto logical_chunks = compute_logical_chunks(input_file);
        if (logical_chunks.size() == 1) return {input_file};

        // Map file again for chunk extraction
        map_input_file(input_file);
        std::vector<std::string> chunk_files(logical_chunks.size());

        for (int i = 0; i < (int)logical_chunks.size(); ++i) {
            const auto& curr_chunk = logical_chunks[i];
            std::string output_file = temp_dir_ + "/chunk_" + std::to_string(i) + ".bin";
            std::ofstream out{output_file, std::ios::binary};
            // Write chunk data directly from mmap buffer
            // Write 'curr_chunk.size' bytes starting at 'mapped_data_ + curr_chunk.start_offset' (raw pointer offset) to output file (we avoid record parsing)
            out.write(reinterpret_cast<char*>(mapped_data_ + curr_chunk.start_offset), curr_chunk.size);
            chunk_files[i] = output_file;
        }

        unmap_input_file();
        return chunk_files;
    }

    // std::vector<std::string> generate_chunk_files_sequential(const std::string& input_file) {
    //     auto logical_chunks = compute_logical_chunks(input_file);
    //     std::vector<std::string> chunk_files;

    //     std::ifstream input(input_file, std::ios::binary);
    //     if (!input.is_open()) {
    //         throw std::runtime_error("Failed to reopen input file for reading chunks");
    //     }

    //     for (const auto& chunk : logical_chunks) {
    //         std::string chunk_path = temp_dir_ + "/chunk_" + std::to_string(chunk.chunk_id) + ".bin";
    //         std::ofstream out(chunk_path, std::ios::binary);
    //         if (!out) {
    //             throw std::runtime_error("Failed to create chunk file: " + chunk_path);
    //         }

    //         input.seekg(chunk.start_offset, std::ios::beg);

    //         std::vector<char> buffer(chunk.size);
    //         if (!input.read(buffer.data(), chunk.size)) {
    //             throw std::runtime_error("Failed to read chunk data");
    //         }

    //         out.write(buffer.data(), chunk.size);
    //         out.close();
    //         chunk_files.push_back(chunk_path);
    //     }

    //     input.close();
    //     return chunk_files;
    // }


    bool process_chunk(const std::string& chunk_file, const std::string& temp_file) {
        std::ifstream input(chunk_file, std::ios::binary);
        if (!input.is_open()) {
            std::cerr << "Could not open chunk: " << chunk_file << std::endl;
            return false;
        }

        std::vector<Record> records;
        while (input.good()) {
            Record record;
            if (!record.read_from_stream(input)) break;
            records.push_back(std::move(record));
        }
        input.close();

        std::sort(records.begin(), records.end(), [](const Record& a, const Record& b) {
            return a.key < b.key;
        });

        std::ofstream output(temp_file, std::ios::binary);
        for (const auto& record : records) {
            if (!record.write_to_stream(output)) {
                std::cerr << "Failed to write record to " << temp_file << std::endl;
                return false;
            }
        }
        output.close();
        return true;
    }

    void cleanup_temp_files() {
        for (const auto& f : temp_files_) {
            std::error_code ec;
            std::filesystem::remove(f, ec);
        }
        temp_files_.clear();
    }
};

#endif
