#ifndef SEQUENTIAL_EXTERNAL_MERGESORT_H
#define SEQUENTIAL_EXTERNAL_MERGESORT_H

#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <algorithm>
#include <filesystem>
#include <chrono>

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
        auto chunk_files = generate_chunk_files_sequential(input_file);
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
    struct ChunkMeta {
        uint64_t start_offset;
        uint64_t end_offset;
        size_t size;
        int chunk_id;
    };

    // Compute record-aligned chunk boundaries (logical chunks)
    std::vector<ChunkMeta> compute_logical_chunks_sequential(const std::string& input_path) {
        std::ifstream input(input_path, std::ios::binary);
        if (!input.is_open()) {
            throw std::runtime_error("Failed to open input file");
        }

        std::vector<ChunkMeta> logical_chunks;
        uint64_t curr_offset = 0;
        uint64_t chunk_start = 0;
        size_t curr_chunk_size = 0;

        // compute est chunk size (80% of memory budget, just to be safe)
        size_t estimated_chunk_size = memory_budget_ * 0.8;

        while (input.peek() != EOF) {
            std::streampos record_start = input.tellg();

            uint64_t key;
            uint32_t len;

            // Read key (8 bytes)
            if (!input.read(reinterpret_cast<char*>(&key), sizeof(key))) break;

            // Read length (4 bytes)
            if (!input.read(reinterpret_cast<char*>(&len), sizeof(len))) break;

            // Validate payload length
            if (len < 8 || len > PAYLOAD_MAX) {
                std::cerr << "Invalid record length: " << len << " at offset " << curr_offset << std::endl;
                break;
            }

            size_t rec_size = sizeof(key) + sizeof(len) + len;

            // Skip payload
            if (!input.seekg(len, std::ios::cur)) break;

            curr_offset = static_cast<uint64_t>(input.tellg());
            curr_chunk_size += rec_size;

            if (curr_chunk_size > estimated_chunk_size) {
                logical_chunks.push_back({
                    .start_offset = chunk_start,
                    .end_offset = static_cast<uint64_t>(record_start),
                    .size = static_cast<size_t>(static_cast<uint64_t>(record_start) - chunk_start),
                    .chunk_id = static_cast<int>(logical_chunks.size())
                });

                chunk_start = static_cast<uint64_t>(record_start);
                curr_chunk_size = rec_size;  // start next chunk with current record
            }
        }

        // Final chunk
        if (chunk_start < curr_offset) {
            logical_chunks.push_back({
                .start_offset = chunk_start,
                .end_offset = curr_offset,
                .size = curr_offset - chunk_start,
                .chunk_id = static_cast<int>(logical_chunks.size())
            });
        }

        input.close();
        return logical_chunks;
    }

    // Generate chunk files based on logical chunks
    std::vector<std::string> generate_chunk_files_sequential(const std::string& input_file) {
        auto logical_chunks = compute_logical_chunks_sequential(input_file);
        std::vector<std::string> chunk_files;

        std::ifstream input(input_file, std::ios::binary);
        if (!input.is_open()) {
            throw std::runtime_error("Failed to reopen input file for reading chunks");
        }

        for (const auto& chunk : logical_chunks) {
            std::string chunk_path = temp_dir_ + "/chunk_" + std::to_string(chunk.chunk_id) + ".bin";
            std::ofstream out(chunk_path, std::ios::binary);
            if (!out) {
                throw std::runtime_error("Failed to create chunk file: " + chunk_path);
            }

            input.seekg(chunk.start_offset, std::ios::beg);

            std::vector<char> buffer(chunk.size);
            if (!input.read(buffer.data(), chunk.size)) {
                throw std::runtime_error("Failed to read chunk data");
            }

            out.write(buffer.data(), chunk.size);
            out.close();
            chunk_files.push_back(chunk_path);
        }

        input.close();
        return chunk_files;
    }


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
