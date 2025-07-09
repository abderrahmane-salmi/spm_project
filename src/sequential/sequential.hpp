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
        auto start_time = std::chrono::high_resolution_clock::now();
        std::cout << "Sequential External MergeSort: input=" << input_file
                  << ", output=" << output_file << std::endl;

        auto chunk_files = generate_chunk_files_sequential(input_file, memory_budget_ * 0.8, temp_dir_);

        temp_files_.resize(chunk_files.size());
        for (size_t i = 0; i < chunk_files.size(); ++i) {
            temp_files_[i] = temp_dir_ + "/run_" + std::to_string(i) + ".tmp";
            if (!process_chunk(chunk_files[i], temp_files_[i])) {
                std::cerr << "Failed to process chunk " << i << std::endl;
                return false;
            }
        }

        if (!merge_sorted_files(temp_files_, output_file)) {
            std::cerr << "Merging failed!" << std::endl;
            return false;
        }

        auto end_time = std::chrono::high_resolution_clock::now();
        double total_time = std::chrono::duration<double>(end_time - start_time).count();

        std::cout << "Sequential sort completed in " << total_time << " seconds" << std::endl;
        return true;
    }

    std::vector<std::string> generate_chunk_files_sequential(const std::string& input_file,
                                                         size_t memory_budget_bytes,
                                                         const std::string& temp_dir) {
        std::ifstream in(input_file, std::ios::binary | std::ios::ate);
        if (!in.is_open())
            throw std::runtime_error("Cannot open input file: " + input_file);

        size_t file_size = in.tellg();
        in.seekg(0, std::ios::beg);

        size_t est_chunk_size = std::min(memory_budget_bytes, file_size);
        size_t pos = 0;
        std::vector<std::string> chunk_files;
        int chunk_index = 0;

        while (pos < file_size) {
            size_t chunk_start = pos;
            size_t estimated_end = std::min(pos + est_chunk_size, file_size);
            size_t aligned_end = estimated_end;

            in.seekg(estimated_end, std::ios::beg);

            // Try to align to a record boundary
            Record dummy;
            bool found = false;
            for (size_t offset = 0; offset < 1024 && aligned_end < file_size; ++offset) {
                in.clear();
                in.seekg(aligned_end, std::ios::beg);
                if (dummy.read_from_stream(in)) {
                    found = true;
                    break;
                }
                ++aligned_end;
            }

            if (!found)
                aligned_end = file_size;

            size_t length = aligned_end - chunk_start;

            std::vector<char> buffer(length);
            in.seekg(chunk_start, std::ios::beg);
            in.read(buffer.data(), length);

            std::string chunk_file = temp_dir + "/chunk_" + std::to_string(chunk_index++) + ".bin";
            std::ofstream out(chunk_file, std::ios::binary);
            if (!out.is_open())
                throw std::runtime_error("Cannot open chunk file: " + chunk_file);
            out.write(buffer.data(), length);
            out.close();

            chunk_files.push_back(chunk_file);
            pos = aligned_end;
        }

        in.close();
        return chunk_files;
    }


private:
    bool process_chunk(const std::string& chunk_file, const std::string& temp_file) {
        std::ifstream input(chunk_file, std::ios::binary);
        if (!input.is_open()) {
            std::cerr << "Could not open chunk: " << chunk_file << std::endl;
            return false;
        }

        std::vector<Record> records;
        while (input.good()) {
            Record r;
            if (!r.read_from_stream(input)) break;
            records.push_back(std::move(r));
        }
        input.close();

        std::sort(records.begin(), records.end(), [](const Record& a, const Record& b) {
            return a.key < b.key;
        });

        std::ofstream output(temp_file, std::ios::binary);
        for (const auto& r : records) {
            if (!r.write_to_stream(output)) {
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
