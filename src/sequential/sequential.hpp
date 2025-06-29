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

        auto chunk_files = generate_chunk_files(input_file, memory_budget_ * 0.8, temp_dir_);

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
