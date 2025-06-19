#include "merging.hpp"
#include "../include/record.hpp"

#include <fstream>
#include <queue>
#include <iostream>
#include <stdexcept>
#include <filesystem>

namespace fs = std::filesystem;

bool merge_sorted_files(const std::vector<std::string>& sorted_files, const std::string& output_file) {
    if (sorted_files.empty()) {
        std::cerr << "No files provided for merging." << std::endl;
        return false;
    }

    if (sorted_files.size() == 1) {
        fs::copy_file(sorted_files[0], output_file, fs::copy_options::overwrite_existing);
        return true;
    }

    struct FileRecord {
        Record record;
        size_t file_index;

        bool operator>(const FileRecord& other) const {
            return record.key > other.record.key;
        }
    };

    std::priority_queue<FileRecord, std::vector<FileRecord>, std::greater<FileRecord>> pq;
    std::vector<std::ifstream> input_files(sorted_files.size());

    for (size_t i = 0; i < sorted_files.size(); ++i) {
        input_files[i].open(sorted_files[i], std::ios::binary);
        if (!input_files[i].is_open()) {
            std::cerr << "Failed to open file: " << sorted_files[i] << std::endl;
            return false;
        }
        Record rec;
        if (rec.read_from_stream(input_files[i])) {
            pq.push({std::move(rec), i});
        }
    }

    std::ofstream output(output_file, std::ios::binary);
    if (!output.is_open()) {
        std::cerr << "Failed to open output file: " << output_file << std::endl;
        return false;
    }

    size_t merged_count = 0;
    while (!pq.empty()) {
        auto top = pq.top();
        pq.pop();

        if (!top.record.write_to_stream(output)) {
            std::cerr << "Failed to write record during merge" << std::endl;
            return false;
        }
        ++merged_count;

        Record next;
        if (next.read_from_stream(input_files[top.file_index])) {
            pq.push({std::move(next), top.file_index});
        }
    }

    std::cout << "Merged " << merged_count << " records into output file" << std::endl;
    return true;
}
