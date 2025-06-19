#include "chunking.hpp"
#include "../include/record.hpp"
#include <fstream>
#include <stdexcept>

std::vector<ChunkInfo> analyze_file_for_chunks(const std::string& input_file, size_t memory_budget_bytes) {
    std::ifstream in(input_file, std::ios::binary);
    if (!in.is_open()) {
        throw std::runtime_error("Cannot open input file: " + input_file);
    }

    std::vector<ChunkInfo> chunks;
    size_t current_offset = 0;

    while (in.good()) {
        size_t chunk_start = current_offset;
        size_t bytes_used = 0;
        size_t record_count = 0;

        while (in.good() && bytes_used < memory_budget_bytes) {
            Record record;
            size_t pos_before = in.tellg();

            if (record.read_from_stream(in)) {
                size_t pos_after = in.tellg();
                size_t record_size = pos_after - pos_before;
                bytes_used += record_size;
                current_offset += record_size;
                record_count++;
            } else {
                break;
            }
        }

        if (record_count > 0) {
            chunks.emplace_back(chunk_start, bytes_used, record_count);
        }
    }

    in.close();
    return chunks;
}
