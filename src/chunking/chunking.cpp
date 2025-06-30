#pragma once
#include <cstddef>
#include <algorithm>
#include <fstream>
#include <stdexcept>
#include <vector>

#include "chunking.hpp"
#include "../include/record.hpp"

/**
 * Analyze a binary file of records and split it into memory-bounded chunks.
 * 
 * Reads the input file and creates a vector of ChunkInfo objects, each describing
 * a contiguous block of records in the file. The chunks are sized to stay within
 * the given memory budget. The method returns a vector of ChunkInfo objects
 * describing all the chunks in the file.
 *
 * @param input_file Path to the input file.
 * @param memory_budget_bytes Approximate max size per chunk (in bytes).
 * @return Vector of ChunkInfo, each describing a chunk.
 */
std::vector<ChunkInfo> analyze_file_for_chunks(const std::string& input_file,
                                               size_t memory_budget_bytes,
                                               size_t num_threads)
{
    std::ifstream in(input_file, std::ios::binary | std::ios::ate);
    if (!in.is_open()) {
        throw std::runtime_error("Cannot open input file: " + input_file);
    }

    size_t file_size = in.tellg();
    in.seekg(0, std::ios::beg);

    // Ensure enough chunks for parallelism
    size_t min_chunk_size = file_size / std::max<size_t>(4, num_threads * 4);
    size_t effective_chunk_budget = std::min(memory_budget_bytes, min_chunk_size);

    std::vector<ChunkInfo> chunks;
    size_t current_offset = 0;

    while (in.good() && current_offset < file_size) {
        size_t chunk_start = current_offset;
        size_t bytes_used = 0;
        size_t record_count = 0;

        while (in.good() && bytes_used < effective_chunk_budget) {
            Record record;
            size_t pos_before = in.tellg();

            if (record.read_from_stream(in)) {
                size_t pos_after = in.tellg();
                size_t record_size = pos_after - pos_before;
                if (bytes_used + record_size > effective_chunk_budget && record_count > 0) {
                    // Stop before exceeding budget, leave this record for next chunk
                    in.seekg(pos_before, std::ios::beg);
                    break;
                }
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

/**
 * Analyzes the file and creates actual chunk files on disk.
 *
 * @param input_file Path to the original input file.
 * @param memory_budget_bytes Memory budget per chunk.
 * @param temp_dir Directory where chunk files will be stored.
 * @return Vector of paths to generated chunk files.
 */
std::vector<std::string> generate_chunk_files(const std::string& input_file,
    size_t memory_budget_bytes,
    const std::string& temp_dir,
    size_t num_threads
) {
    auto chunks = analyze_file_for_chunks(input_file, memory_budget_bytes, num_threads);

    // Optimization: if only one chunk covers the entire input file, just return the input file path
    if (chunks.size() == 1) {
        return { input_file };
    }

    std::vector<std::string> chunk_files;

    for (size_t i = 0; i < chunks.size(); ++i) {
        const ChunkInfo& info = chunks[i];
        std::string chunk_file = temp_dir + "/chunk_" + std::to_string(i) + ".bin";

        std::ifstream in(input_file, std::ios::binary);
        std::ofstream out(chunk_file, std::ios::binary);
        if (!in.is_open() || !out.is_open()) {
            throw std::runtime_error("Error accessing files for chunk writing");
        }

        in.seekg(info.offset_bytes, std::ios::beg);

        std::vector<char> buffer(info.length_bytes);
        in.read(buffer.data(), info.length_bytes);
        out.write(buffer.data(), info.length_bytes);

        in.close();
        out.close();
        chunk_files.push_back(chunk_file);
    }

    return chunk_files;
}

inline size_t compute_optimal_chunk_size(size_t total_file_size,
                                         size_t memory_per_process,
                                         size_t num_threads,
                                         double safety_margin = 0.8) {
    // Apply safety margin
    size_t safe_mem = memory_per_process * safety_margin;

    // Estimate memory per thread
    size_t mem_per_thread = safe_mem / std::max(size_t(1), num_threads);

    // Conservative size for thread-safety and overhead
    size_t estimated_chunk_size = mem_per_thread;

    // Clamp to sane limits
    return std::clamp(estimated_chunk_size, size_t(16 * 1024 * 1024), size_t(1L * 1024 * 1024 * 1024));
}
