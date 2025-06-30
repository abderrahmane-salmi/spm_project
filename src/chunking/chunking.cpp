#include <cstddef>
#include <algorithm>
#include <fstream>
#include <stdexcept>
#include <vector>
#include <string>
#include <mutex>
#include <thread>
#include <future>

#include "chunking.hpp"
#include "../include/record.hpp"

// Constants
constexpr size_t ALIGN_PROBE_BYTES = 1024;

std::vector<ChunkInfo> analyze_file_for_chunks(const std::string& input_file,
                                               size_t memory_budget_bytes,
                                               size_t num_threads)
{
    std::ifstream in(input_file, std::ios::binary | std::ios::ate);
    if (!in.is_open())
        throw std::runtime_error("Cannot open input file: " + input_file);

    size_t file_size = in.tellg();
    in.seekg(0, std::ios::beg);

    // Divide file into chunk offsets
    size_t est_chunk_size = std::min(memory_budget_bytes, file_size);
    size_t pos = 0;
    std::vector<ChunkInfo> chunks;

    while (pos < file_size) {
        size_t chunk_start = pos;
        size_t estimated_end = std::min(pos + est_chunk_size, file_size);
        size_t aligned_end = estimated_end;

        in.seekg(estimated_end, std::ios::beg);

        // Alignment adjustment: read ahead until the next record parses
        Record dummy;
        bool found = false;
        for (size_t offset = 0; offset < ALIGN_PROBE_BYTES && aligned_end < file_size; offset++) {
            in.clear(); // Clear EOF flags
            in.seekg(aligned_end, std::ios::beg);
            if (dummy.read_from_stream(in)) {
                found = true;
                break;
            }
            aligned_end++;
        }

        // If no full record found, just finish at file_size
        if (!found)
            aligned_end = file_size;

        size_t length = aligned_end - chunk_start;
        if (length > 0)
            chunks.emplace_back(chunk_start, length, 0); // record count optional

        pos = aligned_end;
    }

    in.close();
    return chunks;
}

std::vector<std::string> generate_chunk_files(const std::string& input_file,
                                              size_t memory_budget_bytes,
                                              const std::string& temp_dir,
                                              size_t num_threads)
{
    auto chunks = analyze_file_for_chunks(input_file, memory_budget_bytes, num_threads);
    if (chunks.size() == 1)
        return { input_file };

    std::vector<std::string> chunk_files(chunks.size());

    // Open the file once
    std::ifstream in(input_file, std::ios::binary);
    if (!in.is_open())
        throw std::runtime_error("Cannot open input file for chunk extraction");

    std::mutex file_mutex;

    // Parallel write using threads or OpenMP
    std::vector<std::future<void>> futures;

    for (size_t i = 0; i < chunks.size(); ++i) {
        futures.push_back(std::async(std::launch::async, [&, i]() {
            const ChunkInfo& info = chunks[i];
            std::string chunk_file = temp_dir + "/chunk_" + std::to_string(i) + ".bin";

            std::vector<char> buffer(info.length_bytes);

            {
                std::lock_guard<std::mutex> lock(file_mutex);
                in.seekg(info.offset_bytes, std::ios::beg);
                in.read(buffer.data(), info.length_bytes);
            }

            std::ofstream out(chunk_file, std::ios::binary);
            if (!out.is_open())
                throw std::runtime_error("Failed to open output chunk file: " + chunk_file);
            out.write(buffer.data(), info.length_bytes);
            out.close();

            chunk_files[i] = chunk_file;
        }));
    }

    for (auto& f : futures)
        f.get();

    in.close();
    return chunk_files;
}

inline size_t compute_optimal_chunk_size(size_t total_file_size,
                                         size_t memory_per_process,
                                         size_t num_threads,
                                         double safety_margin = 1.0) {
    size_t safe_mem = memory_per_process * safety_margin;
    size_t mem_per_thread = safe_mem / std::max(size_t(1), num_threads);
    size_t estimated_chunk_size = mem_per_thread;
    return std::clamp(estimated_chunk_size, size_t(16 * 1024 * 1024), size_t(1L * 1024 * 1024 * 1024));
}
