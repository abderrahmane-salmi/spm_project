#ifndef CHUNKING_HPP
#define CHUNKING_HPP

#include <vector>
#include <string>
#include <cstddef> // for size_t

struct ChunkInfo {
    size_t offset_bytes;
    size_t length_bytes;
    size_t num_records;

    ChunkInfo(size_t offset, size_t length, size_t records)
        : offset_bytes(offset), length_bytes(length), num_records(records) {}
};

/**
 * Analyze a binary file of records and split it into memory-bounded chunks.
 * 
 * @param input_file Path to the input file.
 * @param memory_budget_bytes Approximate max size per chunk (in bytes).
 * @return Vector of ChunkInfo, each describing a chunk.
 */
std::vector<ChunkInfo> analyze_file_for_chunks(const std::string& input_file, size_t memory_budget_bytes);

/**
 * Analyzes the file and creates actual chunk files on disk.
 *
 * @param input_file Path to the original input file.
 * @param memory_budget_bytes Memory budget per chunk.
 * @param temp_dir Directory where chunk files will be stored.
 * @return Vector of paths to generated chunk files.
 */
std::vector<std::string> generate_chunk_files(const std::string& input_file, size_t memory_budget_bytes, const std::string& temp_dir);

#endif // CHUNKING_HPP
