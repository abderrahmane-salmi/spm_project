#include <queue>
#include <fstream>
#include <vector>
#include <string>
#include <iostream>
#include "include/record.hpp"
#include "include/record_io.hpp"

// Struct to hold current record and the input stream from chunk file
struct ChunkRecord {
    Record record;
    size_t chunk_index;

    bool operator>(const ChunkRecord& other) const {
        return record.key > other.record.key; // min-heap by key
    }
};

void merge_sorted_chunks(const std::string& temp_dir, const std::string& output_path, size_t total_chunks) {
    std::cout << "Merging sorted chunks...\n";
    
    // Open all chunk files for reading
    std::cout << "Opening chunk files...\n";
    std::vector<std::ifstream> chunk_streams(total_chunks);
    for (size_t i = 0; i < total_chunks; ++i) {
        std::string chunk_file = temp_dir + "/chunk_" + std::to_string(i) + ".bin";
        chunk_streams[i].open(chunk_file, std::ios::binary);
        if (!chunk_streams[i]) {
            std::cerr << "Failed to open chunk file: " << chunk_file << std::endl;
            return;
        }
    }

    // Read first record from each chunk to initialize heap
    std::priority_queue<ChunkRecord, std::vector<ChunkRecord>, std::greater<>> min_heap;

    for (size_t i = 0; i < total_chunks; ++i) {
        uint64_t key;
        uint32_t len;

        // Read key and len
        chunk_streams[i].read(reinterpret_cast<char*>(&key), sizeof(key));
        chunk_streams[i].read(reinterpret_cast<char*>(&len), sizeof(len));
        if (!chunk_streams[i] || len == 0 || len > PAYLOAD_MAX) continue;

        char* payload = new char[len];
        chunk_streams[i].read(payload, len);
        if (!chunk_streams[i]) {
            delete[] payload;
            continue;
        }

        min_heap.push(ChunkRecord{Record(key, len, payload), i});
        delete[] payload; // Record copies payload
    }

    std::ofstream out(output_path, std::ios::binary);
    if (!out) {
        std::cerr << "Failed to open output file: " << output_path << std::endl;
        return;
    }

    while (!min_heap.empty()) {
        auto top = min_heap.top();
        min_heap.pop();

        // Write the smallest record to output file
        const Record& rec = top.record;
        out.write(reinterpret_cast<const char*>(&rec.key), sizeof(rec.key));
        out.write(reinterpret_cast<const char*>(&rec.len), sizeof(rec.len));
        out.write(rec.payload, rec.len);

        // Read next record from the chunk that this record came from
        size_t idx = top.chunk_index;

        uint64_t key;
        uint32_t len;
        if (chunk_streams[idx].read(reinterpret_cast<char*>(&key), sizeof(key)) &&
            chunk_streams[idx].read(reinterpret_cast<char*>(&len), sizeof(len)) &&
            len > 0 && len <= PAYLOAD_MAX) {

            char* payload = new char[len];
            if (chunk_streams[idx].read(payload, len)) {
                min_heap.push(ChunkRecord{Record(key, len, payload), idx});
            }
            delete[] payload;
        }
    }

    // Close all chunk files and output
    for (auto& stream : chunk_streams) {
        stream.close();
    }
    out.close();

    std::cout << "Merged " << total_chunks << " chunks into " << output_path << std::endl;
}
