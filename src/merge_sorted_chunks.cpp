#include "include/record_io.hpp"
#include <filesystem>
#include <fstream>
#include <iostream>
#include <vector>
#include <queue>
#include <string>
#include "include/record.hpp"

struct ChunkRecord {
    Record rec;
    size_t idx;
    bool operator>(const ChunkRecord& other) const {
        return rec.key > other.rec.key;
    }
};

void merge_sorted_chunks(const std::string& temp_dir,
                         const std::string& output_file,
                         size_t chunk_count) {
    std::vector<std::ifstream> streams(chunk_count);
    for (size_t i = 0; i < chunk_count; ++i) {
        streams[i].open(temp_dir + "/chunk_" + std::to_string(i) + ".bin",
                        std::ios::binary);
    }

    std::priority_queue<ChunkRecord, std::vector<ChunkRecord>, std::greater<>> pq;

    // Initialize heap
    for (size_t i = 0; i < chunk_count; ++i) {
        uint64_t key;
        uint32_t len;
        if (!(streams[i].read(reinterpret_cast<char*>(&key), sizeof(key)))) continue;
        streams[i].read(reinterpret_cast<char*>(&len), sizeof(len));
        if (len == 0 || len > PAYLOAD_MAX) continue;
        char* payload = new char[len];
        streams[i].read(payload, len);
        pq.push({Record(key, len, payload), i});
        delete[] payload;
    }

    std::ofstream out(output_file, std::ios::binary);

    while (!pq.empty()) {
        auto top = pq.top(); pq.pop();
        const Record& r = top.rec;
        out.write(reinterpret_cast<const char*>(&r.key), sizeof(r.key));
        out.write(reinterpret_cast<const char*>(&r.len), sizeof(r.len));
        out.write(r.payload, r.len);

        size_t i = top.idx;
        uint64_t key; uint32_t len;
        if (streams[i].read(reinterpret_cast<char*>(&key), sizeof(key))) {
            streams[i].read(reinterpret_cast<char*>(&len), sizeof(len));
            if (len > 0 && len <= PAYLOAD_MAX) {
                char* payload = new char[len];
                streams[i].read(payload, len);
                pq.push({Record(key, len, payload), i});
                delete[] payload;
            }
        }
    }

    for (auto& s : streams) s.close();
    out.close();

    std::cout << "Merged " << chunk_count << " chunks into " << output_file << std::endl;
}
