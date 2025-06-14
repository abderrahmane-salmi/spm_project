#include "../include/record_io.hpp"
#include "../include/sort.hpp"

#include <filesystem>  // C++17: to create directories
#include <fstream>
#include <iostream>
#include <vector>
#include <string>

constexpr size_t DEFAULT_CHUNK_SIZE = 10 * 1024 * 1024; // 10 MB

// Reads the large input file in small chunks, sorts them, and writes sorted chunks to temp_dir
void chunk_and_sort_file(const std::string& input_path, const std::string& temp_dir, size_t chunk_size = DEFAULT_CHUNK_SIZE) {
    std::ifstream infile(input_path, std::ios::binary);
    if (!infile) {
        std::cerr << "Error: failed to open input file: " << input_path << std::endl;
        return;
    }

    // Create temp directory if it doesn’t exist
    std::filesystem::create_directories(temp_dir);

    size_t chunk_index = 0;
    size_t bytes_read = 0;

    while (!infile.eof()) {
        std::vector<Record> records;
        size_t chunk_bytes = 0;

        // Read records into this chunk until we hit chunk_size
        while (chunk_bytes < chunk_size && infile.peek() != EOF) {
            uint64_t key;
            uint32_t len;

            // Read key and length
            infile.read(reinterpret_cast<char*>(&key), sizeof(key));
            infile.read(reinterpret_cast<char*>(&len), sizeof(len));

            if (infile.eof() || len > PAYLOAD_MAX || len == 0) break;

            // Read payload
            char* payload = new char[len];
            infile.read(payload, len);

            if (infile.gcount() != static_cast<std::streamsize>(len)) {
                delete[] payload;
                break;
            }

            chunk_bytes += sizeof(key) + sizeof(len) + len;

            records.emplace_back(key, len, payload);
            delete[] payload; // payload is copied in Record constructor
        }

        // Sort current chunk in memory
        sort_records_by_key(records);

        // Output chunk file path: temp_dir/chunk_0.bin, chunk_1.bin, etc.
        std::string chunk_path = temp_dir + "/chunk_" + std::to_string(chunk_index++) + ".bin";
        write_records_to_file(chunk_path, records);

        std::cout << "Success: wrote sorted chunk: " << chunk_path << " (" << records.size() << " records)" << std::endl;
    }

    infile.close();
    std::cout << "Finished chunking. Total chunks: " << chunk_index << "\n";
}
