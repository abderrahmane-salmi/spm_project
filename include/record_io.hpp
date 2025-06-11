#pragma once
#include "record.hpp"
#include <fstream>
#include <vector>
#include <string>
#include <iostream>

constexpr size_t BLOCK_SIZE = 4096; // Read/write block size (4KB)

// Write a list of records to a binary file
void write_records_to_file(const std::string& path, const std::vector<Record>& records) {
    std::ofstream out(path, std::ios::binary); // Open file in binary write mode
    if (!out) {
        throw std::runtime_error("Failed to open file for writing: " + path);
    }

    for (const auto& rec : records) {
        // Write key
        out.write(reinterpret_cast<const char*>(&rec.key), sizeof(uint64_t));

        // Write length of payload
        out.write(reinterpret_cast<const char*>(&rec.len), sizeof(uint32_t));

        // Write payload bytes
        out.write(rec.payload, rec.len);
    }

    out.close();
}

// Read records from a binary file block-by-block to avoid memory overload
std::vector<Record> read_records_from_file(const std::string& path) {
    std::vector<Record> records;

    std::ifstream in(path, std::ios::binary); // Open file in binary read mode
    if (!in) {
        throw std::runtime_error("Failed to open file for reading: " + path);
    }

    char buffer[BLOCK_SIZE]; // Temporary buffer for reading
    while (in.read(buffer, sizeof(uint64_t) + sizeof(uint32_t))) {
        // Read key and length
        uint64_t key = *reinterpret_cast<uint64_t*>(buffer);
        uint32_t len;
        in.read(reinterpret_cast<char*>(&len), sizeof(uint32_t));

        if (in.eof()) break;

        // Read payload of variable size
        char* payload = new char[len];
        in.read(payload, len);

        if (in.eof()) {
            delete[] payload;
            break;
        }

        // Add record to vector
        records.emplace_back(key, len, payload);

        // Free memory (payload gets copied inside Record)
        delete[] payload;
    }

    in.close();
    return records;
}
