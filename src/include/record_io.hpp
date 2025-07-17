#pragma once
#include "record.hpp"
#include <fstream>
#include <vector>
#include <string>
#include <iostream>

constexpr size_t BLOCK_SIZE = 4096; // Read/write block size (4KB)

// Write a list of records to a binary file
// void write_records_to_file(const std::string& path, const std::vector<Record>& records) {
//     std::ofstream out(path, std::ios::binary); // Open file in binary write mode
//     if (!out) {
//         throw std::runtime_error("Failed to open file for writing: " + path);
//     }

//     for (const auto& rec : records) {
//         // Write key
//         out.write(reinterpret_cast<const char*>(&rec.key), sizeof(uint64_t));

//         // Write length of payload
//         out.write(reinterpret_cast<const char*>(&rec.len), sizeof(uint32_t));

//         // Write payload bytes using .data()
//         out.write(rec.payload.data(), rec.len);
//     }

//     out.close();
// }

// // Read records from a binary file block-by-block to avoid memory overload
// std::vector<Record> read_records_from_file(const std::string& path) {
//     std::ifstream in(path, std::ios::binary); // Open file in binary read mode
//     if (!in) {
//         throw std::runtime_error("Failed to open file for reading: " + path);
//     }

//     std::vector<Record> records;
//     std::vector<char> buffer(sizeof(uint64_t) + sizeof(uint32_t)); // buffer for key+len

//     while (in.read(buffer.data(), buffer.size())) {
//         // Read key and length
//         uint64_t key = *reinterpret_cast<uint64_t*>(buffer.data());
//         uint32_t len = *reinterpret_cast<uint32_t*>(buffer.data() + sizeof(uint64_t));

//         if (len > PAYLOAD_MAX || len == 0) throw std::runtime_error("Invalid payload length");

//         // Read payload of variable size
//         std::vector<char> payload(len);
//         if (!in.read(payload.data(), len)) break;

//         // Add record to vector using payload.data()
//         records.emplace_back(key, len, payload.data());
//     }

//     in.close();
//     return records;
// }
