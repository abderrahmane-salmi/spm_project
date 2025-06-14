#pragma once

#include "record.hpp"
#include "record_io.hpp"

#include <random>
#include <chrono>
#include <string>
#include <vector>
#include <fstream>
#include <iostream>

enum class PayloadMode {
    RANDOM_LEN,   // random [8, PAYLOAD_MAX]
    FIXED_LEN,    // all records have same length
    MAX_LEN       // all payloads are PAYLOAD_MAX
};

// Utility to generate a random payload of specified length
inline std::string random_payload(uint32_t len) {
    std::string result;
    result.reserve(len);
    static const char charset[] =
        "0123456789"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "abcdefghijklmnopqrstuvwxyz";

    static thread_local std::mt19937 rg(std::random_device{}());
    static thread_local std::uniform_int_distribution<> pick(0, sizeof(charset) - 2);

    for (uint32_t i = 0; i < len; ++i) {
        result += charset[pick(rg)];
    }

    return result;
}

// Generate test file with specified size and payload configuration
inline void generate_test_file(const std::string& path, size_t num_records, PayloadMode mode = PayloadMode::RANDOM_LEN, uint32_t fixed_len = 64) {
    std::ofstream out(path, std::ios::binary);
    if (!out) {
        throw std::runtime_error("Failed to open file for writing: " + path);
    }

    std::mt19937_64 rng(std::chrono::steady_clock::now().time_since_epoch().count());
    std::uniform_int_distribution<uint64_t> key_dist(1, 1'000'000'000);
    std::uniform_int_distribution<uint32_t> len_dist(8, PAYLOAD_MAX);

    for (size_t i = 0; i < num_records; ++i) {
        uint64_t key = key_dist(rng);

        uint32_t len = 0;
        switch (mode) {
            case PayloadMode::RANDOM_LEN:
                len = len_dist(rng);
                break;
            case PayloadMode::FIXED_LEN:
                len = fixed_len;
                break;
            case PayloadMode::MAX_LEN:
                len = PAYLOAD_MAX;
                break;
        }

        std::string payload = random_payload(len);
        Record rec(key, len, payload.c_str());

        out.write(reinterpret_cast<const char*>(&rec.key), sizeof(uint64_t));
        out.write(reinterpret_cast<const char*>(&rec.len), sizeof(uint32_t));
        out.write(rec.payload, rec.len);
    }

    out.close();
    std::cout << "✅ Generated " << num_records << " records in file: " << path << std::endl;
}


/* How to Use It in main.cpp:

#include "utils/generator.hpp"

int main() {
    // Small file: fits in memory
    generate_test_file("small_test.bin", 1000, PayloadMode::FIXED_LEN, 64);

    // Medium file: varies payloads
    generate_test_file("medium_test.bin", 500'000, PayloadMode::RANDOM_LEN);

    // Large file: simulates stress test with max payloads
    generate_test_file("large_test.bin", 2'000'000, PayloadMode::MAX_LEN);

    return 0;
}

*/