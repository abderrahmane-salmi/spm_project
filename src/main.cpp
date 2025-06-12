#include "include/record_io.hpp"
#include "include/sort.hpp"
#include <random>
#include <chrono>
#include <iostream>
#include <string>
#include <vector>
#include <fstream>

// --- Helper Functions ---
std::string random_payload(uint32_t len) {
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

bool is_sorted(const std::vector<Record>& records) {
    for (size_t i = 1; i < records.size(); ++i) {
        if (records[i-1].key > records[i].key) return false;
    }
    return true;
}

// --- Step 2 Logic ---
void step2SequentialMergeSort() {
    std::string filename = "test_records.bin";

    std::vector<Record> records = {
        Record(55, 10, "abcdefghij"),
        Record(89, 8,  "12345678"),
        Record(17, 12, "Hello World!")
    };

    std::cout << "Writing records to file: " << filename << std::endl;
    write_records_to_file(filename, records);

    std::cout << "Reading records back from file..." << std::endl;
    std::vector<Record> read_back = read_records_from_file(filename);

    for (const auto& rec : read_back) {
        std::cout << "Key: " << rec.key << ", Len: " << rec.len
                  << ", Payload: " << std::string(rec.payload, rec.len) << std::endl;
    }

    std::cout << "Sorting records by key..." << std::endl;
    sort_records_by_key(read_back);

    std::cout << "Sorted records:" << std::endl;
    for (const auto& rec : read_back) {
        std::cout << "Key: " << rec.key << ", Len: " << rec.len
                  << ", Payload: " << std::string(rec.payload, rec.len) << std::endl;
    }
}

// --- Step 3 – External Sort Phase 1: Chunking + Sorting Logic ---
void step3Chunking() {
    std::string filename = "large_test_file.bin";
    // size_t num_records = 10'000'000; // ~4.92GB
    size_t num_records = 500'000; // ~252MB

    std::vector<Record> records;
    records.reserve(1000); // optional buffer, not used here

    std::mt19937_64 rng(std::chrono::steady_clock::now().time_since_epoch().count());
    std::uniform_int_distribution<uint64_t> key_dist(1, 1'000'000'000);
    std::uniform_int_distribution<uint32_t> len_dist(10, 1024);

    std::ofstream out(filename, std::ios::binary);
    if (!out) {
        throw std::runtime_error("Failed to open file for writing: " + filename);
    }

    for (size_t i = 0; i < num_records; ++i) {
        uint64_t key = key_dist(rng);
        uint32_t len = len_dist(rng);
        std::string payload = random_payload(len);
        Record rec(key, len, payload.c_str());

        out.write(reinterpret_cast<const char*>(&rec.key), sizeof(uint64_t));
        out.write(reinterpret_cast<const char*>(&rec.len), sizeof(uint32_t));
        out.write(rec.payload, rec.len);
    }

    out.close();
    std::cout << "Generated " << num_records << " records in file: " << filename << std::endl;
}

// --- Select Main ---
int main() {
    int step = 3;

    if (step == 2) {
        step2SequentialMergeSort();
    } else if (step == 3) {
        step3Chunking();
    } else {
        std::cerr << "Invalid step number: " << step << std::endl;
        return 1;
    }

    return 0;
}
