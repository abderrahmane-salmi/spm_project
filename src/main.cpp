#include "include/record_io.hpp"
#include "include/sort.hpp"
#include "chunk_sort.cpp"
#include "merge_sorted_chunks.cpp"
#include <random>
#include <chrono>
#include <iostream>
#include <string>
#include <vector>
#include <fstream>

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

// --- Generate Large Test File ---
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

bool is_sorted_file(const std::string& path) {
    std::vector<Record> records = read_records_from_file(path);
    for (size_t i = 1; i < records.size(); ++i) {
        if (records[i - 1].key > records[i].key) {
            std::cerr << "Error: Not sorted at index " << i << std::endl;
            return false;
        }
    }
    return true;
}

void generateLargeTestFile() {
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

// --- Step 3 – External Sort Phase 1: Chunking + Sorting Logic ---
void step3ChunkAndSort() {
    std::string input_path = "large_test_file.bin";
    std::string output_path = "sorted_large_test_file.bin";
    std::string temp_dir = "temp_chunks";

    std::cout << "Starting external sort...\n";
    auto start = std::chrono::steady_clock::now();

    chunk_and_sort_file(input_path, temp_dir);

    auto end = std::chrono::steady_clock::now();
    std::cout << "External sort completed in "
              << std::chrono::duration_cast<std::chrono::seconds>(end - start).count()
              << " seconds.\n";

    // 2. Count how many chunks were created
    size_t chunk_count = 0;
    for (const auto& entry : std::filesystem::directory_iterator(temp_dir)) {
        if (entry.is_regular_file() && entry.path().extension() == ".bin") {
            ++chunk_count;
        }
    }
    if (chunk_count == 0) {
        std::cerr << "No chunks found in " << temp_dir << std::endl;
        return;
    }

    // 3. Merge sorted chunks
    merge_sorted_chunks(temp_dir, output_path, chunk_count);

    // 4. Verify sorted file
    if (is_sorted_file(output_path)) {
        std::cout << "Sorted file is valid." << std::endl;
    } else {
        std::cerr << "Sorted file is not valid." << std::endl;
    }
}

// --- Select Main ---
int main() {
    int step = 3;

    if (step == 2) {
        step2SequentialMergeSort();
    } else if (step == 3) {
        // generateLargeTestFile();
        step3ChunkAndSort();
    } else {
        std::cerr << "Invalid step number: " << step << std::endl;
        return 1;
    }

    return 0;
}
