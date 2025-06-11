#include "record.hpp"
#include <fstream>
#include <stdexcept>
#include <cstring>

Record::Record() : key(0), len(0), payload(nullptr) {}

Record::Record(uint64_t k, uint32_t l, const char* data) : key(k), len(l) {
    payload = new char[len];
    std::memcpy(payload, data, len);
}

Record::Record(const Record& other) : key(other.key), len(other.len) {
    payload = new char[len];
    std::memcpy(payload, other.payload, len);
}

Record& Record::operator=(const Record& other) {
    if (this == &other) return *this;
    delete[] payload;
    key = other.key;
    len = other.len;
    payload = new char[len];
    std::memcpy(payload, other.payload, len);
    return *this;
}

Record::~Record() {
    delete[] payload;
}

// writes a list of records into a binary file
void write_records_to_file(const std::string& path, const std::vector<Record>& records) {
    std::ofstream out(path, std::ios::binary);
    if (!out) throw std::runtime_error("Failed to open file for writing");

    for (const auto& rec : records) {
        out.write(reinterpret_cast<const char*>(&rec.key), sizeof(uint64_t));
        out.write(reinterpret_cast<const char*>(&rec.len), sizeof(uint32_t));
        out.write(rec.payload, rec.len);
    }

    out.close();
}

// reads the binary file and reconstructs the records.
std::vector<Record> read_records_from_file(const std::string& path, size_t block_size) {
    std::ifstream in(path, std::ios::binary);
    if (!in) throw std::runtime_error("Failed to open file for reading");

    std::vector<Record> records;

    while (true) {
        uint64_t key;
        uint32_t len;

        in.read(reinterpret_cast<char*>(&key), sizeof(uint64_t));
        if (in.eof()) break;
        in.read(reinterpret_cast<char*>(&len), sizeof(uint32_t));
        if (in.eof()) break;

        if (len == 0 || len > PAYLOAD_MAX) throw std::runtime_error("Invalid payload length");

        char* payload = new char[len];
        in.read(payload, len);
        if (!in) {
            delete[] payload;
            throw std::runtime_error("Failed to read payload");
        }

        records.emplace_back(key, len, payload);
        delete[] payload; // We create a copy inside the Record constructor
    }

    in.close();
    return records;
}
