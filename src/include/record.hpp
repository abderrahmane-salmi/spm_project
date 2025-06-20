#pragma once
#include <cstdint>
#include <vector>
#include <cstring>
#include <stdexcept>
#include <iostream>

constexpr size_t PAYLOAD_MAX = 1024;

// the structure of a single Record
struct Record {
    uint64_t key;
    uint32_t len; // Length of the payload
    std::vector<char> payload; // Use vector<char> instead of raw pointer

    // Default constructor
    Record() : key(0), len(0), payload() {}

    // Constructor with key, length, and payload data
    Record(uint64_t k, uint32_t l, const char* p)
        : key(k), len(l), payload(p, p + l) {
        if (len < 8 || len > PAYLOAD_MAX) {
            throw std::invalid_argument("Payload length out of range");
        }
    }

    Record(uint64_t k, const std::vector<char>& data)
    : key(k), len(static_cast<uint32_t>(data.size())), payload(data) {
    if (len < 8 || len > PAYLOAD_MAX) {
        throw std::invalid_argument("Payload length out of range");
    }
}

    // Copy constructor, assignment operator, and destructor
    // are implicitly correct with std::vector

    // Read record from binary input stream
    bool read_from_stream(std::istream& in) {
        if (!in.read(reinterpret_cast<char*>(&key), sizeof(key))) return false;
        if (!in.read(reinterpret_cast<char*>(&len), sizeof(len))) return false;

        // Sanity check
        if (len < 8 || len > PAYLOAD_MAX) {
            in.setstate(std::ios::failbit);
            return false;
        }

        payload.resize(len);
        return static_cast<bool>(in.read(payload.data(), len));
    }

    // Write record to binary output stream
    bool write_to_stream(std::ostream& out) const {
        if (!out.write(reinterpret_cast<const char*>(&key), sizeof(key))) return false;
        if (!out.write(reinterpret_cast<const char*>(&len), sizeof(len))) return false;
        return out.write(payload.data(), len).good();
    }

    // Return total size of record (key + len + payload)
    size_t total_size() const {
        return sizeof(key) + sizeof(len) + len;
    }

    bool operator<(const Record& other) const {
        return key < other.key;
    }
};
