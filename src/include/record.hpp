#pragma once
#include <cstdint>
#include <cstring>
#include <stdexcept>

constexpr size_t PAYLOAD_MAX = 1024;

// the structure of a single Record
struct Record {
    uint64_t key;
    uint32_t len; // Length of the payload
    char* payload; // Pointer to dynamically allocated data (text/data)

    // Why char* and not std::string?
    // Because char* gives us more control. std::string adds some overhead and formatting

    // Default constructor
    Record() : key(0), len(0), payload(nullptr) {}

    // Constructor with key, length, and payload data
    Record(uint64_t k, uint32_t l, const char* p)
        : key(k), len(l), payload(new char[l]) {
        std::memcpy(payload, p, l); // Copy the payload content
    }

    // Copy constructor
    Record(const Record& other)
        : key(other.key), len(other.len), payload(new char[other.len]) {
        std::memcpy(payload, other.payload, len);
    }

    // Copy assignment operator
    Record& operator=(const Record& other) {
        if (this != &other) {
            delete[] payload; // Release old payload
            key = other.key;
            len = other.len;
            payload = new char[len];
            std::memcpy(payload, other.payload, len);
        }
        return *this;
    }

    // Destructor (to avoid memory leaks)
    ~Record() {
        delete[] payload;
    }

    // Read record from binary input stream
    bool read_from_stream(std::istream& in) {
        if (!in.read(reinterpret_cast<char*>(&key), sizeof(key))) return false;
        if (!in.read(reinterpret_cast<char*>(&len), sizeof(len))) return false;

        // Sanity check
        if (len < 8 || len > PAYLOAD_MAX) {
            in.setstate(std::ios::failbit);
            return false;
        }

        delete[] payload;
        payload = new char[len];
        return static_cast<bool>(in.read(payload, len));
    }

    // Write record to binary output stream
    bool write_to_stream(std::ostream& out) const {
        if (!out.write(reinterpret_cast<const char*>(&key), sizeof(key))) return false;
        if (!out.write(reinterpret_cast<const char*>(&len), sizeof(len))) return false;
        return out.write(payload, len).good();
    }

    // Return total size of record (key + len + payload)
    size_t total_size() const {
        return sizeof(key) + sizeof(len) + len;
    }
};
