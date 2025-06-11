#ifndef RECORD_HPP
#define RECORD_HPP

#include <string>
#include <vector>
#include <cstdint>

constexpr size_t PAYLOAD_MAX = 1024;

// the structure of a single Record
struct Record {
    uint64_t key;
    uint32_t len; // Length of the payload
    char* payload; // Pointer to dynamically allocated data (text/data)

    // Why char* and not std::string?
    // Because char* gives us more control. std::string adds some overhead and formatting

    // Default constructor
    Record();

    // Constructor with key, length, and payload data
    Record(uint64_t k, uint32_t l, const char* data);

    // Copy constructor
    Record(const Record& other);

    // Copy assignment operator
    Record& operator=(const Record& other);

    // Destructor (to avoid memory leaks)
    ~Record();
};

// We have 2 I/O functions:

// reads the binary file and reconstructs the records
std::vector<Record> read_records_from_file(const std::string& path, size_t block_size = 4096);

// writes a list of records into a binary file
void write_records_to_file(const std::string& path, const std::vector<Record>& records);

#endif // RECORD_HPP
