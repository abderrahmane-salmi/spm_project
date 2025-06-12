#include "include/record_io.hpp"
#include "include/sort.hpp"
#include <iostream>
#include <string>

int main() {
    std::string filename = "test_records.bin";

    // Create some sample records to write
    std::vector<Record> records = {
        Record(55, 10, "abcdefghij"),
        Record(89, 8,  "12345678"),
        Record(17, 12, "Hello World!")
    };

    // Writing records to file
    std::cout << "Writing records to file: " << filename << std::endl;
    write_records_to_file(filename, records);

    // Reading records back from file
    std::cout << "Reading records back from file..." << std::endl;
    std::vector<Record> read_back = read_records_from_file(filename);

    // Display read records
    for (const auto& rec : read_back) {
        std::cout << "Key: " << rec.key << ", Len: " << rec.len
                  << ", Payload: " << std::string(rec.payload, rec.len) << std::endl;
    }

    // Sorting records
    std::cout << "Sorting records by key..." << std::endl;
    sort_records_by_key(read_back);

    std::cout << "Sorted records:" << std::endl;
    for (const auto& rec : read_back) {
        std::cout << "Key: " << rec.key << ", Len: " << rec.len
                  << ", Payload: " << std::string(rec.payload, rec.len) << std::endl;
    }

    return 0;
}
