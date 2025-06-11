#include "include/record_io.hpp"
#include <iostream>
#include <string>

int main() {
    std::string filename = "test_records.bin";

    // Create some sample records to write
    std::vector<Record> records = {
        Record(1, 10, "abcdefghij"),
        Record(2, 8,  "12345678"),
        Record(3, 12, "Hello World!")
    };

    std::cout << "Writing records to file: " << filename << std::endl;
    write_records_to_file(filename, records);

    std::cout << "Reading records back from file..." << std::endl;
    std::vector<Record> read_back = read_records_from_file(filename);

    // Display read records
    for (const auto& rec : read_back) {
        std::cout << "Key: " << rec.key << ", Len: " << rec.len
                  << ", Payload: " << std::string(rec.payload, rec.len) << std::endl;
    }

    return 0;
}
