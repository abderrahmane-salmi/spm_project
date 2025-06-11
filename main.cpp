#include "include/record.hpp"
#include <iostream>

int main() {
    // Create some sample records to write
    std::vector<Record> original_records = {
        {123, 12, "Hello World!"},
        {456, 8,  "12345678"},
        {789, 10, "abcdefghij"},
    };

    // Writing records to file
    std::cout << "Writing records to file...";
    write_records_to_file("records.bin", original_records);

    auto loaded = read_records_from_file("records.bin");

    // Display read records
    std::cout << "Loaded " << loaded.size() << " records:\n";
    for (const auto& rec : loaded) {
        std::cout << "Key: " << rec.key << ", Len: " << rec.len
                  << ", Payload: " << std::string(rec.payload, rec.len) << "\n";
    }

    return 0;
}
