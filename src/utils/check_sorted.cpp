// Description: This code checks whether a binary file of integers is sorted or not
// g++ -o check_sorted utils/check_sorted.cpp
// ./check_sorted data/output200m_omp.bin

#include <iostream>
#include <fstream>
#include <string>
#include <vector>

int main(int argc, char* argv[]) {
    // Check if exactly one argument (the filename) is provided
    if (argc != 2) {
        std::cerr << "Usage: ./check_sorted <binary_file>" << std::endl;
        return 1;
    }

    // Retrieve the filename from command-line argument
    std::string filename = argv[1];

    // Open the binary file for reading
    std::ifstream file(filename, std::ios::binary);

    // Check if the file was opened successfully
    if (!file) {
        std::cerr << "Error opening file: " << filename << std::endl;
        return 1;
    }

    int prev, curr; // Variables to hold the current and previous integers
    bool first = true; // Flag to handle the first integer read
    size_t index = 0; // Counter to keep track of the position of each integer

    // Read the file one integer at a time
    while (file.read(reinterpret_cast<char*>(&curr), sizeof(int))) {
        // If this is not the first integer, compare it with the previous
        if (!first && curr < prev) {
            std::cerr << "Failure: File is not sorted: value " << curr << " at index " << index
                      << " is smaller than previous value " << prev << std::endl;
            return 1; // Exit early if not sorted
        }
        prev = curr; // Update previous value
        first = false; // First value has now been processed
        ++index; // Increment position counter
    }

    // If the loop completes, the file is sorted
    std::cout << "Success: File is correctly sorted (" << index << " integers)." << std::endl;
    return 0;
}
