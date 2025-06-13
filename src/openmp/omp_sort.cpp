// Description: OpenMP version of external sorting using binary files
// Compile with: g++ -fopenmp -O2 -o omp_sort src/shared_mem/omp_sort.cpp

#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <algorithm>
#include <omp.h>
#include <filesystem>

namespace fs = std::filesystem;

// Function to read a chunk of integers from a binary file
std::vector<int> read_chunk(const std::string& filename, size_t offset, size_t count) {
    std::vector<int> data(count);
    std::ifstream in(filename, std::ios::binary);
    in.seekg(offset * sizeof(int));
    in.read(reinterpret_cast<char*>(data.data()), count * sizeof(int));
    return data;
}

// Function to write a sorted chunk to a binary file
void write_chunk(const std::string& filename, const std::vector<int>& data) {
    std::ofstream out(filename, std::ios::binary);
    out.write(reinterpret_cast<const char*>(data.data()), data.size() * sizeof(int));
}

// Merge sorted temporary chunk files into one final sorted file
void merge_chunks(const std::vector<std::string>& temp_files, const std::string& output_file) {
    size_t k = temp_files.size();
    std::vector<std::ifstream> streams(k);
    std::vector<int> buffer(k);
    std::vector<bool> finished(k, false);

    // Open all input chunk files
    for (size_t i = 0; i < k; ++i) {
        streams[i].open(temp_files[i], std::ios::binary);
        if (!streams[i].read(reinterpret_cast<char*>(&buffer[i]), sizeof(int))) {
            finished[i] = true;
        }
    }

    std::ofstream out(output_file, std::ios::binary);

    while (true) {
        int min_value = INT32_MAX;
        int min_index = -1;
        for (size_t i = 0; i < k; ++i) {
            if (!finished[i] && buffer[i] < min_value) {
                min_value = buffer[i];
                min_index = i;
            }
        }
        if (min_index == -1) break; // All streams finished

        out.write(reinterpret_cast<char*>(&min_value), sizeof(int));

        if (!streams[min_index].read(reinterpret_cast<char*>(&buffer[min_index]), sizeof(int))) {
            finished[min_index] = true;
        }
    }

    for (auto& s : streams) s.close();
}

int main(int argc, char* argv[]) {
    if (argc != 4) {
        std::cerr << "Usage: ./omp_sort input.bin chunk_size output.bin\n";
        return 1;
    }

    std::string input_file = argv[1];
    size_t chunk_size = std::stoull(argv[2]);
    std::string output_file = argv[3];

    // Determine total number of integers in the file
    std::ifstream in(input_file, std::ios::binary | std::ios::ate);
    size_t file_size = in.tellg();
    size_t num_ints = file_size / sizeof(int);
    in.close();

    size_t num_chunks = (num_ints + chunk_size - 1) / chunk_size;
    std::vector<std::string> temp_files(num_chunks);

    // Step 1: Sort each chunk in parallel
    #pragma omp parallel for
    for (size_t i = 0; i < num_chunks; ++i) {
        size_t start = i * chunk_size;
        size_t count = std::min(chunk_size, num_ints - start);
        std::vector<int> data = read_chunk(input_file, start, count);
        std::sort(data.begin(), data.end());
        std::string temp_file = "chunk_" + std::to_string(i) + ".bin";
        write_chunk(temp_file, data);
        temp_files[i] = temp_file;
    }

    // Step 2: Merge all sorted chunks into one final output file
    merge_chunks(temp_files, output_file);

    // Step 3: Clean up temporary files
    for (const auto& f : temp_files) {
        fs::remove(f);
    }

    std::cout << "Sorting complete. Output written to: " << output_file << std::endl;
    return 0;
}
