#include <iostream>
#include <string>
#include <filesystem>
#include "../include/record.hpp"
#include "sequential.hpp"
#include "../filegen/filegen.hpp"

void print_usage(const std::string& prog) {
    std::cout << "Usage:\n"
              << "  " << prog << " sort <input_file> [memory_MB]\n"
              << "  " << prog << " benchmark <input_file>\n";
}

int main(int argc, char* argv[]) {
    if (argc < 3) {
        print_usage(argv[0]);
        return 1;
    }

    std::string cmd = argv[1];
    std::string input_file = argv[2];
    size_t memory_mb = (argc >= 4) ? std::stoul(argv[3]) : 256;
    size_t memory_bytes = memory_mb * 1024 * 1024;

    if (!std::filesystem::exists(input_file)) {
        std::cerr << "File not found: " << input_file << std::endl;
        return 1;
    }

    std::filesystem::path path(input_file);
    std::string output_file = path.stem().string() + "_seq_output" + path.extension().string();

    if (cmd == "sort") {
        SequentialExternalMergeSort sorter(memory_bytes);
        if (!sorter.sort_file(input_file, output_file)) {
            std::cerr << "Sort failed." << std::endl;
            return 1;
        }
        verify_sorted_output(output_file);
    }
    else if (cmd == "benchmark") {
        if (argc < 4) {
            std::cerr << "Usage: " << argv[0] << " benchmark <input_file> <memory_limit_mb>" << std::endl;
            return 1;
        }

        std::string input_file = argv[2];
        size_t memory_limit_mb = std::stoul(argv[3]);
        size_t mem_bytes = memory_limit_mb * 1024 * 1024;

        SequentialExternalMergeSort sorter(mem_bytes);
        std::string out = "benchmark_output.bin";

        std::cout << "Benchmarking sort..." << std::endl;
        std::cout << "Input file: " << input_file << std::endl;
        std::cout << "Output file: " << out << std::endl;
        std::cout << "Memory limit: " << memory_limit_mb << "MB" << std::endl;

        auto start = std::chrono::high_resolution_clock::now();
        bool ok = sorter.sort_file(input_file, out);
        auto end = std::chrono::high_resolution_clock::now();
        double dur = std::chrono::duration<double>(end - start).count();

        if (ok) {
            std::cout << "Sort successful" << std::endl;
            std::cout << "[SEQ] File=" << input_file
                  << " Memory=" << memory_limit_mb
                  << " Time=" << dur << std::endl;
        } else {
            std::cerr << "Sort failed!" << std::endl;
        }

        std::filesystem::remove(out);
    }
    else {
        print_usage(argv[0]);
        return 1;
    }

    return 0;
}
