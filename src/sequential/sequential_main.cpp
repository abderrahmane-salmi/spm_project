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
        std::vector<size_t> mems = {64, 128, 256, 512, 1024};
        for (size_t mb : mems) {
            SequentialExternalMergeSort sorter(mb * 1024 * 1024);
            std::string out = "bench_" + std::to_string(mb) + "mb.bin";

            auto start = std::chrono::high_resolution_clock::now();
            bool ok = sorter.sort_file(input_file, out);
            auto end = std::chrono::high_resolution_clock::now();
            double dur = std::chrono::duration<double>(end - start).count();

            std::cout << "Memory: " << mb << "MB, Time: " << dur << "s" << std::endl;

            if (ok) verify_sorted_output(out);
            std::filesystem::remove(out);
        }
    }
    else {
        print_usage(argv[0]);
        return 1;
    }

    return 0;
}
