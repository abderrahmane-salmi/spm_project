#include <iostream>
#include <string>
#include <cstdlib>
#include "filegen.hpp"

void print_usage(const std::string& prog) {
    std::cout << "Usage:\n";
    std::cout << "  " << prog << " gen_count <filename> <num_records>\n";
    std::cout << "  " << prog << " gen_size  <filename> <size_in_MB>\n";
    std::cout << "  " << prog << " verify    <filename>\n";
    std::cout << "  " << prog << " compare   <file1> <file2>\n";
    std::cout << "  " << prog << " delete    <filename>\n";
}

int main(int argc, char* argv[]) {
    if (argc < 3) {
        print_usage(argv[0]);
        return 1;
    }

    std::string cmd = argv[1];
    FileGenerator gen;

    if (cmd == "gen_count") {
        if (argc != 4) {
            print_usage(argv[0]);
            return 1;
        }
        std::string filename = argv[2];
        size_t num_records = std::stoull(argv[3]);
        gen.generateFile(filename, num_records);
        std::cout << "Generated file: " << filename << " with " << num_records << " records\n";
    }

    else if (cmd == "gen_size") {
        if (argc != 4) {
            print_usage(argv[0]);
            return 1;
        }
        std::string filename = argv[2];
        size_t size_mb = std::stoull(argv[3]);
        gen.generateFileBySize(filename, size_mb * 1024 * 1024);
        std::cout << "Generated file: " << filename << " (~" << size_mb << " MB)\n";
    }

    else if (cmd == "verify") {
        if (argc != 3) {
            print_usage(argv[0]);
            return 1;
        }
        std::string filename = argv[2];
        verify_sorted_output(filename);
    }

    else if (cmd == "compare") {
        if (argc != 4) {
            print_usage(argv[0]);
            return 1;
        }
        std::string file1 = argv[2];
        std::string file2 = argv[3];
        compare_files(file1, file2);
    }

    else if (cmd == "delete") {
        if (argc != 3) {
            print_usage(argv[0]);
            return 1;
        }
        std::string filename = argv[2];
        delete_file(filename);
    }

    else {
        std::cerr << "Unknown command: " << cmd << "\n";
        print_usage(argv[0]);
        return 1;
    }

    return 0;
}


/* ## Example Usage ##

# Generate 100k records
./filegen gen_count test_100k.bin 100000

# Generate file ~500MB
./filegen gen_size test_500mb.bin 500

# Verify if a file is sorted
./filegen verify output_sorted.bin

# Compare two binary files
./filegen compare output1.bin output2.bin
 */