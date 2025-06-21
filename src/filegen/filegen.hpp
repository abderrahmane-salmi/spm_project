#ifndef FILE_UTILS_HPP
#define FILE_UTILS_HPP

#include <string>

// === Helper struct to generate binary test files with records ===
class FileGenerator {
public:
    void generateFile(const std::string& filename, size_t num_records);
    void generateFileBySize(const std::string& filename, size_t target_size_bytes);
};

bool verify_sorted_output(const std::string& filename);

bool compare_files(const std::string& file1, const std::string& file2);

bool delete_file(const std::string& filename);

#endif // FILE_UTILS_HPP
