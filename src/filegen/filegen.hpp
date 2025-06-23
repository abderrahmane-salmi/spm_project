#ifndef FILE_UTILS_HPP
#define FILE_UTILS_HPP

#include <string>

class FileGenerator {
public:
    void generateFile(const std::string& filename, size_t num_records, uint32_t payload_len = 100);
    void generateFileBySize(const std::string& filename, size_t target_size_bytes, uint32_t payload_len = 100);
};

bool verify_sorted_output(const std::string& filename);

bool compare_files(const std::string& file1, const std::string& file2);

bool delete_file(const std::string& filename);

#endif // FILE_UTILS_HPP
