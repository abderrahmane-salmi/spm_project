#ifndef MERGING_HPP
#define MERGING_HPP

#include <string>
#include <vector>

/**
 * Merges multiple sorted files into a single sorted output file.
 *
 * @param sorted_files Vector of sorted file paths.
 * @param output_file Path to the output file.
 * @return true if successful, false otherwise.
 */
bool merge_sorted_files(const std::vector<std::string>& sorted_files, const std::string& output_file);

#endif // MERGING_HPP
