#pragma once
#include "record.hpp"
#include <string>

// Reads a large file, splits it into sorted chunks, and writes them to temp files
// Params:
// - input_path: path to the large binary input file
// - temp_dir: where to write the sorted chunks (e.g., "temp_chunks/")
void chunk_and_sort_file(const std::string& input_path, const std::string& temp_dir);
