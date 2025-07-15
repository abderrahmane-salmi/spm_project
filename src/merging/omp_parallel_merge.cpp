#include <iostream>
#include <fstream>
#include <vector>
#include <queue>
#include <algorithm>
#include <memory>
#include <string>
#include <cstdint>
#include <filesystem>
#include <omp.h> 

#include "../include/record.hpp"
#include "../include/record_io.hpp"
    
std::string merge_filename(int pass, int id) {
        std::ostringstream oss;
        oss << "merge_pass_" << pass << "_" << id << ".bin";
        return oss.str();
    }

void kway_merge(const std::vector<std::string>& input_files, const std::string& output_file) {
    struct HeapItem {
        Record record;
        int stream_index;

        bool operator<(const HeapItem& a) const {
            return record.key > a.record.key;  // min-heap
        }
    };

    int k = input_files.size();
    std::vector<std::ifstream> streams(k);
    std::priority_queue<HeapItem> min_heap;

    for (int i = 0; i < k; ++i) {
        streams[i].open(input_files[i], std::ios::binary);
        Record rec;
        if (rec.read_from_stream(streams[i])) {
            min_heap.push({rec, i});
        }
    }

    std::ofstream out(output_file, std::ios::binary);

    while (!min_heap.empty()) {
        auto [record, idx] = min_heap.top();
        min_heap.pop();
        record.write_to_stream(out);

        Record next;
        if (next.read_from_stream(streams[idx])) {
            min_heap.push({next, idx});
        }
    }

    for (auto& s : streams) s.close();
}

void multistage_merge(std::vector<std::string>& files, int num_threads, std::string final_output) {
    int pass = 0;
    int fanin = std::min(8, num_threads);

    while (files.size() > 1) {
        std::vector<std::string> next_pass_files;
        int total_groups = (files.size() + fanin - 1) / fanin;

        #pragma omp parallel for num_threads(num_threads) schedule(dynamic)
        for (int i = 0; i < total_groups; ++i) {
            int start = i * fanin;
            int end = std::min(start + fanin, static_cast<int>(files.size()));
            std::vector<std::string> group(files.begin() + start, files.begin() + end);

            std::string out_file = merge_filename(pass, i);
            kway_merge(group, out_file);

            #pragma omp critical
            {
                next_pass_files.push_back(out_file);
            }
        }

        files = next_pass_files;
        pass++;
    }

    std::filesystem::rename(files.front(), final_output);
}