#ifndef FASTFLOW_MERGESORT_HPP
#define FASTFLOW_MERGESORT_HPP

#include <ff/ff.hpp>
#include <ff/farm.hpp>
#include <vector>
#include <string>
#include <fstream>
#include <algorithm>
#include <queue>
#include <filesystem>
#include <memory>
#include <chrono>

#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>

#include "../include/record.hpp"
#include "../chunking/chunking.hpp"
#include "../merging/merging.hpp"

using namespace ff;

namespace fs = std::filesystem;

struct ChunkTask {
    int chunk_id;
    uint8_t* data;
    size_t size;
    std::string output_path;
};

// ------------------------
// FastFlow Node: Chunk Emitter
// ------------------------
// Distributes chunk sorting tasks to workers
class ChunkEmitter : public ff_node {
private:
    const std::string input_file_;
    std::string temp_dir_;
    uint8_t* mmap_ptr_{nullptr};
    size_t file_size_{0};
    size_t num_workers_;
    size_t per_worker_budget_;
    size_t safe_budget_;

public:
    ChunkEmitter(const std::string& infile, const std::string& temp_dir, size_t num_workers, size_t memory_budget)
        : input_file_(infile), temp_dir_(temp_dir), num_workers_(num_workers) {
        safe_budget_ = static_cast<size_t>(memory_budget * 0.8);
        per_worker_budget_ = safe_budget_ / num_workers_;
        // memory-map input
        int fd = open(input_file_.c_str(), O_RDONLY);
        if (fd < 0) throw std::runtime_error("open input failed");
        struct stat st;
        fstat(fd, &st);
        file_size_ = st.st_size;
        mmap_ptr_ = static_cast<uint8_t*>(mmap(nullptr, file_size_, PROT_READ, MAP_PRIVATE, fd, 0));
        close(fd);
        if (mmap_ptr_ == MAP_FAILED) throw std::runtime_error("mmap failed");
    }

    ~ChunkEmitter() {
        munmap(mmap_ptr_, file_size_);
    }

    /**
     * This service function is called by FastFlow for each item in the input stream.
     * In this case, it emits a SortTask for each chunk file in the input list.
     * On each svc() call, it sends a SortTask to a worker.
     * 
     * @return A SortTask for the next chunk file, or EOS to signal end of stream.
     */
    void* svc(void*) override {
        size_t est_chunks = std::max<size_t>(
            (file_size_ + per_worker_budget_ - 1) / per_worker_budget_,
            num_workers_
        );
        size_t est_size = file_size_ / est_chunks;

        size_t offset = 0, start = 0, acc = 0;
        int id = 0;

        while (offset + sizeof(uint64_t) + sizeof(uint32_t) <= file_size_) {
            uint64_t key = *reinterpret_cast<uint64_t*>(mmap_ptr_ + offset);
            offset += sizeof(key);
            uint32_t len = *reinterpret_cast<uint32_t*>(mmap_ptr_ + offset);
            offset += sizeof(len);
            if (len < 8 || len > PAYLOAD_MAX) break;
            if (offset + len > file_size_) break;
            size_t recsz = sizeof(key) + sizeof(len) + len;
            if (acc + recsz > est_size && acc > 0) {
                ChunkTask* tk = new ChunkTask{id, mmap_ptr_ + start, offset - sizeof(key) - sizeof(len) - start,
                                              fs::path(temp_dir_) / ("chunk_" + std::to_string(id) + ".bin")};
                ff_send_out(tk);
                id++;
                start = offset - sizeof(key) - sizeof(len);
                acc = 0;
            }
            offset += len;
            acc += recsz;
        }
        if (acc > 0 && start < file_size_) {
            ChunkTask* tk = new ChunkTask{id, mmap_ptr_ + start, file_size_ - start,
                                          fs::path(temp_dir_) / ("chunk_" + std::to_string(id) + ".bin")};
            ff_send_out(tk);
            id++;
        }
        return EOS;
    }
};

class ChunkPipeline : public ff_node {
private:
    size_t per_worker_budget_;
public:
    ChunkPipeline(size_t per_worker_budget) : per_worker_budget_(per_worker_budget) {}

    void* svc(void* t) override {
        auto *tk = static_cast<ChunkTask*>(t);

        // Parse
        std::vector<Record> records;
        size_t bytes = 0;
        size_t offset = 0;
        while (offset + sizeof(uint64_t) + sizeof(uint32_t) <= tk->size) {
            uint64_t key = *reinterpret_cast<uint64_t*>(tk->data + offset);
            offset += sizeof(key);
            uint32_t len = *reinterpret_cast<uint32_t*>(tk->data + offset);
            offset += sizeof(len);
            if (len < 8 || len > PAYLOAD_MAX || offset + len > tk->size) break;
            Record r(key, len, reinterpret_cast<char*>(tk->data + offset));
            offset += len;
            bytes += r.total_size();
            if (bytes > per_worker_budget_) {
                std::cerr << "[warn] worker " << tk->chunk_id
                          << " exceeded budget\n";
                break;
            }
            records.push_back(std::move(r));
        }

        // Sort
        std::sort(records.begin(), records.end(),
                  [](auto& a, auto& b){ return a.key < b.key; });

        // Write
        std::ofstream ofs(tk->output_path, std::ios::binary);
        for (auto& r : records) {
            r.write_to_stream(ofs);
        }
        // delete tk;
        return tk;
    }
};

class ChunkCollector : public ff_node {
private:
    std::vector<std::string> runs_;
    std::string final_out_;

public:
    ChunkCollector(const std::string& final_out): final_out_(final_out){}

    void* svc(void* tk) override {
        auto *task = static_cast<ChunkTask*>(tk);
        runs_.push_back(task->output_path);
        delete tk;
        return GO_ON;
    }

    void svc_end() override {
        if (!runs_.empty()) {
            merge_sorted_files(runs_, final_out_);
        }
    }

    // void on_end() {
    //     if (!runs_.empty()) {
    //         merge_sorted_files(runs_, final_out_);
    //     }
    // }
};

// ------------------------
// Main class: FastFlowExternalMergeSort
// ------------------------
// Coordinates the whole external merge sort: chunking, sorting, merging.
class FastFlowExternalMergeSort {
private:
    size_t memory_budget; // Total memory to use per chunk
    std::string temp_dir; // Directory for temporary files
    int num_workers; // Number of parallel workers

public:
    // Constructor: sets memory and workers, creates temp directory
    FastFlowExternalMergeSort(size_t memory_budget_bytes = 256 * 1024 * 1024, int workers = 4)
        : memory_budget(memory_budget_bytes), num_workers(workers) {
        temp_dir = "temp_ff_" + std::to_string(getpid());
        fs::create_directory(temp_dir);
    }

    // Destructor: removes temp files
    ~FastFlowExternalMergeSort() {
        cleanup_temp_files();
    }

    /**
     * Main entry point to sort an input file and save the sorted result.
     *
     * This function coordinates the external merge sort process using FastFlow.
     * It performs the following steps:
     * 1. Splits the input file into smaller chunks.
     * 2. Sorts the chunks in parallel using FastFlow Farm.
     * 3. Merges the sorted chunks into a single sorted output file.
     * 4. Cleans up temporary files created during the process.
     *
     * @param input_file The path to the input file to be sorted.
     * @param output_file The path to the file where the sorted result will be saved.
    */
    void sort_file(const std::string& input_file, const std::string& output_file) {
        using Clock = std::chrono::high_resolution_clock;

        std::cout << "FastFlow External MergeSort starting..." << std::endl;
        std::cout << "Memory budget: " << (memory_budget / 1024 / 1024) << " MB" << std::endl;
        std::cout << "Workers: " << num_workers << std::endl;

        auto t_start = Clock::now();

        ChunkEmitter emitter(input_file, temp_dir, num_workers, memory_budget);
        ChunkCollector collector(output_file);

        // Create Workers
        std::vector<ff_node*> workers_v;
        for (int i = 0; i < num_workers; ++i) {
            workers_v.push_back(new ChunkPipeline(memory_budget / num_workers));
        }

        // Set up FastFlow farm
        ff_farm farm;
        farm.add_emitter(&emitter);
        farm.add_workers(workers_v);
        farm.add_collector(&collector);

        // Run farm
        auto t1 = Clock::now();
        if (farm.run_and_wait_end() < 0) {
            std::cerr << "FastFlow farm execution failed!" << std::endl;
            return;
        }
        auto t2 = Clock::now();

        std::chrono::duration<double> total_time = t2 - t_start;
        std::chrono::duration<double> sort_time = t2 - t1;
        std::cout << "[TIMING] Sorting + Merging time: " << sort_time.count() << " s" << std::endl;
        std::cout << "[TIMING] Total time: " << total_time.count() << " s" << std::endl;

        cleanup_temp_files();
    }


private:
    void cleanup_temp_files() {
        if (fs::exists(temp_dir)) {
            fs::remove_all(temp_dir);
        }
    }
};

#endif // FASTFLOW_MERGESORT_HPP