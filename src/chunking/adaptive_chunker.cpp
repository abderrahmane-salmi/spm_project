#pragma once
#include <vector>
#include <string>
#include <cstddef>
#include <fstream>
#include <algorithm>
#include <sys/stat.h>
#include <thread>
#include <iostream>
#include <cmath>

#include "../include/record.hpp"

struct ChunkInfo {
    size_t offset_bytes;
    size_t length_bytes;
    size_t num_records;
    size_t estimated_memory_usage; // Including overhead for sorting

    ChunkInfo(size_t offset, size_t length, size_t records, size_t memory_est)
        : offset_bytes(offset), length_bytes(length), num_records(records), 
          estimated_memory_usage(memory_est) {}
};

struct ChunkingConfig {
    size_t file_size_bytes;
    size_t available_memory_bytes;
    size_t num_threads;
    size_t min_chunk_size_mb;
    size_t max_chunk_size_mb;
    double memory_safety_factor;
    size_t io_buffer_size;
    
    // Performance tuning parameters
    bool optimize_for_io;      // true for HDD, false for SSD
    bool balance_load;         // true to balance chunk sizes
    size_t merge_fan_out;      // k-way merge parameter
    
    ChunkingConfig() 
        : file_size_bytes(0)
        , available_memory_bytes(get_available_memory())
        , num_threads(std::thread::hardware_concurrency())
        , min_chunk_size_mb(64)   // Minimum 64MB per chunk
        , max_chunk_size_mb(2048) // Maximum 2GB per chunk
        , memory_safety_factor(0.8) // Use 80% of available memory
        , io_buffer_size(64 * 1024 * 1024) // 64MB I/O buffer
        , optimize_for_io(true)
        , balance_load(true)
        , merge_fan_out(64) {}
        
private:
    static size_t get_available_memory() {
        // Try to detect available system memory
        // This is a simplified version - in practice you'd use system calls
        return 32ULL * 1024 * 1024 * 1024; // Default to 32GB
    }
};

class AdaptiveChunker {
private:
    ChunkingConfig config_;
    
    // Estimate memory overhead for sorting (std::sort uses ~2x memory)
    static constexpr double SORTING_MEMORY_OVERHEAD = 2.5;
    
    // Estimate record structure overhead in memory vs file
    static constexpr double RECORD_MEMORY_OVERHEAD = 1.3;
    
public:
    explicit AdaptiveChunker(const ChunkingConfig& config = ChunkingConfig())
        : config_(config) {}
    
    /**
     * Intelligently determine optimal chunking strategy
     */
    std::vector<ChunkInfo> analyze_file_adaptive(const std::string& input_file);
    
    /**
     * Generate chunk files with optimal strategy
     */
    std::vector<std::string> generate_adaptive_chunks(
        const std::string& input_file, 
        const std::string& temp_dir);
    
    /**
     * Update configuration based on runtime conditions
     */
    void update_config(size_t num_threads, size_t available_memory = 0);
    
    /**
     * Get recommended merge strategy based on chunk count
     */
    size_t get_optimal_merge_fanout(size_t num_chunks) const;
    
private:
    /**
     * Calculate optimal chunk size based on multiple factors
     */
    size_t calculate_optimal_chunk_size(size_t file_size, size_t sample_record_size);
    
    /**
     * Sample file to estimate average record size and distribution
     */
    struct FileSample {
        size_t avg_record_size;
        size_t min_record_size;
        size_t max_record_size;
        size_t total_records_sampled;
        double size_variance;
    };
    
    FileSample sample_file(const std::string& input_file, size_t sample_size = 1000);
    
    /**
     * Balance chunk sizes for better load distribution
     */
    std::vector<ChunkInfo> balance_chunks(std::vector<ChunkInfo> chunks);
    
    /**
     * Estimate I/O vs CPU bound characteristics
     */
    bool is_io_bound_workload(const FileSample& sample) const;
    
    /**
     * Get file size
     */
    size_t get_file_size(const std::string& filename);
    
    /**
     * Print chunking strategy summary
     */
    void print_strategy_summary(const std::vector<ChunkInfo>& chunks, 
                               const FileSample& sample) const;
};

// Implementation
inline size_t AdaptiveChunker::get_file_size(const std::string& filename) {
    struct stat st;
    if (stat(filename.c_str(), &st) == 0) {
        return st.st_size;
    }
    return 0;
}

inline AdaptiveChunker::FileSample AdaptiveChunker::sample_file(
    const std::string& input_file, size_t sample_size) {
    
    std::ifstream in(input_file, std::ios::binary);
    if (!in.is_open()) {
        throw std::runtime_error("Cannot open file for sampling: " + input_file);
    }
    
    FileSample sample = {};
    std::vector<size_t> record_sizes;
    record_sizes.reserve(sample_size);
    
    // Sample records from different parts of the file
    size_t file_size = get_file_size(input_file);
    if (file_size == 0) {
        throw std::runtime_error("Input file is empty: " + input_file);
    }
    size_t sample_interval = std::max<size_t>(1ULL, file_size / sample_size);
    
    Record record;
    size_t total_size = 0;
    size_t samples_taken = 0;
    
    for (size_t i = 0; i < sample_size && in.good(); ++i) {
        // Jump to different positions in file for better sampling
        size_t pos = (i * sample_interval) % file_size;
        in.seekg(pos);
        
        // Find next record boundary (simple approach)
        while (in.good() && samples_taken < sample_size) {
            size_t pos_before = in.tellg();
            if (record.read_from_stream(in)) {
                size_t record_size = record.total_size();
                record_sizes.push_back(record_size);
                total_size += record_size;
                samples_taken++;
                break;
            } else {
                // Try next byte if record reading failed
                in.clear();
                in.seekg(pos_before + 1);
            }
        }
    }
    
    if (samples_taken == 0) {
        throw std::runtime_error("Could not sample any records from file");
    }
    
    // Calculate statistics
    sample.total_records_sampled = samples_taken;
    sample.avg_record_size = total_size / samples_taken;
    sample.min_record_size = *std::min_element(record_sizes.begin(), record_sizes.end());
    sample.max_record_size = *std::max_element(record_sizes.begin(), record_sizes.end());
    
    // Calculate variance
    double variance_sum = 0.0;
    for (size_t size : record_sizes) {
        double diff = static_cast<double>(size) - sample.avg_record_size;
        variance_sum += diff * diff;
    }
    sample.size_variance = variance_sum / samples_taken;
    
    return sample;
}

inline size_t AdaptiveChunker::calculate_optimal_chunk_size(
    size_t file_size, size_t sample_record_size) {
    
    // Base chunk size calculation
    size_t available_per_chunk = config_.available_memory_bytes * config_.memory_safety_factor;
    
    // Account for sorting overhead
    size_t usable_memory = available_per_chunk / SORTING_MEMORY_OVERHEAD;
    
    // Account for record structure overhead in memory
    size_t effective_chunk_size = usable_memory / RECORD_MEMORY_OVERHEAD;
    
    // Adjust based on number of threads
    if (config_.num_threads > 1) {
        // With multiple threads, we might want smaller chunks for better parallelism
        // But not too small to avoid I/O overhead
        size_t thread_adjusted_size = effective_chunk_size / 
            std::max(1UL, config_.num_threads / 2);
        effective_chunk_size = std::max(effective_chunk_size / 4, thread_adjusted_size);
    }
    
    // Apply file-size-based heuristics
    if (file_size < config_.available_memory_bytes) {
        // Small file - use fewer, larger chunks
        effective_chunk_size = std::max(effective_chunk_size, file_size / 4);
    } else if (file_size > 100ULL * 1024 * 1024 * 1024) {
        // Very large file - ensure reasonable chunk count
        size_t max_reasonable_chunks = 1000;
        effective_chunk_size = std::max(effective_chunk_size, 
                                       file_size / max_reasonable_chunks);
    }
    
    // Apply min/max constraints
    size_t min_size = config_.min_chunk_size_mb * 1024 * 1024;
    size_t max_size = config_.max_chunk_size_mb * 1024 * 1024;
    effective_chunk_size = std::clamp(effective_chunk_size, min_size, max_size);

    // Ensure we don't underutilize workers (at least one chunk per thread)
    size_t min_chunks = std::min(config_.num_threads, static_cast<size_t>(file_size / min_size));
    if (min_chunks > 0) {
        size_t max_chunk_size_for_parallelism = file_size / min_chunks;
        effective_chunk_size = std::min(effective_chunk_size, max_chunk_size_for_parallelism);
    }

    // Ensure chunk size is reasonable relative to record size, but only if we have enough records
    size_t min_records_per_chunk = 1000;
    size_t total_estimated_records = file_size / sample_record_size;

    if (total_estimated_records > min_records_per_chunk) {
        size_t min_size_for_records = sample_record_size * min_records_per_chunk;
        effective_chunk_size = std::max(effective_chunk_size, min_size_for_records);
    }

    
    // Ensure chunk size is reasonable relative to record size
    // size_t min_records_per_chunk = 1000; // At least 1000 records per chunk
    // size_t min_size_for_records = sample_record_size * min_records_per_chunk;
    // effective_chunk_size = std::max(effective_chunk_size, min_size_for_records);
    
    return effective_chunk_size;
}

inline bool AdaptiveChunker::is_io_bound_workload(const FileSample& sample) const {
    // Heuristic: if records are large and have high variance, likely I/O bound
    return sample.avg_record_size > 512 || sample.size_variance > (sample.avg_record_size * 0.5);
}

inline std::vector<ChunkInfo> AdaptiveChunker::analyze_file_adaptive(
    const std::string& input_file) {
    
    // Step 1: Sample the file to understand its characteristics
    auto sample = sample_file(input_file);
    config_.file_size_bytes = get_file_size(input_file);
    
    // Step 2: Calculate optimal chunk size
    size_t optimal_chunk_size = calculate_optimal_chunk_size(
        config_.file_size_bytes, sample.avg_record_size);
    
    // Step 3: Adjust for workload characteristics
    if (is_io_bound_workload(sample)) {
        // For I/O bound workloads, prefer larger chunks to reduce I/O overhead
        optimal_chunk_size = static_cast<size_t>(std::min<double>(
            optimal_chunk_size * 1.5,
            static_cast<double>(config_.max_chunk_size_mb * 1024 * 1024)
        ));
    }
    
    // Step 3.5: Adjust chunk size to ensure even number of chunks
    size_t initial_chunk_count = (config_.file_size_bytes + optimal_chunk_size - 1) / optimal_chunk_size;
    if (initial_chunk_count % 2 != 0) {
        // Increase chunk count by 1 to make it even
        size_t adjusted_chunk_count = initial_chunk_count + 1;
        optimal_chunk_size = (config_.file_size_bytes + adjusted_chunk_count - 1) / adjusted_chunk_count;
        // Clamp to min/max chunk size again
        size_t min_size = config_.min_chunk_size_mb * 1024 * 1024;
        size_t max_size = config_.max_chunk_size_mb * 1024 * 1024;
        optimal_chunk_size = std::clamp(optimal_chunk_size, min_size, max_size);
    }
    
    std::cout << "Adaptive chunking analysis:" << std::endl;
    std::cout << "File size: " << (config_.file_size_bytes / 1024 / 1024) << " MB" << std::endl;
    std::cout << "Avg record size: " << sample.avg_record_size << " bytes" << std::endl;
    std::cout << "Optimal chunk size: " << (optimal_chunk_size / 1024 / 1024) << " MB" << std::endl;
    
    // Step 4: Create chunks with the calculated (and adjusted) size
    std::ifstream in(input_file, std::ios::binary);
    if (!in.is_open()) {
        throw std::runtime_error("Cannot open input file: " + input_file);
    }
    
    std::vector<ChunkInfo> chunks;
    size_t current_offset = 0;
    
    while (in.good()) {
        size_t chunk_start = current_offset;
        size_t bytes_used = 0;
        size_t record_count = 0;
        
        Record record;
        while (in.good() && bytes_used < optimal_chunk_size) {
            size_t pos_before = in.tellg();
            
            if (record.read_from_stream(in)) {
                size_t pos_after = in.tellg();
                size_t record_size = pos_after - pos_before;
                bytes_used += record_size;
                current_offset += record_size;
                record_count++;
            } else {
                break;
            }
        }
        
        if (record_count > 0) {
            size_t memory_estimate = bytes_used * RECORD_MEMORY_OVERHEAD * SORTING_MEMORY_OVERHEAD;
            chunks.emplace_back(chunk_start, bytes_used, record_count, memory_estimate);
        }
        
        if (current_offset >= config_.file_size_bytes) break;
    }
    
    // Step 5: Balance chunks if requested
    if (config_.balance_load && chunks.size() > 1) {
        chunks = balance_chunks(std::move(chunks));
    }
    
    print_strategy_summary(chunks, sample);
    return chunks;
}

inline std::vector<ChunkInfo> AdaptiveChunker::balance_chunks(std::vector<ChunkInfo> chunks) {
    if (chunks.size() < 2) return chunks;
    
    // Calculate total size and target size per chunk
    size_t total_bytes = 0;
    for (const auto& chunk : chunks) {
        total_bytes += chunk.length_bytes;
    }
    
    size_t target_size = total_bytes / chunks.size();
    double imbalance_threshold = 0.2; // 20% imbalance tolerance
    
    // Check if balancing is needed
    bool needs_balancing = false;
    for (const auto& chunk : chunks) {
        double ratio = static_cast<double>(chunk.length_bytes) / target_size;
        if (ratio < (1.0 - imbalance_threshold) || ratio > (1.0 + imbalance_threshold)) {
            needs_balancing = true;
            break;
        }
    }
    
    if (!needs_balancing) {
        std::cout << "Chunks are already well-balanced." << std::endl;
        return chunks;
    }
    
    std::cout << "Rebalancing chunks for better load distribution..." << std::endl;
    
    // Simple rebalancing: merge small chunks, split large ones
    std::vector<ChunkInfo> balanced_chunks;
    
    for (size_t i = 0; i < chunks.size(); ++i) {
        auto& current = chunks[i];
        
        if (current.length_bytes < target_size * 0.8 && i + 1 < chunks.size()) {
            // Merge with next chunk if both are small
            auto& next = chunks[i + 1];
            if (current.length_bytes + next.length_bytes <= target_size * 1.5) {
                balanced_chunks.emplace_back(
                    current.offset_bytes,
                    current.length_bytes + next.length_bytes,
                    current.num_records + next.num_records,
                    current.estimated_memory_usage + next.estimated_memory_usage
                );
                ++i; // Skip next chunk as it's merged
                continue;
            }
        }
        
        balanced_chunks.push_back(current);
    }
    
    return balanced_chunks;
}

inline std::vector<std::string> AdaptiveChunker::generate_adaptive_chunks(
    const std::string& input_file, const std::string& temp_dir) {
    
    auto chunks = analyze_file_adaptive(input_file);
    std::vector<std::string> chunk_files;
    chunk_files.reserve(chunks.size());
    
    std::ifstream in(input_file, std::ios::binary);
    if (!in.is_open()) {
        throw std::runtime_error("Cannot open input file: " + input_file);
    }
    
    // Use larger I/O buffer for better performance
    std::vector<char> io_buffer(config_.io_buffer_size);
    
    for (size_t i = 0; i < chunks.size(); ++i) {
        const ChunkInfo& info = chunks[i];
        std::string chunk_file = temp_dir + "/chunk_" + std::to_string(i) + ".bin";
        
        std::ofstream out(chunk_file, std::ios::binary);
        if (!out.is_open()) {
            throw std::runtime_error("Cannot create chunk file: " + chunk_file);
        }
        
        in.seekg(info.offset_bytes, std::ios::beg);
        
        // Copy data in optimized chunks
        size_t remaining = info.length_bytes;
        while (remaining > 0) {
            size_t to_read = std::min(remaining, io_buffer.size());
            in.read(io_buffer.data(), to_read);
            size_t actually_read = in.gcount();
            
            if (actually_read == 0) break;
            
            out.write(io_buffer.data(), actually_read);
            remaining -= actually_read;
        }
        
        out.close();
        chunk_files.push_back(chunk_file);
    }
    
    in.close();
    return chunk_files;
}

inline void AdaptiveChunker::update_config(size_t num_threads, size_t available_memory) {
    config_.num_threads = num_threads;
    if (available_memory > 0) {
        config_.available_memory_bytes = available_memory;
    }
    
    // Adjust parameters based on thread count
    if (num_threads == 1) {
        // Sequential execution - optimize for I/O
        config_.min_chunk_size_mb = 128;
        config_.memory_safety_factor = 0.9;
    } else if (num_threads <= 4) {
        // Low parallelism - moderate chunk sizes
        config_.min_chunk_size_mb = 64;
        config_.memory_safety_factor = 0.8;
    } else {
        // High parallelism - smaller chunks for better distribution
        config_.min_chunk_size_mb = 32;
        config_.memory_safety_factor = 0.7;
    }
}

inline size_t AdaptiveChunker::get_optimal_merge_fanout(size_t num_chunks) const {
    // Optimize merge fanout based on chunk count and available memory
    size_t max_fanout = config_.available_memory_bytes / (64 * 1024 * 1024); // 64MB per stream
    max_fanout = std::min(max_fanout, static_cast<size_t>(256)); // Reasonable upper limit
    
    if (num_chunks <= max_fanout) {
        return num_chunks; // Single merge phase
    }
    
    // Multi-level merge - find optimal fanout
    size_t optimal_fanout = static_cast<size_t>(std::sqrt(num_chunks));
    return std::clamp(optimal_fanout, static_cast<size_t>(8), max_fanout);
}

inline void AdaptiveChunker::print_strategy_summary(
    const std::vector<ChunkInfo>& chunks, const FileSample& sample) const {
    
    std::cout << "\n=== Chunking Strategy Summary ===" << std::endl;
    std::cout << "Total chunks: " << chunks.size() << std::endl;
    std::cout << "Avg chunk size: " << (config_.file_size_bytes / chunks.size() / 1024 / 1024) << " MB" << std::endl;
    std::cout << "Memory per chunk: " << (config_.available_memory_bytes * config_.memory_safety_factor / 1024 / 1024) << " MB" << std::endl;
    std::cout << "Estimated records per chunk: " << (chunks.empty() ? 0 : chunks[0].num_records) << std::endl;
    std::cout << "Workload type: " << (is_io_bound_workload(sample) ? "I/O bound" : "CPU bound") << std::endl;
    std::cout << "Recommended merge fanout: " << get_optimal_merge_fanout(chunks.size()) << std::endl;
    std::cout << "================================\n" << std::endl;
}