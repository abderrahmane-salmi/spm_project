#include "mpi.hpp"
#include <iostream>
#include <fstream>
#include <algorithm>
#include <chrono>
#include <filesystem>
#include <cstring>

#include "../include/record.hpp"
#include "../chunking/chunking.hpp"
#include "../merging/merging.hpp"
#include "../openmp/omp.hpp"

/**
 * Main MPI sorting function
 * 
 * This function coordinates the sorting process using MPI and OpenMP.
 * It performs the following steps:
 * 1. Calculates the file partition for this rank
 * 2. Reads records from the assigned partition
 * 3. Sorts the local records using OpenMP
 * 4. Performs a distributed merge phase using MPI
 * 
 * @param input_file Path to input file
 * @param output_file Path to output file
 * @param memory_budget Memory budget per rank in bytes
 * @param temp_dir Directory for temporary files
 */
void mpi_sort_file(
    const std::string& input_file,
    const std::string& output_file,
    size_t memory_budget,
    const std::string& temp_dir
) {
    // Get the rank and size of the MPI communicator
    int rank, size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    
    auto start_time = std::chrono::high_resolution_clock::now();
    
    if (rank == 0) {
        std::cout << "MPI MergeSort starting with " << size << " processes" << std::endl;
        std::cout << "Memory budget per rank: " << (memory_budget / (1024*1024)) << " MB" << std::endl;
    }
    
    // Create rank-specific temp directory
    std::string rank_temp_dir = temp_dir + "/rank_" + std::to_string(rank);
    std::filesystem::create_directories(rank_temp_dir);
    
    // Step 1: Calculate file partition for this rank
    size_t start_offset, end_offset;
    calculate_file_partition(input_file, rank, size, start_offset, end_offset);
    
    if (rank == 0) {
        std::cout << "File partitioning completed" << std::endl;
    }
    
    // Step 2: Read records from assigned partition
    std::vector<Record> local_records;
    read_records_from_range(input_file, start_offset, end_offset, local_records);
    
    std::cout << "Rank " << rank << ": Read " << local_records.size() 
              << " records from offset " << start_offset << " to " << end_offset << std::endl;
    
    // Step 3: Local sorting using OpenMP (reusing existing logic)
    auto sort_start = std::chrono::high_resolution_clock::now();

    // Save local_records to temp file
    std::string local_input_file = rank_temp_dir + "/local_input.bin";
    std::ofstream ofs(local_input_file, std::ios::binary);
    for (const auto& rec : local_records) {
        rec.write_to_stream(ofs);
    }
    ofs.close();

    // Sort using OpenMP class
    std::string local_sorted_file = rank_temp_dir + "/local_sorted.bin";
    OpenMPExternalMergeSort omp_sorter(memory_budget, 0, rank_temp_dir);
    omp_sorter.sort_file(local_input_file, local_sorted_file);

    // Load sorted records back into memory
    local_records.clear();
    std::ifstream ifs(local_sorted_file, std::ios::binary);
    Record rec;
    while (rec.read_from_stream(ifs)) {
        local_records.push_back(rec);
    }
    ifs.close();
    
    auto sort_end = std::chrono::high_resolution_clock::now();
    auto sort_duration = std::chrono::duration_cast<std::chrono::milliseconds>(sort_end - sort_start);
    
    std::cout << "Rank " << rank << ": Local OpenMP sorting completed in " 
              << sort_duration.count() << " ms" << std::endl;
    
    // Step 4: Distributed merge phase
    distributed_merge(local_records, output_file, rank, size);
    
    // Cleanup rank-specific temp directory
    std::filesystem::remove_all(rank_temp_dir);
    
    if (rank == 0) {
        auto end_time = std::chrono::high_resolution_clock::now();
        auto total_duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
        std::cout << "MPI MergeSort completed in " << total_duration.count() << " ms" << std::endl;
    }
}

void calculate_file_partition(
    const std::string& filename,
    int rank,
    int size,
    size_t& start_offset,
    size_t& end_offset
) {
    // Get file size
    std::ifstream file(filename, std::ios::binary | std::ios::ate);
    if (!file) {
        throw std::runtime_error("Cannot open file: " + filename);
    }
    
    size_t file_size = file.tellg();
    file.seekg(0);
    
    // Calculate approximate boundaries
    size_t chunk_size = file_size / size;
    start_offset = rank * chunk_size;
    end_offset = (rank == size - 1) ? file_size : (rank + 1) * chunk_size;
    
    // Adjust boundaries to not split records
    if (rank > 0) {
        // Move start_offset to the beginning of the next complete record
        file.seekg(start_offset);
        
        // Skip to find a complete record boundary
        while (static_cast<size_t>(file.tellg()) < file_size) {
            try {
                size_t pos = file.tellg();
                Record temp_record;
                if (temp_record.read_from_stream(file)) {
                    start_offset = pos;
                    break;
                } else {
                    file.clear(); // Clear any failbit
                    file.seekg(pos + 1);
                }
            } catch (...) {
                // If we can't read a complete record, move forward
                file.seekg(file.tellg() + std::streamoff(1));
            }
        }
    }
    
    if (rank < size - 1) {
        // Move end_offset to the beginning of the next complete record
        file.seekg(end_offset);
        
        while (static_cast<size_t>(file.tellg()) < file_size) {
            try {
                size_t pos = file.tellg();
                Record temp_record;
                if (temp_record.read_from_stream(file)) {
                    end_offset = pos;
                    break;
                } else {
                    file.clear(); // Clear any failbit
                    file.seekg(pos + 1);
                }
            } catch (...) {
                file.seekg(file.tellg() + std::streamoff(1));
            }
        }
    }
    
    file.close();
}

void read_records_from_range(
    const std::string& filename,
    size_t start_offset,
    size_t end_offset,
    std::vector<Record>& records
) {
    std::ifstream file(filename, std::ios::binary);
    if (!file) {
        throw std::runtime_error("Cannot open file: " + filename);
    }
    
    file.seekg(start_offset);
    
    while (static_cast<size_t>(file.tellg()) < end_offset) {
        try {
            Record record;
            if (record.read_from_stream(file)) {
                records.push_back(record);
            }
        } catch (...) {
            // If we can't read a complete record, we've reached the end of our partition
            break;
        }
    }
    
    file.close();
}

void distributed_merge(
    std::vector<Record>& local_records,
    const std::string& output_file,
    int rank,
    int size
) {
    // Sample Sort approach for distributed merge
    
    // Step 1: Sample local data
    std::vector<uint64_t> local_samples;
    size_t sample_size = std::min(size_t(100), local_records.size());
    
    for (size_t i = 0; i < sample_size && i * (local_records.size() / sample_size) < local_records.size(); ++i) {
        size_t idx = i * (local_records.size() / sample_size);
        local_samples.push_back(local_records[idx].key);
    }
    
    // Step 2: Gather all samples at rank 0
    std::vector<int> sample_counts(size);
    int local_sample_count = local_samples.size();
    
    MPI_Gather(&local_sample_count, 1, MPI_INT, sample_counts.data(), 1, MPI_INT, 0, MPI_COMM_WORLD);
    
    std::vector<uint64_t> all_samples;
    std::vector<int> sample_displs(size, 0);
    
    if (rank == 0) {
        int total_samples = 0;
        for (int i = 0; i < size; ++i) {
            sample_displs[i] = total_samples;
            total_samples += sample_counts[i];
        }
        all_samples.resize(total_samples);
    }
    
    MPI_Gatherv(local_samples.data(), local_sample_count, MPI_UINT64_T,
                all_samples.data(), sample_counts.data(), sample_displs.data(), 
                MPI_UINT64_T, 0, MPI_COMM_WORLD);
    
    // Step 3: Determine splitters
    std::vector<uint64_t> splitters(size - 1);
    
    if (rank == 0) {
        std::sort(all_samples.begin(), all_samples.end());
        
        for (int i = 1; i < size; ++i) {
            size_t idx = (i * all_samples.size()) / size;
            if (idx < all_samples.size()) {
                splitters[i - 1] = all_samples[idx];
            }
        }
    }
    
    // Broadcast splitters to all ranks
    MPI_Bcast(splitters.data(), size - 1, MPI_UINT64_T, 0, MPI_COMM_WORLD);
    
    // Step 4: Partition local data based on splitters
    std::vector<std::vector<Record>> partitions(size);
    
    for (const auto& record : local_records) {
        int target_rank = 0;
        for (int i = 0; i < size - 1; ++i) {
            if (record.key >= splitters[i]) {
                target_rank = i + 1;
            } else {
                break;
            }
        }
        partitions[target_rank].push_back(record);
    }
    
    // Step 5: All-to-all exchange
    std::vector<std::vector<char>> send_buffers(size);
    std::vector<int> send_counts(size);
    std::vector<int> send_displs(size, 0);
    
    // Serialize partitions
    for (int i = 0; i < size; ++i) {
        send_buffers[i] = serialize_records(partitions[i]);
        send_counts[i] = send_buffers[i].size();
    }
    
    // Calculate displacements
    for (int i = 1; i < size; ++i) {
        send_displs[i] = send_displs[i-1] + send_counts[i-1];
    }
    
    // Combine all send buffers
    std::vector<char> combined_send_buffer;
    for (const auto& buffer : send_buffers) {
        combined_send_buffer.insert(combined_send_buffer.end(), buffer.begin(), buffer.end());
    }
    
    // Exchange counts first
    std::vector<int> recv_counts(size);
    MPI_Alltoall(send_counts.data(), 1, MPI_INT, recv_counts.data(), 1, MPI_INT, MPI_COMM_WORLD);
    
    // Calculate receive displacements and total size
    std::vector<int> recv_displs(size, 0);
    int total_recv_size = recv_counts[0];
    for (int i = 1; i < size; ++i) {
        recv_displs[i] = recv_displs[i-1] + recv_counts[i-1];
        total_recv_size += recv_counts[i];
    }
    
    // Exchange actual data
    std::vector<char> recv_buffer(total_recv_size);
    MPI_Alltoallv(combined_send_buffer.data(), send_counts.data(), send_displs.data(), MPI_CHAR,
                  recv_buffer.data(), recv_counts.data(), recv_displs.data(), MPI_CHAR, MPI_COMM_WORLD);
    
    // Step 6: Deserialize received data and merge locally
    std::vector<Record> final_records;
    
    for (int i = 0; i < size; ++i) {
        if (recv_counts[i] > 0) {
            std::vector<char> partition_buffer(recv_buffer.begin() + recv_displs[i], 
                                               recv_buffer.begin() + recv_displs[i] + recv_counts[i]);
            std::vector<Record> partition_records = deserialize_records(partition_buffer);
            final_records.insert(final_records.end(), partition_records.begin(), partition_records.end());
        }
    }
    
    // Sort final records (should be mostly sorted already)
    std::sort(final_records.begin(), final_records.end());
    
    // Step 7: Write to output file (each rank writes to a separate file)
    std::string rank_output = output_file + "_rank_" + std::to_string(rank);
    std::ofstream outfile(rank_output, std::ios::binary);
    
    for (const auto& record : final_records) {
        record.write_to_stream(outfile);
    }
    outfile.close();
    
    // Step 8: Rank 0 concatenates all rank files to final output
    MPI_Barrier(MPI_COMM_WORLD);
    
    if (rank == 0) {
        std::ofstream final_output(output_file, std::ios::binary);
        
        for (int i = 0; i < size; ++i) {
            std::string rank_file = output_file + "_rank_" + std::to_string(i);
            std::ifstream rank_input(rank_file, std::ios::binary);
            
            final_output << rank_input.rdbuf();
            rank_input.close();
            
            // Clean up rank file
            std::filesystem::remove(rank_file);
        }
        
        final_output.close();
        std::cout << "Final output written to: " << output_file << std::endl;
    }
}

std::vector<char> serialize_records(const std::vector<Record>& records) {
    std::vector<char> buffer;
    
    // First, write the number of records
    size_t count = records.size();
    buffer.insert(buffer.end(), reinterpret_cast<const char*>(&count), 
                  reinterpret_cast<const char*>(&count) + sizeof(count));
    
    // Then write each record
    for (const auto& record : records) {
        // Write key
        buffer.insert(buffer.end(), reinterpret_cast<const char*>(&record.key), 
                      reinterpret_cast<const char*>(&record.key) + sizeof(record.key));
        
        // Write length
        buffer.insert(buffer.end(), reinterpret_cast<const char*>(&record.len), 
                      reinterpret_cast<const char*>(&record.len) + sizeof(record.len));
        
        // Write payload
        buffer.insert(buffer.end(), record.payload.begin(), record.payload.end());
    }
    
    return buffer;
}

std::vector<Record> deserialize_records(const std::vector<char>& buffer) {
    std::vector<Record> records;
    
    if (buffer.size() < sizeof(size_t)) {
        return records;
    }
    
    size_t pos = 0;
    
    // Read number of records
    size_t count;
    std::memcpy(&count, buffer.data() + pos, sizeof(count));
    pos += sizeof(count);
    
    records.reserve(count);
    
    // Read each record
    for (size_t i = 0; i < count && pos < buffer.size(); ++i) {
        Record record;
        
        // Read key
        if (pos + sizeof(record.key) > buffer.size()) break;
        std::memcpy(&record.key, buffer.data() + pos, sizeof(record.key));
        pos += sizeof(record.key);
        
        // Read length
        if (pos + sizeof(record.len) > buffer.size()) break;
        std::memcpy(&record.len, buffer.data() + pos, sizeof(record.len));
        pos += sizeof(record.len);
        
        // Read payload
        if (pos + record.len > buffer.size()) break;
        record.payload.resize(record.len);
        std::memcpy(record.payload.data(), buffer.data() + pos, record.len);
        pos += record.len;
        
        records.push_back(record);
    }
    
    return records;
}

void mpi_performance_test(const std::string& input_file) {
    int rank, size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    
    if (rank == 0) {
        std::cout << "=== MPI Performance Test ===" << std::endl;
    }
    
    // Test with different memory budgets
    std::vector<size_t> memory_budgets = {
        64 * 1024 * 1024,   // 64 MB
        128 * 1024 * 1024,  // 128 MB
        256 * 1024 * 1024,  // 256 MB
        512 * 1024 * 1024   // 512 MB
    };
    
    for (size_t budget : memory_budgets) {
        if (rank == 0) {
            std::cout << "\nTesting with " << (budget / (1024*1024)) << " MB memory budget..." << std::endl;
        }
        
        std::string output_file = "test_mpi_output_" + std::to_string(budget/(1024*1024)) + "mb.bin";
        
        auto start = std::chrono::high_resolution_clock::now();
        mpi_sort_file(input_file, output_file, budget, "temp_mpi");
        auto end = std::chrono::high_resolution_clock::now();
        
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
        
        if (rank == 0) {
            std::cout << "Time: " << duration.count() << " ms" << std::endl;
        }
        
        MPI_Barrier(MPI_COMM_WORLD);
    }
}