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
    int rank, size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    
    // Create rank-specific temp directory
    std::string rank_temp_dir = temp_dir + "/rank_" + std::to_string(rank);
    std::filesystem::create_directories(rank_temp_dir);
    
    // Step 1: Calculate file partition
    size_t start_offset, end_offset;
    calculate_file_partition(input_file, rank, size, start_offset, end_offset);
    
    // Step 2: Create temporary file with just this rank's data
    std::string local_partition_file = rank_temp_dir + "/partition.bin";
    extract_partition_to_file(input_file, start_offset, end_offset, local_partition_file);
    
    // Step 3: Sort partition using OpenMP external sort (stays on disk)
    std::string local_sorted_file = rank_temp_dir + "/sorted.bin";
    OpenMPExternalMergeSort omp_sorter(memory_budget, 0, rank_temp_dir);
    omp_sorter.sort_file(local_partition_file, local_sorted_file);
    
    // Step 4: Sample the sorted file for distributed merge
    std::vector<uint64_t> samples = sample_sorted_file(local_sorted_file, 100);
    
    // Step 5: Distributed merge using streaming approach
    distributed_merge(local_sorted_file, samples, output_file, rank, size, memory_budget);
    
    // Cleanup
    std::filesystem::remove_all(rank_temp_dir);
}

// Extract partition without loading into memory
void extract_partition_to_file(
    const std::string& input_file,
    size_t start_offset,
    size_t end_offset,
    const std::string& output_file
) {
    std::ifstream input(input_file, std::ios::binary);
    std::ofstream output(output_file, std::ios::binary);
    
    input.seekg(start_offset);
    
    // Copy data in chunks to avoid loading everything
    const size_t BUFFER_SIZE = 64 * 1024; // 64KB buffer
    std::vector<char> buffer(BUFFER_SIZE);
    
    size_t remaining = end_offset - start_offset;
    
    while (remaining > 0 && input.good()) {
        size_t to_read = std::min(remaining, BUFFER_SIZE);
        input.read(buffer.data(), to_read);
        size_t actually_read = input.gcount();
        
        output.write(buffer.data(), actually_read);
        remaining -= actually_read;
        
        if (actually_read < to_read) break;
    }
}

// Sample a sorted file without loading all records
std::vector<uint64_t> sample_sorted_file(const std::string& sorted_file, size_t num_samples) {
    std::vector<uint64_t> samples;
    std::ifstream file(sorted_file, std::ios::binary);
    
    // Get file size
    file.seekg(0, std::ios::end);
    size_t file_size = file.tellg();
    file.seekg(0);
    
    // Sample at regular intervals
    for (size_t i = 0; i < num_samples; ++i) {
        size_t target_pos = (i * file_size) / num_samples;
        file.seekg(target_pos);
        
        // Find next complete record
        Record record;
        while (file.tellg() < file_size) {
            size_t pos = file.tellg();
            if (record.read_from_stream(file)) {
                samples.push_back(record.key);
                break;
            } else {
                file.clear();
                file.seekg(pos + 1);
            }
        }
    }
    
    return samples;
}

/**
 * Calculate file partition boundaries for each MPI rank.
 * 
 * This function determines the starting and ending byte offsets for a 
 * given rank's partition of a file. It ensures that partitions do not 
 * split records in the middle by adjusting the offsets to align with 
 * complete records. The function handles edge cases for the first and 
 * last rank to cover the entire file without overlap.
 * 
 * @param filename The name of the file to partition.
 * @param rank The current rank in the MPI communicator.
 * @param size The total number of MPI processes.
 * @param start_offset Output parameter: The starting byte offset for 
 *                     this rank's partition.
 * @param end_offset Output parameter: The ending byte offset for this 
 *                   rank's partition.
 * 
 * @throws std::runtime_error if the file cannot be opened.
 */
void calculate_file_partition(
    const std::string& filename,
    int rank,
    int size,
    size_t& start_offset,
    size_t& end_offset
) {
    // Open the file in binary mode and get its size
    std::ifstream file(filename, std::ios::binary | std::ios::ate);
    if (!file) {
        throw std::runtime_error("Cannot open file: " + filename);
    }
    
    size_t file_size = file.tellg();
    file.seekg(0); // Reset the file position to the beginning
    
    // Calculate approximate boundaries
    size_t chunk_size = file_size / size; // Approximate chunk size
    start_offset = rank * chunk_size; // Starting offset for this rank
    end_offset = (rank == size - 1) ? file_size : (rank + 1) * chunk_size; // Ending offset for this rank
    
    // Adjust boundaries to not split records
    if (rank > 0) {
        // Move start_offset to the beginning of the next complete record
        file.seekg(start_offset);
        
        // Skip to find a complete record boundary
        while (static_cast<size_t>(file.tellg()) < file_size) {
            try {
                size_t pos = file.tellg(); // Current file position
                Record temp_record;
                if (temp_record.read_from_stream(file)) {
                    // If a complete record is read, update the start offset
                    start_offset = pos;
                    break;
                } else {
                    // If not, clear any error flags and move forward
                    file.clear();
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
                size_t pos = file.tellg(); // Current file position
                Record temp_record;
                if (temp_record.read_from_stream(file)) {
                    // If a complete record is read, update the end offset
                    end_offset = file.tellg();
                    break;
                } else {
                    // If not, clear any error flags and move forward
                    file.clear();
                    file.seekg(pos + 1);
                }
            } catch (...) {
                file.seekg(file.tellg() + std::streamoff(1));
            }
        }
    }
    
    file.close();
}

/**
 * Reads records from a specified range in a binary file.
 *
 * This function opens a binary file and reads all complete records
 * within the given byte range, appending them to the provided vector.
 * It starts reading at the specified start offset and continues until
 * reaching the end offset, ensuring that only complete records are
 * included in the output vector.
 *
 * @param filename The name of the binary file to read from.
 * @param start_offset The starting byte offset for reading.
 * @param end_offset The ending byte offset for reading.
 * @param records Output vector to store the read records.
 *
 * @throws std::runtime_error If the file cannot be opened.
 */
void read_records_from_range(
    const std::string& filename,
    size_t start_offset,
    size_t end_offset,
    std::vector<Record>& records
) {
    // Open the file and seek to the starting byte offset
    std::ifstream file(filename, std::ios::binary);
    if (!file) {
        throw std::runtime_error("Cannot open file: " + filename);
    }
    
    file.seekg(start_offset);
    
    // Continue reading records until we reach the end offset
    while (static_cast<size_t>(file.tellg()) < end_offset) {
        try {
            // Attempt to read a record from the file
            Record record;
            if (record.read_from_stream(file)) {
                // If the read was successful, add the record to the output vector
                records.push_back(record);
            }
        } catch (...) {
            // If we can't read a complete record, we've reached the end of our partition
            break;
        }
    }
    
    file.close();
}

/**
 * Distributed merge phase using Sample Sort approach
 * 
 * This function takes a vector of local records sorted by key and
 * performs the following steps:
 * 1. Sample local data
 * 2. Gather all samples at rank 0
 * 3. Determine splitters
 * 4. Partition local data based on splitters
 * 5. All-to-all exchange
 * 6. Deserialize received data and merge locally
 * 7. Write to output file (each rank writes to a separate file)
 * 8. Rank 0 concatenates all rank files to final output
 * 
 * @param local_records Vector of local records sorted by key
 * @param output_file Path to final output file
 * @param rank Current MPI rank
 * @param size Total number of MPI processes
 */
/**
 * Streaming, out-of-core Sample-Sort merge.
 * Each MPI rank:
 *   - Streams own sorted run file in chunks
 *   - Broadcasts splitters
 *   - Routes records to target processes via MPI_Alltoallv
 *   - Each rank writes final output incrementally
 */
void distributed_merge(
    const std::string& local_sorted_file,
    const std::vector<uint64_t>& local_samples, 
    const std::string& output_file,
    int rank,
    int size,
    size_t memory_budget
) {
    // 1. Gather samples at root
    std::vector<int> sample_counts(size);
    int my_scount = local_samples.size();
    MPI_Gather(&my_scount, 1, MPI_INT, sample_counts.data(), 1, MPI_INT, 0, MPI_COMM_WORLD);

    std::vector<uint64_t> all_samples;
    std::vector<int> displs(size, 0);
    if (rank == 0) {
        int total = 0;
        for (int i = 0; i < size; i++) {
            displs[i] = total;
            total += sample_counts[i];
        }
        all_samples.resize(total);
    }
    MPI_Gatherv(local_samples.data(), my_scount, MPI_UINT64_T,
                all_samples.data(), sample_counts.data(), displs.data(),
                MPI_UINT64_T, 0, MPI_COMM_WORLD);

    // 2. Select splitters and broadcast
    std::vector<uint64_t> splitters(size - 1);
    if (rank == 0) {
        std::sort(all_samples.begin(), all_samples.end());
        all_samples.erase(std::unique(all_samples.begin(), all_samples.end()), all_samples.end());

        for (int i = 1; i < size; ++i) {
            size_t idx = (i * all_samples.size()) / size;
            idx = std::min(idx, all_samples.size() - 1);
            splitters[i - 1] = all_samples[idx];
        }
    }
    MPI_Bcast(splitters.data(), size - 1, MPI_UINT64_T, 0, MPI_COMM_WORLD);

    // 3. Stream local_sorted_file chunk by chunk,
    //    partitioning records into size streams
    std::vector<std::vector<char>> send_bufs(size);
    std::vector<int> send_counts(size, 0);

    std::ifstream in(local_sorted_file, std::ios::binary);
    in.seekg(0, std::ios::end);
    size_t fsize = in.tellg();
    in.seekg(0);

    const size_t CHUNK = memory_budget / 4; // e.g. streaming buffer per chunk
    size_t processed = 0;

    while (processed < fsize && in.good()) {
        size_t to_read = std::min(CHUNK, fsize - processed);
        std::vector<char> chunk_buf(to_read);
        in.read(chunk_buf.data(), to_read);
        size_t got = in.gcount();
        processed += got;

        auto [recs, used_bytes] = deserialize_records_with_offset(chunk_buf);
        for (auto& rec : recs) {
            int dst = size - 1;
            for (int d = 0; d < size - 1; ++d) {
                if (rec.key <= splitters[d]) { dst = d; break; }
            }
            std::vector<char>& dst_buf = send_bufs[dst];
            size_t prev = dst_buf.size();
            auto tmp = serialize_records({rec}); // individual record
            dst_buf.insert(dst_buf.end(), tmp.begin(), tmp.end());
            send_counts[dst] = dst_buf.size();
        }
    }
    in.close();

    // 4. Exchange counts
    std::vector<int> recv_counts(size);
    MPI_Alltoall(send_counts.data(), 1, MPI_INT, recv_counts.data(), 1, MPI_INT, MPI_COMM_WORLD);

    // 5. Prepare displacements and total recv
    std::vector<int> send_displs(size, 0), recv_displs(size, 0);
    int total_send = 0, total_recv = 0;
    for (int i = 0; i < size; ++i) {
        send_displs[i] = total_send;
        total_send += send_counts[i];
        recv_displs[i] = total_recv;
        total_recv += recv_counts[i];
    }

    std::vector<char> send_all;
    for (auto& v : send_bufs) send_all.insert(send_all.end(), v.begin(), v.end());

    std::vector<char> recv_all(total_recv);
    MPI_Alltoallv(send_all.data(), send_counts.data(), send_displs.data(), MPI_CHAR,
                  recv_all.data(), recv_counts.data(), recv_displs.data(), MPI_CHAR, MPI_COMM_WORLD);

    // 6. Stream-deserialize received buffer and write sorted output file
    std::string rank_out = output_file + "_rank_" + std::to_string(rank);
    std::ofstream out(rank_out, std::ios::binary);
    size_t pos = 0;
    while (pos < recv_all.size()) {
        std::vector<char> subbuf(recv_all.begin() + pos, recv_all.end());
        auto [recs, consumed] = deserialize_records_with_offset(subbuf);

        if (consumed == 0) {
            std::cerr << "Rank " << rank << " warning: zero bytes consumed at pos=" << pos << "\n";
            break;  // Prevent infinite loop
        }

        for (auto& rec : recs)
            rec.write_to_stream(out);

        pos += consumed;
    }

    MPI_Barrier(MPI_COMM_WORLD);

    // 7. Rank 0 concatenates rank outputs into final output
    if (rank == 0) {
        std::ofstream final_out(output_file, std::ios::binary);
        for (int i = 0; i < size; i++) {
            std::ifstream in_r(output_file + "_rank_" + std::to_string(i), std::ios::binary);
            final_out << in_r.rdbuf();
            in_r.close();
            std::filesystem::remove(output_file + "_rank_" + std::to_string(i));
        }
        final_out.close();
    }
}


/**
 * Serialize a vector of Records into a contiguous block of memory.
 * 
 * The memory layout is as follows:
 * - First, a size_t indicating the number of records
 * - Then, each record is serialized in order:
 *   - uint64_t key
 *   - uint32_t len
 *   - char[len] payload
 */
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

/**
 * Deserialize a contiguous block of memory containing Records.
 * 
 * The memory layout is as follows:
 * - First, a size_t indicating the number of records
 * - Then, each record is deserialized in order:
 *   - uint64_t key
 *   - uint32_t len
 *   - char[len] payload
 * 
 * If the buffer is incomplete, an empty vector of Records is returned.
 */
std::pair<std::vector<Record>, size_t> deserialize_records_with_offset(const std::vector<char>& buffer) {
    std::vector<Record> records;
    size_t pos = 0;
    
    if (buffer.size() < sizeof(size_t)) return {records, pos};
    
    size_t count;
    std::memcpy(&count, buffer.data(), sizeof(count));
    pos += sizeof(count);

    records.reserve(count);
    
    for (size_t i = 0; i < count && pos < buffer.size(); ++i) {
        Record record;
        
        if (pos + sizeof(record.key) > buffer.size()) break;
        std::memcpy(&record.key, buffer.data() + pos, sizeof(record.key));
        pos += sizeof(record.key);

        if (pos + sizeof(record.len) > buffer.size()) break;
        std::memcpy(&record.len, buffer.data() + pos, sizeof(record.len));
        pos += sizeof(record.len);

        if (pos + record.len > buffer.size()) break;
        record.payload.resize(record.len);
        std::memcpy(record.payload.data(), buffer.data() + pos, record.len);
        pos += record.len;

        records.push_back(record);
    }

    return {records, pos};
}
