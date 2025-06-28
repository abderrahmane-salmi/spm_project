#include "mpi.hpp"
#include <iostream>
#include <fstream>
#include <algorithm>
#include <chrono>
#include <filesystem>
#include <cstring>

#include "../include/record.hpp"
// #include "../chunking/chunking.hpp"
#include "../chunking/adaptive_chunker.cpp"
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
    
    // Step 1: Use adaptive chunker to divide file based on memory budget and MPI size
    ChunkingConfig config;
    config.available_memory_bytes = memory_budget; // your size_t memory budget
    AdaptiveChunker chunker(config);
    std::vector<ChunkInfo> chunks = chunker.analyze_file_adaptive(input_file);

    // Sanity check to ensure chunks are for each rank
    if (chunks.size() != static_cast<size_t>(size)) {
        std::cerr << "Error: AdaptiveChunker returned incorrect number of chunks" << std::endl;
        MPI_Abort(MPI_COMM_WORLD, 1);
    }

    // Step 2: Extract this rank’s assigned partition info from chunks
    ChunkInfo my_chunk = chunks[rank];
    std::string local_partition_file = rank_temp_dir + "/partition.bin";
    extract_partition_to_file(input_file, my_chunk.offset_bytes, my_chunk.offset_bytes + my_chunk.length_bytes, local_partition_file);
    
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
    
    if (!file.good()) {
        std::cerr << "Warning: Cannot open sorted file " << sorted_file << " for sampling" << std::endl;
        return samples;
    }
    
    // Get file size
    file.seekg(0, std::ios::end);
    size_t file_size = file.tellg();
    file.seekg(0);
    
    if (file_size == 0) return samples;
    
    // Limit number of samples to prevent excessive memory usage
    num_samples = std::min(num_samples, file_size / sizeof(Record));
    
    // Sample at regular intervals
    for (size_t i = 0; i < num_samples; ++i) {
        size_t target_pos = (i * file_size) / num_samples;
        file.seekg(target_pos);
        
        // Find next complete record
        Record record;
        size_t attempts = 0;
        while (file.tellg() < file_size && attempts < 1000) {
            size_t pos = file.tellg();
            try {
                if (record.read_from_stream(file)) {
                    samples.push_back(record.key);
                    break;
                }
            } catch (...) {
                // Continue searching
            }
            file.clear();
            file.seekg(pos + 1);
            attempts++;
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

    // 3. Stream local_sorted_file in smaller chunks to avoid memory issues
    std::vector<std::vector<char>> send_bufs(size);
    std::vector<int> send_counts(size, 0);

    std::ifstream in(local_sorted_file, std::ios::binary);
    if (!in.good()) {
        std::cerr << "Rank " << rank << " error: cannot open " << local_sorted_file << std::endl;
        MPI_Abort(MPI_COMM_WORLD, 1);
        return;
    }
    
    in.seekg(0, std::ios::end);
    size_t fsize = in.tellg();
    in.seekg(0);

    // Use smaller chunks to avoid memory pressure
    const size_t CHUNK = std::min(memory_budget / 8, size_t(1024 * 1024)); // Max 1MB chunks
    size_t processed = 0;

    while (processed < fsize && in.good()) {
        size_t to_read = std::min(CHUNK, fsize - processed);
        std::vector<char> chunk_buf(to_read);
        in.read(chunk_buf.data(), to_read);
        size_t got = in.gcount();
        if (got == 0) break;
        processed += got;

        // Process records one by one instead of batch deserialization
        size_t chunk_pos = 0;
        while (chunk_pos < got) {
            Record rec;
            if (chunk_pos + sizeof(rec.key) > got) break;
            std::memcpy(&rec.key, chunk_buf.data() + chunk_pos, sizeof(rec.key));
            chunk_pos += sizeof(rec.key);
            
            if (chunk_pos + sizeof(rec.len) > got) break;
            std::memcpy(&rec.len, chunk_buf.data() + chunk_pos, sizeof(rec.len));
            chunk_pos += sizeof(rec.len);
            
            if (rec.len > 1024 * 1024 || chunk_pos + rec.len > got) break;
            rec.payload.resize(rec.len);
            std::memcpy(rec.payload.data(), chunk_buf.data() + chunk_pos, rec.len);
            chunk_pos += rec.len;
            
            // Determine destination
            int dst = size - 1;
            for (int d = 0; d < size - 1; ++d) {
                if (rec.key <= splitters[d]) { dst = d; break; }
            }
            
            // Serialize individual record
            std::vector<char> rec_data;
            rec_data.insert(rec_data.end(), reinterpret_cast<const char*>(&rec.key), 
                           reinterpret_cast<const char*>(&rec.key) + sizeof(rec.key));
            rec_data.insert(rec_data.end(), reinterpret_cast<const char*>(&rec.len), 
                           reinterpret_cast<const char*>(&rec.len) + sizeof(rec.len));
            rec_data.insert(rec_data.end(), rec.payload.begin(), rec.payload.end());
            
            send_bufs[dst].insert(send_bufs[dst].end(), rec_data.begin(), rec_data.end());
            send_counts[dst] = send_bufs[dst].size();
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
    send_all.reserve(total_send);
    for (auto& v : send_bufs) {
        send_all.insert(send_all.end(), v.begin(), v.end());
        v.clear(); // Free memory immediately
    }

    std::vector<char> recv_all(total_recv);
    MPI_Alltoallv(send_all.data(), send_counts.data(), send_displs.data(), MPI_CHAR,
                  recv_all.data(), recv_counts.data(), recv_displs.data(), MPI_CHAR, MPI_COMM_WORLD);

    // 6. Process received data record by record to avoid large vector allocations
    std::string rank_out = output_file + "_rank_" + std::to_string(rank);
    std::ofstream out(rank_out, std::ios::binary);
    
    size_t pos = 0;
    while (pos < recv_all.size()) {
        Record rec;
        
        if (pos + sizeof(rec.key) > recv_all.size()) break;
        std::memcpy(&rec.key, recv_all.data() + pos, sizeof(rec.key));
        pos += sizeof(rec.key);
        
        if (pos + sizeof(rec.len) > recv_all.size()) break;
        std::memcpy(&rec.len, recv_all.data() + pos, sizeof(rec.len));
        pos += sizeof(rec.len);
        
        if (rec.len > 1024 * 1024 || pos + rec.len > recv_all.size()) break;
        rec.payload.resize(rec.len);
        std::memcpy(rec.payload.data(), recv_all.data() + pos, rec.len);
        pos += rec.len;
        
        rec.write_to_stream(out);
    }
    out.close();

    MPI_Barrier(MPI_COMM_WORLD);

    // 7. Rank 0 concatenates rank outputs into final output
    if (rank == 0) {
        std::ofstream final_out(output_file, std::ios::binary);
        for (int i = 0; i < size; i++) {
            std::string rank_file = output_file + "_rank_" + std::to_string(i);
            std::ifstream in_r(rank_file, std::ios::binary);
            if (in_r.good()) {
                final_out << in_r.rdbuf();
            }
            in_r.close();
            std::filesystem::remove(rank_file);
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

    // FIX: Add validation for count to prevent excessive memory allocation
    if (count == 0 || count > 1000000) {  // Reasonable upper limit
        // If count is suspicious, try to parse records without the count header
        pos = 0;  // Reset position
        while (pos < buffer.size()) {
            Record record;
            
            if (pos + sizeof(record.key) > buffer.size()) break;
            std::memcpy(&record.key, buffer.data() + pos, sizeof(record.key));
            pos += sizeof(record.key);

            if (pos + sizeof(record.len) > buffer.size()) break;
            std::memcpy(&record.len, buffer.data() + pos, sizeof(record.len));
            pos += sizeof(record.len);

            // Validate record length
            if (record.len > 1024 * 1024 || pos + record.len > buffer.size()) break;
            
            record.payload.resize(record.len);
            std::memcpy(record.payload.data(), buffer.data() + pos, record.len);
            pos += record.len;

            records.push_back(record);
        }
        return {records, pos};
    }

    // Use a more conservative reservation strategy
    size_t safe_reserve = std::min(count, buffer.size() / 16);  // Conservative estimate
    records.reserve(safe_reserve);
    
    for (size_t i = 0; i < count && pos < buffer.size(); ++i) {
        Record record;
        
        if (pos + sizeof(record.key) > buffer.size()) break;
        std::memcpy(&record.key, buffer.data() + pos, sizeof(record.key));
        pos += sizeof(record.key);

        if (pos + sizeof(record.len) > buffer.size()) break;
        std::memcpy(&record.len, buffer.data() + pos, sizeof(record.len));
        pos += sizeof(record.len);

        // Validate record length
        if (record.len > 1024 * 1024 || pos + record.len > buffer.size()) break;
        
        record.payload.resize(record.len);
        std::memcpy(record.payload.data(), buffer.data() + pos, record.len);
        pos += record.len;

        records.push_back(record);
    }

    return {records, pos};
}