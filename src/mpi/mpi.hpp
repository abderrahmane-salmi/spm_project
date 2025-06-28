#ifndef MPI_HPP
#define MPI_HPP

#include <string>
#include <vector>
#include <mpi.h>

#include "../include/record.hpp"

/**
 * MPI + OpenMP Hybrid Distributed MergeSort
 * 
 * Strategy:
 * 1. Each MPI rank processes a portion of the input file
 * 2. Local sorting using OpenMP within each rank
 * 3. Global merge phase using MPI communication
 */

/**
 * Main MPI sorting function
 * @param input_file Path to input file
 * @param output_file Path to output file  
 * @param memory_budget Memory budget per rank in bytes
 * @param temp_dir Directory for temporary files
 */
void mpi_sort_file(
    const std::string& input_file,
    const std::string& output_file,
    size_t memory_budget,
    const std::string& temp_dir = "temp"
);

/**
 * Performance test for MPI version
 * @param input_file Path to input file
 */
void mpi_performance_test(const std::string& input_file);

/**
 * Calculate file partition boundaries for each rank
 * Ensures we don't split records in the middle
 * @param filename Input file name
 * @param rank Current MPI rank
 * @param size Total number of MPI processes
 * @param start_offset Output: starting byte offset for this rank
 * @param end_offset Output: ending byte offset for this rank
 */
void calculate_file_partition(
    const std::string& filename,
    int rank,
    int size,
    size_t& start_offset,
    size_t& end_offset
);

/**
 * Read records from a specific file range
 * @param filename Input file name
 * @param start_offset Starting byte offset
 * @param end_offset Ending byte offset
 * @param records Output vector of records
 */
void read_records_from_range(
    const std::string& filename,
    size_t start_offset,
    size_t end_offset,
    std::vector<Record>& records
);

/**
 * Distributed merge using sample sort approach
 * @param local_records Sorted records from this rank
 * @param output_file Final output file
 * @param rank Current MPI rank
 * @param size Total number of MPI processes
 */
void distributed_merge(
    const std::string& local_sorted_file,
    const std::vector<uint64_t>& local_samples, 
    const std::string& output_file,
    int rank,
    int size,
    size_t memory_budget
);

/**
 * Helper function to serialize/deserialize records for MPI communication
 */
std::vector<char> serialize_records(const std::vector<Record>& records);
std::vector<Record> deserialize_records(const std::vector<char>& buffer);

#endif // MPI_HPP