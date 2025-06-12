#pragma once
#include "record.hpp"
#include <vector>
#include <algorithm>  // for std::sort

// Custom comparator to sort Records by key
bool compareByKey(const Record& a, const Record& b) {
    return a.key < b.key;
}

// Sort a vector of records using std::sort and the custom comparator
void sort_records_by_key(std::vector<Record>& records) {
    std::sort(records.begin(), records.end(), compareByKey);
}
