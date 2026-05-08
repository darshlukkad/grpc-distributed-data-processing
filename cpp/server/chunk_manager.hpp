#pragma once
#include <vector>
#include <string>
#include <unordered_map>
#include <mutex>
#include <algorithm>
#include "mini2.pb.h"

class ChunkManager {
public:
    explicit ChunkManager(int chunk_size = 500) : chunk_size_(chunk_size) {}

    void store(const std::string& req_id, std::vector<mini2::ServiceRecord> records);
    bool fetch(const std::string& req_id, int chunk_idx,
               std::vector<mini2::ServiceRecord>& out, bool& is_last) const;
    void cancel(const std::string& req_id);
    bool exists(const std::string& req_id) const;

private:
    struct Entry {
        std::vector<mini2::ServiceRecord> records;
        bool cancelled = false;
    };

    int chunk_size_;
    mutable std::mutex mu_;
    std::unordered_map<std::string, Entry> store_;
};