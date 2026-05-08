#include "chunk_manager.hpp"

void ChunkManager::store(const std::string& req_id, std::vector<mini2::ServiceRecord> records) {
    std::lock_guard<std::mutex> lk(mu_);
    store_[req_id] = {std::move(records), false};
}

bool ChunkManager::fetch(const std::string& req_id, int chunk_idx,
                          std::vector<mini2::ServiceRecord>& out, bool& is_last) const {
    std::lock_guard<std::mutex> lk(mu_);
    auto it = store_.find(req_id);
    if (it == store_.end() || it->second.cancelled) return false;

    const auto& recs = it->second.records;
    int start = chunk_idx * chunk_size_;
    int total = static_cast<int>(recs.size());
    if (start > total) { is_last = true; return true; }

    int end = std::min(total, start + chunk_size_);
    out.assign(recs.begin() + start, recs.begin() + end);
    is_last = (end >= total);
    return true;
}

void ChunkManager::cancel(const std::string& req_id) {
    std::lock_guard<std::mutex> lk(mu_);
    auto it = store_.find(req_id);
    if (it != store_.end()) it->second.cancelled = true;
}

bool ChunkManager::exists(const std::string& req_id) const {
    std::lock_guard<std::mutex> lk(mu_);
    return store_.count(req_id) > 0;
}