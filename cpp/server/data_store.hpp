#pragma once
#include <vector>
#include <string>
#include <string_view>
#include <unordered_map>
#include <shared_mutex>
#include <atomic>
#include <memory>
#include <cstdint>
#include "mini2.pb.h"

// ── StringRef ─────────────────────────────────────────────────────────────────
struct StringRef { uint32_t offset; uint16_t length; };

// ── StringPool ────────────────────────────────────────────────────────────────
class StringPool {
public:
    explicit StringPool(size_t capacity);
    StringRef        store(const char* data, uint16_t len);
    std::string_view get(StringRef r) const { return {buf_.get() + r.offset, r.length}; }
private:
    std::unique_ptr<char[]> buf_;
    size_t                  capacity_;
    std::atomic<size_t>     top_{0};
};

// ── StringRegistry ────────────────────────────────────────────────────────────
template<typename T>
class StringRegistry {
public:
    T                encode(std::string_view sv);
    std::string_view decode(T code) const;
private:
    mutable std::shared_mutex         mu_;
    std::unordered_map<std::string,T> map_;
    std::vector<std::string>          names_;
};

// ── LatLon (together for cache-efficient bbox search) ─────────────────────────
struct LatLon { float lat; float lon; };

// ── DataStore ─────────────────────────────────────────────────────────────────
class DataStore {
public:
    void load(const std::string& csv_path, uint64_t row_start, uint64_t row_end,
              int num_threads = 4);

    std::vector<mini2::ServiceRecord> searchByZip (uint32_t zip_min,  uint32_t zip_max)  const;
    std::vector<mini2::ServiceRecord> searchByDate(uint32_t date_min, uint32_t date_max) const;
    std::vector<mini2::ServiceRecord> searchByBBox(double lat_min, double lat_max,
                                                   double lon_min, double lon_max)       const;
    size_t size() const { return unique_key_.size(); }

private:
    // ── Pools ─────────────────────────────────────────────────────────────
    std::unique_ptr<StringPool> main_pool_;
    std::unique_ptr<StringPool> res_pool_;

    // ── Registries (low/medium cardinality) ───────────────────────────────
    StringRegistry<uint8_t>  reg_agency_;
    StringRegistry<uint8_t>  reg_loc_type_;
    StringRegistry<uint8_t>  reg_addr_type_;
    StringRegistry<uint8_t>  reg_facility_;
    StringRegistry<uint8_t>  reg_status_;
    StringRegistry<uint8_t>  reg_channel_;
    StringRegistry<uint8_t>  reg_borough_;
    StringRegistry<uint8_t>  reg_park_borough_;
    StringRegistry<uint16_t> reg_problem_;
    StringRegistry<uint16_t> reg_problem_detail_;

    // ── SoA columns ───────────────────────────────────────────────────────
    std::vector<uint64_t>  unique_key_;
    std::vector<uint64_t>  bbl_;
    std::vector<uint32_t>  created_date_;
    std::vector<uint32_t>  closed_date_;
    std::vector<uint32_t>  due_date_;
    std::vector<uint32_t>  res_updated_date_;
    std::vector<uint32_t>  incident_zip_;
    std::vector<uint16_t>  community_board_;
    std::vector<uint16_t>  council_district_;
    std::vector<uint16_t>  police_precinct_;
    std::vector<int32_t>   x_coord_;
    std::vector<int32_t>   y_coord_;

    std::vector<uint8_t>   agency_code_;
    std::vector<uint8_t>   loc_type_code_;
    std::vector<uint8_t>   addr_type_code_;
    std::vector<uint8_t>   facility_code_;
    std::vector<uint8_t>   status_code_;
    std::vector<uint8_t>   channel_code_;
    std::vector<uint8_t>   borough_code_;
    std::vector<uint8_t>   park_borough_code_;
    std::vector<uint16_t>  problem_code_;
    std::vector<uint16_t>  problem_detail_code_;

    std::vector<StringRef> agency_name_;
    std::vector<StringRef> additional_details_;
    std::vector<StringRef> incident_address_;
    std::vector<StringRef> street_name_;
    std::vector<StringRef> cross_street_1_;
    std::vector<StringRef> cross_street_2_;
    std::vector<StringRef> intersect_street_1_;
    std::vector<StringRef> intersect_street_2_;
    std::vector<StringRef> city_;
    std::vector<StringRef> landmark_;
    std::vector<StringRef> resolution_desc_;
    std::vector<StringRef> park_facility_;
    std::vector<StringRef> vehicle_type_;
    std::vector<StringRef> taxi_company_borough_;
    std::vector<StringRef> taxi_pickup_;
    std::vector<StringRef> bridge_hwy_name_;
    std::vector<StringRef> bridge_hwy_dir_;
    std::vector<StringRef> road_ramp_;
    std::vector<StringRef> bridge_hwy_seg_;
    std::vector<StringRef> location_;

    std::vector<LatLon>    latlon_;

    // ── Helpers ───────────────────────────────────────────────────────────
    mini2::ServiceRecord toProto(size_t i) const;
};