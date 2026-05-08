#include "data_store.hpp"
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <omp.h>
#include <cstring>
#include <cstdlib>
#include <ctime>
#include <stdexcept>
#include <iostream>
#include <charconv>
#include <algorithm>

namespace col {
    static constexpr int UNIQUE_KEY   = 0;
    static constexpr int CREATED_DATE = 1;
    static constexpr int CLOSED_DATE  = 2;
    static constexpr int AGENCY       = 3;
    static constexpr int AGENCY_NAME  = 4;
    static constexpr int COMPLAINT    = 5;
    static constexpr int DESCRIPTOR   = 6;
    static constexpr int LOC_TYPE     = 7;
    static constexpr int ADDL_DETAILS = 8;
    static constexpr int ZIP          = 9;
    static constexpr int INC_ADDR     = 10;
    static constexpr int STREET       = 11;
    static constexpr int CROSS1       = 12;
    static constexpr int CROSS2       = 13;
    static constexpr int INTERSECT1   = 14;
    static constexpr int INTERSECT2   = 15;
    static constexpr int ADDR_TYPE    = 16;
    static constexpr int CITY         = 17;
    static constexpr int LANDMARK     = 18;
    static constexpr int FACILITY     = 19;
    static constexpr int STATUS       = 20;
    static constexpr int DUE_DATE     = 21;
    static constexpr int RES_DESC     = 22;
    static constexpr int RES_UPDATED  = 23;
    static constexpr int COMM_BOARD   = 24;
    static constexpr int BBL          = 25;
    static constexpr int BOROUGH      = 26;
    static constexpr int X_COORD      = 27;
    static constexpr int Y_COORD      = 28;
    static constexpr int CHANNEL      = 29;
    static constexpr int PARK_FAC     = 30;
    static constexpr int PARK_BORO    = 31;
    static constexpr int VEHICLE      = 32;
    static constexpr int TAXI_BORO    = 33;
    static constexpr int TAXI_PICKUP  = 34;
    static constexpr int BRIDGE_NAME  = 35;
    static constexpr int BRIDGE_DIR   = 36;
    static constexpr int ROAD_RAMP    = 37;
    static constexpr int BRIDGE_SEG   = 38;
    static constexpr int LATITUDE     = 41;
    static constexpr int LONGITUDE    = 42;
    static constexpr int LOCATION     = 43;
    static constexpr int MAX          = 44;
}

// ── StringPool ─────────────────────────────────────────────────────────────────
StringPool::StringPool(size_t capacity)
    : buf_(std::make_unique<char[]>(capacity)), capacity_(capacity) {}

StringRef StringPool::store(const char* data, uint16_t len) {
    if (!len) return {0, 0};
    size_t off = top_.fetch_add(len, std::memory_order_relaxed);
    if (off + len > capacity_) return {0, 0};
    std::memcpy(buf_.get() + off, data, len);
    return {static_cast<uint32_t>(off), len};
}

// ── StringRegistry ─────────────────────────────────────────────────────────────
template<typename T>
T StringRegistry<T>::encode(std::string_view sv) {
    std::string key(sv);
    {
        std::shared_lock lk(mu_);
        auto it = map_.find(key);
        if (it != map_.end()) return it->second;
    }
    std::unique_lock lk(mu_);
    auto [it, ins] = map_.emplace(key, static_cast<T>(names_.size()));
    if (ins) names_.push_back(it->first);
    return it->second;
}

template<typename T>
std::string_view StringRegistry<T>::decode(T code) const {
    std::shared_lock lk(mu_);
    return size_t(code) < names_.size() ? std::string_view(names_[code]) : std::string_view{};
}

template class StringRegistry<uint8_t>;
template class StringRegistry<uint16_t>;

// ── Parsing helpers ────────────────────────────────────────────────────────────
static uint32_t parseDate(const char* s, size_t len) {
    if (!s || !len) return 0;
    char buf[32];
    size_t n = std::min(len, size_t(31));
    std::memcpy(buf, s, n); buf[n] = '\0';
    int mo, dy, yr, hr, mi, sc; char ap = 0;
    if (sscanf(buf, "%d/%d/%d %d:%d:%d %c", &mo, &dy, &yr, &hr, &mi, &sc, &ap) < 6) return 0;
    if (ap == 'P' && hr != 12) hr += 12;
    else if (ap == 'A' && hr == 12) hr = 0;
    struct tm t{};
    t.tm_year = yr - 1900; t.tm_mon = mo - 1; t.tm_mday = dy;
    t.tm_hour = hr; t.tm_min = mi; t.tm_sec = sc; t.tm_isdst = -1;
    time_t ts = mktime(&t);
    return ts < 0 ? 0 : static_cast<uint32_t>(ts);
}

template<typename T>
static T parseInt(const char* s, size_t len) {
    if (!s || !len) return T{};
    T v{}; std::from_chars(s, s + len, v); return v;
}

static float parseFloat(const char* s, size_t len) {
    if (!s || !len) return 0.0f;
    char buf[32];
    size_t n = std::min(len, size_t(31));
    std::memcpy(buf, s, n); buf[n] = '\0';
    return std::strtof(buf, nullptr);
}

// ── CSV splitter ───────────────────────────────────────────────────────────────
struct Field { const char* ptr; size_t len; };

static int splitCSV(const char* line, size_t llen, Field* out, int maxf) {
    int f = 0;
    const char* p = line;
    const char* end = line + llen;
    while (p <= end && f < maxf) {
        if (p < end && *p == '"') {
            ++p;
            const char* s = p;
            while (p < end && *p != '"') ++p;
            out[f++] = {s, size_t(p - s)};
            if (p < end) ++p;
            if (p < end && *p == ',') ++p;
        } else {
            const char* s = p;
            while (p < end && *p != ',') ++p;
            size_t l = size_t(p - s);
            if (l && s[l-1] == '\r') --l;
            out[f++] = {s, l};
            if (p < end) ++p;
        }
    }
    return f;
}

static const char* snapToLineStart(const char* q, const char* end) {
    const char* nl = static_cast<const char*>(memchr(q, '\n', end - q));
    return nl ? nl + 1 : end;
}

// ── Thread-local accumulator ───────────────────────────────────────────────────
struct TLSoA {
    std::vector<uint64_t>  unique_key, bbl;
    std::vector<uint32_t>  created_date, closed_date, due_date, res_updated, incident_zip;
    std::vector<uint16_t>  community_board, council_district, police_precinct;
    std::vector<int32_t>   x_coord, y_coord;
    std::vector<uint8_t>   agency_code, loc_type_code, addr_type_code;
    std::vector<uint8_t>   facility_code, status_code, channel_code;
    std::vector<uint8_t>   borough_code, park_borough_code;
    std::vector<uint16_t>  problem_code, problem_detail_code;
    std::vector<StringRef> agency_name, addl_details, inc_addr, street_name;
    std::vector<StringRef> cross1, cross2, intersect1, intersect2;
    std::vector<StringRef> city, landmark, res_desc;
    std::vector<StringRef> park_fac, vehicle, taxi_boro, taxi_pickup;
    std::vector<StringRef> bridge_name, bridge_dir, road_ramp, bridge_seg;
    std::vector<StringRef> location;
    std::vector<LatLon>    latlon;

    void reserve(size_t n) {
        unique_key.reserve(n);      bbl.reserve(n);
        created_date.reserve(n);    closed_date.reserve(n);
        due_date.reserve(n);        res_updated.reserve(n);
        incident_zip.reserve(n);
        community_board.reserve(n); council_district.reserve(n);
        police_precinct.reserve(n);
        x_coord.reserve(n);         y_coord.reserve(n);
        agency_code.reserve(n);     loc_type_code.reserve(n);
        addr_type_code.reserve(n);  facility_code.reserve(n);
        status_code.reserve(n);     channel_code.reserve(n);
        borough_code.reserve(n);    park_borough_code.reserve(n);
        problem_code.reserve(n);    problem_detail_code.reserve(n);
        agency_name.reserve(n);     addl_details.reserve(n);
        inc_addr.reserve(n);        street_name.reserve(n);
        cross1.reserve(n);          cross2.reserve(n);
        intersect1.reserve(n);      intersect2.reserve(n);
        city.reserve(n);            landmark.reserve(n);
        res_desc.reserve(n);        park_fac.reserve(n);
        vehicle.reserve(n);         taxi_boro.reserve(n);
        taxi_pickup.reserve(n);     bridge_name.reserve(n);
        bridge_dir.reserve(n);      road_ramp.reserve(n);
        bridge_seg.reserve(n);      location.reserve(n);
        latlon.reserve(n);
    }
};

// ── DataStore::load ────────────────────────────────────────────────────────────
void DataStore::load(const std::string& csv_path, uint64_t row_start, uint64_t row_end,
                     int num_threads) {
    main_pool_ = std::make_unique<StringPool>(400ULL * 1024 * 1024);
    res_pool_  = std::make_unique<StringPool>(400ULL * 1024 * 1024);

    int fd = open(csv_path.c_str(), O_RDONLY);
    if (fd < 0) throw std::runtime_error("cannot open: " + csv_path);
    struct stat sb; fstat(fd, &sb);
    size_t fsz = sb.st_size;
    const char* base = static_cast<const char*>(
        mmap(nullptr, fsz, PROT_READ, MAP_PRIVATE, fd, 0));
    close(fd);
    if (base == MAP_FAILED) throw std::runtime_error("mmap failed");
    madvise(const_cast<char*>(base), fsz, MADV_SEQUENTIAL);

    const char* fend = base + fsz;
    const char* p0 = static_cast<const char*>(memchr(base, '\n', fsz));
    if (!p0) { munmap(const_cast<char*>(base), fsz); throw std::runtime_error("empty file"); }
    ++p0;

    static constexpr uint64_t TOTAL_ROWS = 20129232ULL;
    size_t data_bytes = fend - p0;

    const char* rs = (row_start == 0) ? p0
        : snapToLineStart(p0 + (size_t)((double)row_start / TOTAL_ROWS * data_bytes), fend);

    const char* re = (row_end >= TOTAL_ROWS - 1) ? fend
        : snapToLineStart(p0 + (size_t)((double)(row_end + 1) / TOTAL_ROWS * data_bytes), fend);

    int T = num_threads;
    size_t span = re - rs;
    std::vector<const char*> cs(T + 1);
    cs[0] = rs; cs[T] = re;
    for (int t = 1; t < T; ++t) {
        const char* q = rs + (span / T) * t;
        const char* nl = static_cast<const char*>(memchr(q, '\n', re - q));
        cs[t] = nl ? nl + 1 : re;
    }

    std::vector<TLSoA> tl(T);
    size_t est = (row_end - row_start + 1) / T + 1;
    for (auto& s : tl) s.reserve(est);

    #pragma omp parallel for schedule(static,1) num_threads(T)
    for (int t = 0; t < T; ++t) {
        TLSoA& s = tl[t];
        const char* p = cs[t];
        const char* tend = cs[t + 1];
        Field f[col::MAX];

        while (p < tend) {
            const char* nl = static_cast<const char*>(memchr(p, '\n', tend - p));
            size_t llen = nl ? size_t(nl - p) : size_t(tend - p);
            int nf = splitCSV(p, llen, f, col::MAX);
            p = nl ? nl + 1 : tend;
            if (nf <= col::LONGITUDE) continue;

            auto fld = [&](int c) -> Field {
                return c < nf ? f[c] : Field{nullptr, 0};
            };
            auto sv = [&](int c) -> std::string_view {
                auto ff = fld(c);
                return {ff.ptr ? ff.ptr : "", ff.len};
            };
            auto store_main = [&](int c) -> StringRef {
                auto ff = fld(c);
                if (!ff.len) return {0, 0};
                return main_pool_->store(ff.ptr, uint16_t(std::min(ff.len, size_t(65535))));
            };

            s.unique_key.push_back(parseInt<uint64_t>(fld(col::UNIQUE_KEY).ptr, fld(col::UNIQUE_KEY).len));
            s.created_date.push_back(parseDate(fld(col::CREATED_DATE).ptr, fld(col::CREATED_DATE).len));
            s.closed_date.push_back(parseDate(fld(col::CLOSED_DATE).ptr, fld(col::CLOSED_DATE).len));
            s.due_date.push_back(parseDate(fld(col::DUE_DATE).ptr, fld(col::DUE_DATE).len));
            s.res_updated.push_back(parseDate(fld(col::RES_UPDATED).ptr, fld(col::RES_UPDATED).len));
            s.incident_zip.push_back(parseInt<uint32_t>(fld(col::ZIP).ptr, fld(col::ZIP).len));
            s.community_board.push_back(parseInt<uint16_t>(fld(col::COMM_BOARD).ptr, fld(col::COMM_BOARD).len));
            s.council_district.push_back(0);
            s.police_precinct.push_back(0);
            s.bbl.push_back(parseInt<uint64_t>(fld(col::BBL).ptr, fld(col::BBL).len));
            s.x_coord.push_back(parseInt<int32_t>(fld(col::X_COORD).ptr, fld(col::X_COORD).len));
            s.y_coord.push_back(parseInt<int32_t>(fld(col::Y_COORD).ptr, fld(col::Y_COORD).len));

            s.agency_code.push_back(reg_agency_.encode(sv(col::AGENCY)));
            s.loc_type_code.push_back(reg_loc_type_.encode(sv(col::LOC_TYPE)));
            s.addr_type_code.push_back(reg_addr_type_.encode(sv(col::ADDR_TYPE)));
            s.facility_code.push_back(reg_facility_.encode(sv(col::FACILITY)));
            s.status_code.push_back(reg_status_.encode(sv(col::STATUS)));
            s.channel_code.push_back(reg_channel_.encode(sv(col::CHANNEL)));
            s.borough_code.push_back(reg_borough_.encode(sv(col::BOROUGH)));
            s.park_borough_code.push_back(reg_park_borough_.encode(sv(col::PARK_BORO)));
            s.problem_code.push_back(reg_problem_.encode(sv(col::COMPLAINT)));
            s.problem_detail_code.push_back(reg_problem_detail_.encode(sv(col::DESCRIPTOR)));

            s.agency_name.push_back(store_main(col::AGENCY_NAME));
            s.addl_details.push_back(store_main(col::ADDL_DETAILS));
            s.inc_addr.push_back(store_main(col::INC_ADDR));
            s.street_name.push_back(store_main(col::STREET));
            s.cross1.push_back(store_main(col::CROSS1));
            s.cross2.push_back(store_main(col::CROSS2));
            s.intersect1.push_back(store_main(col::INTERSECT1));
            s.intersect2.push_back(store_main(col::INTERSECT2));
            s.city.push_back(store_main(col::CITY));
            s.landmark.push_back(store_main(col::LANDMARK));
            s.park_fac.push_back(store_main(col::PARK_FAC));
            s.vehicle.push_back(store_main(col::VEHICLE));
            s.taxi_boro.push_back(store_main(col::TAXI_BORO));
            s.taxi_pickup.push_back(store_main(col::TAXI_PICKUP));
            s.bridge_name.push_back(store_main(col::BRIDGE_NAME));
            s.bridge_dir.push_back(store_main(col::BRIDGE_DIR));
            s.road_ramp.push_back(store_main(col::ROAD_RAMP));
            s.bridge_seg.push_back(store_main(col::BRIDGE_SEG));
            s.location.push_back(store_main(col::LOCATION));

            {
                auto ff = fld(col::RES_DESC);
                s.res_desc.push_back(ff.len
                    ? res_pool_->store(ff.ptr, uint16_t(std::min(ff.len, size_t(65535))))
                    : StringRef{0, 0});
            }

            float lat = parseFloat(fld(col::LATITUDE).ptr, fld(col::LATITUDE).len);
            float lon = parseFloat(fld(col::LONGITUDE).ptr, fld(col::LONGITUDE).len);
            s.latlon.push_back({lat, lon});
        }
    }

    munmap(const_cast<char*>(base), fsz);

    size_t total = 0;
    for (const auto& s : tl) total += s.unique_key.size();

    unique_key_.resize(total);          bbl_.resize(total);
    created_date_.resize(total);        closed_date_.resize(total);
    due_date_.resize(total);            res_updated_date_.resize(total);
    incident_zip_.resize(total);
    community_board_.resize(total);     council_district_.resize(total);
    police_precinct_.resize(total);
    x_coord_.resize(total);             y_coord_.resize(total);
    agency_code_.resize(total);         loc_type_code_.resize(total);
    addr_type_code_.resize(total);      facility_code_.resize(total);
    status_code_.resize(total);         channel_code_.resize(total);
    borough_code_.resize(total);        park_borough_code_.resize(total);
    problem_code_.resize(total);        problem_detail_code_.resize(total);
    agency_name_.resize(total);         additional_details_.resize(total);
    incident_address_.resize(total);    street_name_.resize(total);
    cross_street_1_.resize(total);      cross_street_2_.resize(total);
    intersect_street_1_.resize(total);  intersect_street_2_.resize(total);
    city_.resize(total);                landmark_.resize(total);
    resolution_desc_.resize(total);     park_facility_.resize(total);
    vehicle_type_.resize(total);        taxi_company_borough_.resize(total);
    taxi_pickup_.resize(total);         bridge_hwy_name_.resize(total);
    bridge_hwy_dir_.resize(total);      road_ramp_.resize(total);
    bridge_hwy_seg_.resize(total);      location_.resize(total);
    latlon_.resize(total);

    size_t off = 0;
    for (int t = 0; t < T; ++t) {
        TLSoA& s = tl[t];
        size_t n = s.unique_key.size();
        if (!n) continue;

        auto cp = [&](auto& dst, auto& src) {
            std::copy(src.begin(), src.end(), dst.begin() + off);
        };

        cp(unique_key_, s.unique_key);          cp(bbl_, s.bbl);
        cp(created_date_, s.created_date);      cp(closed_date_, s.closed_date);
        cp(due_date_, s.due_date);              cp(res_updated_date_, s.res_updated);
        cp(incident_zip_, s.incident_zip);
        cp(community_board_, s.community_board);
        cp(council_district_, s.council_district);
        cp(police_precinct_, s.police_precinct);
        cp(x_coord_, s.x_coord);                cp(y_coord_, s.y_coord);
        cp(agency_code_, s.agency_code);        cp(loc_type_code_, s.loc_type_code);
        cp(addr_type_code_, s.addr_type_code);  cp(facility_code_, s.facility_code);
        cp(status_code_, s.status_code);        cp(channel_code_, s.channel_code);
        cp(borough_code_, s.borough_code);      cp(park_borough_code_, s.park_borough_code);
        cp(problem_code_, s.problem_code);      cp(problem_detail_code_, s.problem_detail_code);
        cp(agency_name_, s.agency_name);        cp(additional_details_, s.addl_details);
        cp(incident_address_, s.inc_addr);      cp(street_name_, s.street_name);
        cp(cross_street_1_, s.cross1);          cp(cross_street_2_, s.cross2);
        cp(intersect_street_1_, s.intersect1);  cp(intersect_street_2_, s.intersect2);
        cp(city_, s.city);                      cp(landmark_, s.landmark);
        cp(resolution_desc_, s.res_desc);       cp(park_facility_, s.park_fac);
        cp(vehicle_type_, s.vehicle);           cp(taxi_company_borough_, s.taxi_boro);
        cp(taxi_pickup_, s.taxi_pickup);        cp(bridge_hwy_name_, s.bridge_name);
        cp(bridge_hwy_dir_, s.bridge_dir);      cp(road_ramp_, s.road_ramp);
        cp(bridge_hwy_seg_, s.bridge_seg);      cp(location_, s.location);
        cp(latlon_, s.latlon);

        off += n;
    }

    std::cerr << "[DataStore] loaded " << total << " records\n";
}

// ── toProto ────────────────────────────────────────────────────────────────────
mini2::ServiceRecord DataStore::toProto(size_t i) const {
    mini2::ServiceRecord sr;
    sr.set_unique_key(unique_key_[i]);
    sr.set_created_date(created_date_[i]);
    sr.set_incident_zip(incident_zip_[i]);
    sr.set_latitude(latlon_[i].lat);
    sr.set_longitude(latlon_[i].lon);
    sr.set_borough(borough_code_[i]);
    return sr;
}

// ── Search ─────────────────────────────────────────────────────────────────────
std::vector<mini2::ServiceRecord> DataStore::searchByZip(uint32_t zip_min, uint32_t zip_max) const {
    std::vector<mini2::ServiceRecord> out;
    size_t n = incident_zip_.size();
    #pragma omp parallel num_threads(4)
    {
        std::vector<mini2::ServiceRecord> local;
        #pragma omp for nowait schedule(static)
        for (size_t i = 0; i < n; ++i)
            if (incident_zip_[i] >= zip_min && incident_zip_[i] <= zip_max)
                local.push_back(toProto(i));
        #pragma omp critical
        out.insert(out.end(), local.begin(), local.end());
    }
    return out;
}

std::vector<mini2::ServiceRecord> DataStore::searchByDate(uint32_t date_min, uint32_t date_max) const {
    std::vector<mini2::ServiceRecord> out;
    size_t n = created_date_.size();
    #pragma omp parallel num_threads(4)
    {
        std::vector<mini2::ServiceRecord> local;
        #pragma omp for nowait schedule(static)
        for (size_t i = 0; i < n; ++i)
            if (created_date_[i] >= date_min && created_date_[i] <= date_max)
                local.push_back(toProto(i));
        #pragma omp critical
        out.insert(out.end(), local.begin(), local.end());
    }
    return out;
}

std::vector<mini2::ServiceRecord> DataStore::searchByBBox(double lat_min, double lat_max,
                                                           double lon_min, double lon_max) const {
    std::vector<mini2::ServiceRecord> out;
    float fla = float(lat_min), flb = float(lat_max);
    float flo = float(lon_min), flp = float(lon_max);
    size_t n = latlon_.size();
    #pragma omp parallel num_threads(4)
    {
        std::vector<mini2::ServiceRecord> local;
        #pragma omp for nowait schedule(static)
        for (size_t i = 0; i < n; ++i) {
            float lat = latlon_[i].lat, lon = latlon_[i].lon;
            if (lat >= fla && lat <= flb && lon >= flo && lon <= flp)
                local.push_back(toProto(i));
        }
        #pragma omp critical
        out.insert(out.end(), local.begin(), local.end());
    }
    return out;
}