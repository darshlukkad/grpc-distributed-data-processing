#include <iostream>
#include <fstream>
#include <string>
#include <chrono>
#include <grpcpp/grpcpp.h>
#include <nlohmann/json.hpp>
#include "mini2.grpc.pb.h"

using json = nlohmann::json;

static void usage(const char* prog) {
    std::cerr
        << "Usage: " << prog << " --config <topology.json> --query <zip|date|bbox> [params]\n"
        << "  zip:  --zip-min N  --zip-max N\n"
        << "  date: --date-min EPOCH  --date-max EPOCH\n"
        << "  bbox: --lat-min F --lat-max F --lon-min F --lon-max F\n";
}

int main(int argc, char** argv) {
    std::string config_path, query_type;
    uint32_t zip_min = 0, zip_max = 0, date_min = 0, date_max = 0;
    double lat_min = 0, lat_max = 0, lon_min = 0, lon_max = 0;

    for (int i = 1; i + 1 < argc; ++i) {
        std::string k = argv[i];
        if (k == "--config")   config_path = argv[i + 1];
        if (k == "--query")    query_type  = argv[i + 1];
        if (k == "--zip-min")  zip_min     = static_cast<uint32_t>(std::stoul(argv[i + 1]));
        if (k == "--zip-max")  zip_max     = static_cast<uint32_t>(std::stoul(argv[i + 1]));
        if (k == "--date-min") date_min    = static_cast<uint32_t>(std::stoul(argv[i + 1]));
        if (k == "--date-max") date_max    = static_cast<uint32_t>(std::stoul(argv[i + 1]));
        if (k == "--lat-min")  lat_min     = std::stod(argv[i + 1]);
        if (k == "--lat-max")  lat_max     = std::stod(argv[i + 1]);
        if (k == "--lon-min")  lon_min     = std::stod(argv[i + 1]);
        if (k == "--lon-max")  lon_max     = std::stod(argv[i + 1]);
    }
    if (config_path.empty() || query_type.empty()) { usage(argv[0]); return 1; }

    std::ifstream cfg_file(config_path);
    if (!cfg_file) { std::cerr << "Cannot open config\n"; return 1; }
    json cfg = json::parse(cfg_file);

    auto& a    = cfg["nodes"]["A"];
    std::string addr = a["host"].get<std::string>() + ":"
                     + std::to_string(a["port"].get<int>());

    int max_msg = 64 * 1024 * 1024;
    grpc::ChannelArguments ch_args;
    ch_args.SetInt(GRPC_ARG_MAX_RECEIVE_MESSAGE_LENGTH, max_msg);
    ch_args.SetInt(GRPC_ARG_MAX_SEND_MESSAGE_LENGTH,    max_msg);

    auto channel = grpc::CreateCustomChannel(addr,
                       grpc::InsecureChannelCredentials(), ch_args);
    auto stub    = mini2::NodeService::NewStub(channel);

    mini2::Query q;
    if (query_type == "zip") {
        q.set_type(mini2::ZIP_RANGE);
        q.set_zip_min(zip_min);
        q.set_zip_max(zip_max);
    } else if (query_type == "date") {
        q.set_type(mini2::DATE_RANGE);
        q.set_date_min(date_min);
        q.set_date_max(date_max);
    } else if (query_type == "bbox") {
        q.set_type(mini2::BBOX);
        q.set_lat_min(lat_min); q.set_lat_max(lat_max);
        q.set_lon_min(lon_min); q.set_lon_max(lon_max);
    } else {
        std::cerr << "Unknown query type: " << query_type << "\n"; return 1;
    }

    auto t0 = std::chrono::steady_clock::now();

    grpc::ClientContext sub_ctx;
    mini2::SubmitResponse sub_resp;
    auto st = stub->Submit(&sub_ctx, q, &sub_resp);
    if (!st.ok()) { std::cerr << "Submit failed: " << st.error_message() << "\n"; return 1; }

    std::string req_id = sub_resp.request_id();
    std::cerr << "request_id: " << req_id << "\n";

    uint64_t total    = 0;
    int      offset   = 0;
    int      csize    = 500;
    int      num_chunks = 0;
    bool     done     = false;

    while (!done) {
        grpc::ClientContext fetch_ctx;
        mini2::FetchRequest freq;
        freq.set_request_id(req_id);
        freq.set_chunk_idx(offset);
        freq.set_chunk_size(csize);

        mini2::FetchResponse fresp;
        st = stub->Fetch(&fetch_ctx, freq, &fresp);
        if (!st.ok()) {
            std::cerr << "Fetch[" << offset << "] failed: " << st.error_message() << "\n";
            break;
        }

        for (const auto& r : fresp.records())
            std::cout << r.unique_key() << ","
                      << r.created_date() << ","
                      << r.closed_date() << ","
                      << r.due_date() << ","
                      << r.resolution_updated_date() << ","
                      << r.agency() << ","
                      << r.agency_name() << ","
                      << r.complaint_type() << ","
                      << r.complaint_detail() << ","
                      << r.location_type() << ","
                      << r.incident_zip() << ","
                      << r.incident_address() << ","
                      << r.street_name() << ","
                      << r.cross_street_1() << ","
                      << r.cross_street_2() << ","
                      << r.intersect_street_1() << ","
                      << r.intersect_street_2() << ","
                      << r.address_type() << ","
                      << r.city() << ","
                      << r.landmark() << ","
                      << r.facility_type() << ","
                      << r.status() << ","
                      << r.resolution_desc() << ","
                      << r.community_board() << ","
                      << r.bbl() << ","
                      << r.borough_name() << ","
                      << r.x_coord() << ","
                      << r.y_coord() << ","
                      << r.open_data_channel() << ","
                      << r.park_facility() << ","
                      << r.park_borough() << ","
                      << r.vehicle_type() << ","
                      << r.taxi_company_borough() << ","
                      << r.taxi_pickup() << ","
                      << r.bridge_hwy_name() << ","
                      << r.bridge_hwy_dir() << ","
                      << r.road_ramp() << ","
                      << r.bridge_hwy_seg() << ","
                      << r.latitude() << ","
                      << r.longitude() << ","
                      << r.location() << "\n";

        int got = fresp.records_size();
        total  += static_cast<uint64_t>(got);
        offset += got;
        done    = fresp.is_last();
        ++num_chunks;
        if (csize < 50000) csize = std::min(csize * 2, 50000);
    }

    auto dt_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                     std::chrono::steady_clock::now() - t0).count();

    std::cerr << "total_records=" << total
              << " chunks=" << num_chunks
              << " dt_ms=" << dt_ms << "\n";
    return 0;
}