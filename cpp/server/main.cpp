#include <iostream>
#include <fstream>
#include <string>
#include <filesystem>
#include <grpcpp/grpcpp.h>
#include <nlohmann/json.hpp>
#include "data_store.hpp"
#include "chunk_manager.hpp"
#include "node_service.hpp"

using json = nlohmann::json;

static void usage(const char* prog) {
    std::cerr << "Usage: " << prog << " --id <NODE_ID> --config <topology.json>\n";
}

int main(int argc, char** argv) {
    std::string node_id, config_path;
    for (int i = 1; i + 1 < argc; ++i) {
        std::string k = argv[i];
        if (k == "--id")     node_id     = argv[i + 1];
        if (k == "--config") config_path = argv[i + 1];
    }
    if (node_id.empty() || config_path.empty()) { usage(argv[0]); return 1; }

    std::ifstream cfg_file(config_path);
    if (!cfg_file) { std::cerr << "Cannot open config: " << config_path << "\n"; return 1; }
    json cfg = json::parse(cfg_file);

    if (!cfg["nodes"].contains(node_id)) {
        std::cerr << "Node '" << node_id << "' not found in config\n"; return 1;
    }

    auto& nc       = cfg["nodes"][node_id];
    int      port      = nc["port"];
    uint64_t row_start = nc["row_start"];
    uint64_t row_end   = nc["row_end"];
    std::string csv = (std::filesystem::path(config_path).parent_path()
                       / cfg["csv_path"].get<std::string>()).lexically_normal().string();
    int chunk_size     = cfg.value("chunk_size", 500);
    int max_msg        = 64 * 1024 * 1024; // 64 MB

    std::cerr << "[" << node_id << "] Loading rows " << row_start << ".." << row_end << "\n";
    DataStore store;
    store.load(csv, row_start, row_end);

    ChunkManager chunks(chunk_size);

    std::vector<NodeService::PeerInfo> peers;
    for (const auto& pid : cfg["peers"][node_id]) {
        std::string id  = pid.get<std::string>();
        auto& pc        = cfg["nodes"][id];
        std::string addr = pc["host"].get<std::string>() + ":"
                         + std::to_string(pc["port"].get<int>());
        peers.push_back({id, addr});
    }

    NodeService service(node_id, store, chunks, std::move(peers), max_msg);

    grpc::ServerBuilder builder;
    builder.AddListeningPort("0.0.0.0:" + std::to_string(port),
                             grpc::InsecureServerCredentials());
    builder.SetMaxReceiveMessageSize(max_msg);
    builder.SetMaxSendMessageSize(max_msg);
    builder.RegisterService(&service);

    auto server = builder.BuildAndStart();
    std::cerr << "[" << node_id << "] Listening on port " << port << "\n";
    server->Wait();
    return 0;
}