#pragma once
#include <string>
#include <vector>
#include <grpcpp/grpcpp.h>
#include "mini2.grpc.pb.h"
#include "data_store.hpp"
#include "chunk_manager.hpp"

class NodeService final : public mini2::NodeService::Service {
public:
    struct PeerInfo {
        std::string id;
        std::string address;
    };

    NodeService(std::string node_id,
                DataStore& store,
                ChunkManager& chunks,
                std::vector<PeerInfo> peers,
                int max_msg_bytes);

    grpc::Status Submit      (grpc::ServerContext*, const mini2::Query*,               mini2::SubmitResponse*)       override;
    grpc::Status Fetch       (grpc::ServerContext*, const mini2::FetchRequest*,        mini2::FetchResponse*)        override;
    grpc::Status Forward     (grpc::ServerContext*, const mini2::Query*,               mini2::ForwardResponse*)      override;
    grpc::Status ForwardFetch(grpc::ServerContext*, const mini2::ForwardFetchRequest*, mini2::ForwardFetchResponse*) override;
    grpc::Status Cancel      (grpc::ServerContext*, const mini2::CancelRequest*,       mini2::CancelResponse*)       override;

private:
    std::string           node_id_;
    DataStore&            store_;
    ChunkManager&         chunks_;
    std::vector<PeerInfo> peers_;
    int                   max_msg_bytes_;

    std::string generateRequestId() const;
    std::vector<mini2::ServiceRecord> searchLocal(const mini2::Query& q) const;
    std::vector<mini2::ServiceRecord> fetchAllFromPeer(const PeerInfo& peer, const mini2::Query& q) const;
    std::vector<mini2::ServiceRecord> gatherFromPeers(const mini2::Query& q) const;
};