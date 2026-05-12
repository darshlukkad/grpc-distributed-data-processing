#include "node_service.hpp"
#include <future>
#include <chrono>
#include <iostream>

NodeService::NodeService(std::string node_id,
                         DataStore& store,
                         ChunkManager& chunks,
                         std::vector<PeerInfo> peers,
                         int max_msg_bytes)
    : node_id_(std::move(node_id))
    , store_(store)
    , chunks_(chunks)
    , peers_(std::move(peers))
    , max_msg_bytes_(max_msg_bytes)
{}

std::string NodeService::generateRequestId() const {
    auto ns = std::chrono::steady_clock::now().time_since_epoch().count();
    return node_id_ + "_" + std::to_string(ns);
}

std::vector<mini2::ServiceRecord> NodeService::searchLocal(const mini2::Query& q) const {
    switch (q.type()) {
        case mini2::ZIP_RANGE:  return store_.searchByZip(q.zip_min(), q.zip_max());
        case mini2::DATE_RANGE: return store_.searchByDate(q.date_min(), q.date_max());
        case mini2::BBOX:       return store_.searchByBBox(q.lat_min(), q.lat_max(),
                                                           q.lon_min(), q.lon_max());
        default:                return {};
    }
}

std::vector<mini2::ServiceRecord> NodeService::gatherFromPeers(const mini2::Query& q) const {
    using RecVec = std::vector<mini2::ServiceRecord>;

    std::vector<std::future<RecVec>> futures;
    futures.reserve(peers_.size());

    for (const auto& peer : peers_) {
        futures.push_back(std::async(std::launch::async, [&peer, &q, this]() -> RecVec {
            grpc::ChannelArguments args;
            args.SetInt(GRPC_ARG_MAX_RECEIVE_MESSAGE_LENGTH, max_msg_bytes_);
            args.SetInt(GRPC_ARG_MAX_SEND_MESSAGE_LENGTH,    max_msg_bytes_);
            auto ch   = grpc::CreateCustomChannel(peer.address,
                            grpc::InsecureChannelCredentials(), args);
            auto stub = mini2::NodeService::NewStub(ch);

            grpc::ClientContext ctx;
            mini2::ForwardResponse resp;
            auto st = stub->Forward(&ctx, q, &resp);
            if (!st.ok()) {
                std::cerr << "[" << node_id_ << "] Forward to " << peer.id
                          << " failed: " << st.error_message() << "\n";
                return {};
            }
            return RecVec(resp.records().begin(), resp.records().end());
        }));
    }

    RecVec all;
    for (auto& f : futures) {
        auto part = f.get();
        all.insert(all.end(),
                   std::make_move_iterator(part.begin()),
                   std::make_move_iterator(part.end()));
    }
    return all;
}

grpc::Status NodeService::Submit(grpc::ServerContext*,
                                  const mini2::Query* req,
                                  mini2::SubmitResponse* resp) {
    std::string req_id = generateRequestId();
    resp->set_request_id(req_id);

    auto local_future = std::async(std::launch::async, [&]{ return searchLocal(*req); });
    auto peer_recs    = gatherFromPeers(*req);
    auto local_recs   = local_future.get();

    peer_recs.insert(peer_recs.end(),
                     std::make_move_iterator(local_recs.begin()),
                     std::make_move_iterator(local_recs.end()));

    std::cerr << "[" << node_id_ << "] Submit " << req_id
              << " total=" << peer_recs.size() << "\n";

    chunks_.store(req_id, std::move(peer_recs));
    return grpc::Status::OK;
}

grpc::Status NodeService::Fetch(grpc::ServerContext*,
                                 const mini2::FetchRequest* req,
                                 mini2::FetchResponse* resp) {
    resp->set_request_id(req->request_id());
    resp->set_chunk_idx(req->chunk_idx());

    std::vector<mini2::ServiceRecord> recs;
    bool is_last = false;
    if (!chunks_.fetch(req->request_id(), req->chunk_idx(), req->chunk_size(), recs, is_last))
        return grpc::Status(grpc::StatusCode::NOT_FOUND, "unknown or cancelled request_id");

    for (auto& r : recs) *resp->add_records() = std::move(r);
    resp->set_is_last(is_last);
    if (is_last) chunks_.cancel(req->request_id());
    return grpc::Status::OK;
}

grpc::Status NodeService::Forward(grpc::ServerContext*,
                                   const mini2::Query* req,
                                   mini2::ForwardResponse* resp) {
    auto local_future = std::async(std::launch::async, [&]{ return searchLocal(*req); });
    auto peer_recs    = gatherFromPeers(*req);
    auto local_recs   = local_future.get();

    for (auto& r : peer_recs)  *resp->add_records() = std::move(r);
    for (auto& r : local_recs) *resp->add_records() = std::move(r);

    std::cerr << "[" << node_id_ << "] Forward returning "
              << resp->records_size() << " records\n";
    return grpc::Status::OK;
}

grpc::Status NodeService::Cancel(grpc::ServerContext*,
                                  const mini2::CancelRequest* req,
                                  mini2::CancelResponse*) {
    chunks_.cancel(req->request_id());
    return grpc::Status::OK;
}