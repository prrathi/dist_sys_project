#include <iostream>
#include <string>
#include <mutex>
#include <map>
#include <memory>
#include <thread>

#include "rainstorm_factory_server.h"

using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;
using rainstorm_factory::ServerRequest;
using rainstorm_factory::OperationStatus;
using rainstorm_factory::StatusCode;

RainstormFactory::~RainstormFactory() {
    stop();
}

grpc::Status RainstormFactory::CreateServer(grpc::ServerContext* context,
    const rainstorm_factory::ServerRequest* request,
    rainstorm_factory::OperationStatus* response) {
    
    std::lock_guard<std::mutex> lock(servers_mutex_);
    int port = request->port();

    if (active_servers_.find(port) != active_servers_.end()) {
        response->set_status(rainstorm_factory::StatusCode::ALREADY_EXISTS);
        response->set_message("Server already exists on port " + std::to_string(port));
        return grpc::Status::OK;
    }

    std::unique_ptr<RainstormNodeBase> new_server;
    switch (request->node_type()) {
        case rainstorm_factory::NodeType::SRC_NODE:
            new_server = std::make_unique<RainstormNodeSrc>(hydfs_);
            break;
        case rainstorm_factory::NodeType::STAGE_NODE:
            new_server = std::make_unique<RainstormNodeStage>(hydfs_);
            break;
        default:
            response->set_status(rainstorm_factory::StatusCode::INVALID);
            response->set_message("Invalid node type");
            return grpc::Status::OK;
    }

    active_servers_[port] = std::move(new_server);
    response->set_status(rainstorm_factory::StatusCode::SUCCESS);
    response->set_message("Server created successfully on port " + std::to_string(port));
    return grpc::Status::OK;
}

grpc::Status RainstormFactory::RemoveServer(grpc::ServerContext* context,
    const rainstorm_factory::ServerRequest* request,
    rainstorm_factory::OperationStatus* response) {
    
    std::lock_guard<std::mutex> lock(servers_mutex_);
    int port = request->port();

    auto it = active_servers_.find(port);
    if (it == active_servers_.end()) {
        response->set_status(rainstorm_factory::StatusCode::NOT_FOUND);
        response->set_message("Server not found on port " + std::to_string(port));
        return grpc::Status::OK;
    }

    active_servers_.erase(it);
    response->set_status(rainstorm_factory::StatusCode::SUCCESS);
    response->set_message("Server removed successfully from port " + std::to_string(port));
    return grpc::Status::OK;
}

void RainstormFactory::run(int port) {
    std::string server_address = "0.0.0.0:" + std::to_string(port);
    grpc::ServerBuilder builder;
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(this);
    server_ = builder.BuildAndStart();
    std::cout << "Factory server listening on " << server_address << std::endl;
    server_->Wait();
}

void RainstormFactory::stop() {
    if (server_) {
        server_->Shutdown();
        server_->Wait();
    }
    active_servers_.clear();
}

void RainstormFactory::runHydfs() {
    std::thread listener_thread([this](){ hydfs_.pipeListener(); });
    std::thread swim_thread([this](){ hydfs_.swim(); });
    listener_thread.join();
    swim_thread.join();
}
