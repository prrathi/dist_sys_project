#include <iostream>
#include <string>
#include <mutex>
#include <map>
#include <memory>
#include <thread>

#include "rainstorm_factory_server.h"

static const int SERVER_PORT = 8083;
static const int FACTORY_PORT = 8084;

using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;
using rainstorm_factory::ServerRequest;
using rainstorm_factory::OperationStatus;
using rainstorm_factory::StatusCode;

RainstormFactory::RainstormFactory() : rainstorm_node_server_(this, SERVER_PORT){
    char hostname[256];
    if (gethostname(hostname, sizeof(hostname)) != 0) {
        perror("gethostname");
        exit(1);
    }
    string hostname_str = hostname;
    server_address_ = hostname_str + ":" + to_string(FACTORY_PORT);

    ServerBuilder builder;
    grpc::ChannelArguments args;
    args.SetInt(GRPC_ARG_ALLOW_REUSEPORT, 1);
    builder.AddListeningPort(server_address_, grpc::InsecureServerCredentials());
    builder.AddChannelArgument(GRPC_ARG_ALLOW_REUSEPORT, 1);
    builder.RegisterService(this);
    server_ = builder.BuildAndStart();
    if (!server_) {
        cerr << "Failed to start factory server on " << server_address_ << endl;
        exit(1);
    }
    cout << "Factory gRPC Server listening on " << server_address_ << endl;
}

RainstormFactory::~RainstormFactory() {
    if (server_) server_->Shutdown();
    active_servers_.clear();
}

grpc::Status RainstormFactory::CreateServer(grpc::ServerContext* context,
    const rainstorm_factory::ServerRequest* request,
    rainstorm_factory::OperationStatus* response) {
    
    cout << "\n=== Factory CreateServer Request ===" << endl;
    cout << "Peer: " << context->peer() << endl;
    cout << "Port: " << request->port() << endl;
    cout << "Node Type: " << (request->node_type() == rainstorm_factory::SRC_NODE ? "SRC_NODE" : "STAGE_NODE") << endl;
    
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
            new_server = std::make_unique<RainstormNodeSrc>(hydfs_, port); // huh
            break;
        case rainstorm_factory::NodeType::STAGE_NODE:
            new_server = std::make_unique<RainstormNodeStage>(hydfs_, port); // huh
            break;
        default:
            response->set_status(rainstorm_factory::StatusCode::INVALID);
            response->set_message("Invalid node type");
            return grpc::Status::OK;
    }

    active_servers_[port] = std::move(new_server);
    response->set_status(rainstorm_factory::StatusCode::SUCCESS);
    response->set_message("");
    cout << "Server creation successful" << endl;
    cout << "=== End CreateServer Request ===\n" << endl;
    return grpc::Status::OK;
}

grpc::Status RainstormFactory::RemoveServer(grpc::ServerContext* context,
    const rainstorm_factory::ServerRequest* request,
    rainstorm_factory::OperationStatus* response) {
    
    cout << "\n=== Factory RemoveServer Request ===" << endl;
    cout << "Peer: " << context->peer() << endl;
    cout << "Port: " << request->port() << endl;
    
    std::lock_guard<std::mutex> lock(servers_mutex_);
    int port = request->port();

    auto it = active_servers_.find(port);
    if (it == active_servers_.end()) {
        cout << "Server not found on port " << port << endl;
        response->set_status(rainstorm_factory::StatusCode::NOT_FOUND);
        response->set_message("Server not found on port " + std::to_string(port));
        return grpc::Status::OK;
    }

    cout << "Removing server from active_servers_" << endl;
    active_servers_.erase(it);
    response->set_status(rainstorm_factory::StatusCode::SUCCESS);
    response->set_message("Server removed successfully from port " + std::to_string(port));
    cout << "Server removal successful" << endl;
    cout << "=== End RemoveServer Request ===\n" << endl;
    return grpc::Status::OK;
}

void RainstormFactory::runHydfs() {
    std::thread listener_thread([this](){ hydfs_.pipeListener(); });
    std::thread swim_thread([this](){ hydfs_.swim(); });
    listener_thread.detach();
    swim_thread.detach();
}
