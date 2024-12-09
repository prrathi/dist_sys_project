#pragma once

#include <memory>
#include <unordered_map>
#include <mutex>
#include <grpcpp/grpcpp.h>

#include "hydfs.h"
#include "rainstorm_factory.grpc.pb.h"
#include "rainstorm_node_server.h"
#include "rainstorm_node.h"

class RainstormFactory : public rainstorm_factory::RainstormFactoryService::Service {
public:
    RainstormFactory();
    ~RainstormFactory();

    void runHydfs();
    void runServer() { rainstorm_node_server_.wait(); }
    grpc::Status CreateServer(grpc::ServerContext* context, 
        const rainstorm_factory::ServerRequest* request, 
        rainstorm_factory::OperationStatus* response) override;
    grpc::Status RemoveServer(grpc::ServerContext* context,
        const rainstorm_factory::ServerRequest* request, 
        rainstorm_factory::OperationStatus* response) override;
    RainstormNodeBase* getNode(int port) {
        std::lock_guard<std::mutex> lock(servers_mutex_);
        auto it = active_servers_.find(port);
        return it != active_servers_.end() ? it->second.get() : nullptr;
    }

private:
    std::string server_address_;
    std::unique_ptr<grpc::Server> server_;
    std::unordered_map<int, std::unique_ptr<RainstormNodeBase>> active_servers_;
    RainStormServer rainstorm_node_server_;
    std::mutex servers_mutex_;
    Hydfs hydfs_;
};
