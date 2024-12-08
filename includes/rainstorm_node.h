#pragma once

#include <atomic>
#include <chrono>
#include <mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <memory>
#include <grpcpp/grpcpp.h>
#include "rainstorm.grpc.pb.h"
#include "rainstorm_factory.grpc.pb.h"
#include "hydfs.h"
#include "rainstorm_common.h"
#include "rainstorm_node_server.h"

// Error codes for server operations
enum class ServerError {
    SUCCESS = 0,
    PORT_IN_USE,
    PORT_NOT_FOUND,
    INTERNAL_ERROR
};

class RainstormFactoryServiceImpl final : public rainstorm_factory::RainstormFactoryService::Service {
    RainStormNode* node_;
public:
    explicit RainstormFactoryServiceImpl(RainStormNode* node) : node_(node) {}
    grpc::Status CreateServer(grpc::ServerContext* context, const rainstorm_factory::ServerRequest* request, rainstorm_factory::OperationStatus* response) override;
    grpc::Status RemoveServer(grpc::ServerContext* context, const rainstorm_factory::ServerRequest* request, rainstorm_factory::OperationStatus* response) override;
};

class RainStormNode {
public:
    RainStormNode(); 
    ~RainStormNode() {
        if (factory_server_) {
            factory_server_->Shutdown();
        }
        for (auto& [port, server] : rainstorm_servers_) {
            if (server) {
                server->Shutdown();
            }
        }
    }

    void runHydfs();
    ServerError createServer(int port);
    ServerError removeServer(int port);
    void handleNewStageTask(const rainstorm::NewStageTaskRequest* request);

    void enqueueIncomingData(const std::vector<KVStruct>& data);
    bool dequeueAcks(std::vector<int>& acks);

private:
    static const std::chrono::seconds ACK_TIMEOUT; 
    static const std::chrono::seconds PERSIST_INTERVAL; 

    void loadProcessedIds();
    void persistNewIds();
    void persistNewOutput(const std::vector<PendingAck>& new_pending_acks);
    void checkPendingAcks();
    void retryPendingData(const PendingAck& pending);
    void processData();
    void recoverDataState();
    int partitionData(const std::string& key, std::size_t num_partitions);
    void sendData(std::size_t downstream_node_index);

    TaskInfo current_task_;
    std::mutex task_mtx_;
    std::mutex upstream_mtx_;

    std::unordered_set<int> processed_ids_;
    std::unordered_set<int> new_processed_ids_;
    std::unordered_set<int> new_filtered_ids_;
    std::unordered_set<int> new_acked_ids_;
    std::mutex acked_ids_mtx_;
    std::unordered_map<int, PendingAck> pending_acked_dict_;
    std::mutex pending_ack_mtx_;

    std::unordered_map<std::string, int> key_to_aggregate_{};

    std::atomic<bool> should_stop_;
    const int factory_port_;
    std::mutex servers_mutex_;
    std::unique_ptr<grpc::Server> factory_server_;
    std::unordered_map<int, std::unique_ptr<grpc::Server>> rainstorm_servers_;
    Hydfs hydfs_;

    void runFactoryServer();

    std::chrono::steady_clock::time_point last_persist_time_;
};
