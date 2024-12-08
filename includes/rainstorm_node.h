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
    rainstorm_factory::StatusCode createServer(int port);
    rainstorm_factory::StatusCode removeServer(int port);
    void handleNewStageTask(const rainstorm::NewStageTaskRequest* request);

    void enqueueIncomingData(const std::vector<KVStruct>& data);
    bool dequeueAcks(std::vector<int>& acks, int task_index);

private:
    static const std::chrono::seconds ACK_TIMEOUT; 
    static const std::chrono::seconds PERSIST_INTERVAL; 

    void processData();
    void sendData(std::size_t downstream_node_index);
    void recoverDataState();
    void loadIds(string filename, unordered_set<int>& ids);
    void storeIds(string filename, unordered_set<int>& ids);
    void checkPendingAcks();
    void retryPendingData(const PendingAck& pending);
    void persistNewIds();
    void persistNewOutput(const std::vector<PendingAck>& new_pending_acks);
    void enqueueAcks(const std::vector<vector<int>>& acks);
    int partitionData(const std::string& key, std::size_t num_partitions);

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
    const int factory_port_ = 8083;
    std::mutex servers_mutex_;
    std::unique_ptr<grpc::Server> factory_server_;
    std::unordered_map<int, std::unique_ptr<grpc::Server>> rainstorm_servers_;
    Hydfs hydfs_;

    void runFactoryServer();

    std::chrono::steady_clock::time_point last_persist_time_;
};
