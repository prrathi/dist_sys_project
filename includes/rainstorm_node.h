#pragma once

#include <atomic>
#include <chrono>
#include <mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "hydfs.h"
#include "rainstorm_common.h"
#include "rainstorm_node_server.h"

class RainStormNode {
public:
    RainStormNode(); 
    ~RainStormNode() {}

    void runHydfs();
    void runServer() { rainstorm_node_server_.wait(); }
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
    RainStormServer rainstorm_node_server_;
    Hydfs hydfs_;

    std::chrono::steady_clock::time_point last_persist_time_;
};
