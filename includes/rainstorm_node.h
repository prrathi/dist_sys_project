#pragma once

#include <string>
#include <vector>
#include <unordered_map>
#include <set>
#include <memory>
#include <mutex>
#include <atomic>
#include <filesystem>
#include <fstream>
#include "safe_queue.h"
#include "hydfs.h"
#include "rainstorm_node_server.h"
#include "rainstorm_service_client.h"

using namespace std;

struct TaskInfo {
    string task_id;
    int stage_number;
    string operator_executable;
    vector<string> upstream_nodes;
    vector<string> downstream_nodes;
    vector<std::unique_ptr<SafeQueue<vector<pair<string, string>>>>> downstream_queue;
    vector<std::unique_ptr<SafeQueue<vector<pair<string, string>>>>> ack_queue;
    string saved_state_output_file;
    string processed_file;
    string acked_file;

    TaskInfo() = default;
    TaskInfo(const TaskInfo&) = delete;
    TaskInfo(TaskInfo&&) = default;
    TaskInfo& operator=(const TaskInfo&) = delete;
    TaskInfo& operator=(TaskInfo&&) = default;
};

class RainStormNode {
public:
    RainStormNode(const std::string& server_address)
        : should_stop_(false)
        , rainstorm_node_server(server_address) {}
    ~RainStormNode() {}

    void runHydfs();
    void runServer() { rainstorm_node_server.wait(); }
    void HandleNewStageTask(const rainstorm::NewStageTaskRequest* request);
    void EnqueueToBeProcessed(const std::string& task_id, const rainstorm::KV& kv);
    bool DequeueProcessed(std::string& task_id, std::vector<std::pair<std::string, std::string>>& acked_ids);

private:
    void ProcessData(const TaskInfo& task_info);
    void SendDataToDownstreamNode(const TaskInfo& task_info, size_t downstream_node_index);
    size_t partitionData(const std::string& key, size_t num_partitions);

private:
    unordered_map<string, set<pair<string, string>>> processed_tasks;
    unordered_map<string, set<pair<string, string>>> acked_data;
    SafeQueue<vector<pair<string, string>>> to_be_processed_queue;
    unordered_map<string, TaskInfo> task_info_;
    std::mutex task_info_mtx_;
    std::atomic<bool> should_stop_;
    Hydfs hydfs;
    RainStormServer rainstorm_node_server;
};
