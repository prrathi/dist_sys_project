#pragma once

#include <atomic>
#include <chrono>
#include <mutex>
#include <string>
#include <vector>
#include <memory>
#include <queue>
#include <grpcpp/grpcpp.h>

#include "safequeue.hpp"
#include "rainstorm.grpc.pb.h"
#include "rainstorm_common.h"
#include "hydfs.h"

struct PendingAck {
    std::chrono::steady_clock::time_point timestamp;
    std::vector<KVStruct> data;
    int task_index;
};

class RainstormNodeBase {
public:
    int port_; 
    explicit RainstormNodeBase(Hydfs& hydfs) : should_stop_(false), hydfs_(hydfs) {}
    virtual ~RainstormNodeBase() {
        should_stop_ = true;
    }
    virtual void handleUpdateTask(const rainstorm::UpdateTaskSndRequest* request) = 0;

protected:
    virtual void processData() = 0;
    inline int partitionData(const std::string& key, std::size_t num_partitions) {
        size_t hash = std::hash<std::string>{}(key);
        return hash % num_partitions;
    }

    static inline const std::chrono::seconds ACK_TIMEOUT{5};
    std::atomic<bool> should_stop_;
    Hydfs& hydfs_;

    std::string job_id_;
    int task_index_;
    int task_count_;
    std::string state_output_file_;
    std::string processed_file_;
    std::unordered_map<int, PendingAck> pending_acked_dict_;
    std::mutex pending_ack_mtx_;
};

class RainstormNodeSrc : public RainstormNodeBase {
public:
    explicit RainstormNodeSrc::RainstormNodeSrc(Hydfs& hydfs) : RainstormNodeBase(hydfs) {}
    void handleNewSrcTask(const rainstorm::NewSrcTaskRequest* request);
    void handleUpdateTask(const rainstorm::UpdateTaskSndRequest* request);

private:
    void processData() override;
    void sendData();
    void loadIds(std::string filename, std::unordered_set<int>& ids);
    bool isAcked(int id);
    void persistNewOutput(const std::vector<KVStruct>& batch);

    std::string input_file_;
    std::string job_id_;
    int task_count_;
    int task_index_;
    bool file_read_ = false;
    std::string downstream_address_;
    int downstream_port_;

    std::string state_output_file_;
    std::string processed_file_;
    std::string next_processed_file_;
    std::shared_ptr<SafeQueue<std::vector<KVStruct>>> downstream_queue_;
    std::unordered_set<int> acked_ids_;
    std::mutex state_mutex_;
    std::mutex pending_ack_mtx_;
    std::unordered_map<int, PendingAck> pending_acked_dict_;

    std::unique_ptr<std::thread> process_thread_;
    std::unique_ptr<std::thread> send_thread_;
    std::mutex thread_mtx_;
};

class RainstormNodeStage : public RainstormNodeBase {
public:
    explicit RainstormNodeStage::RainstormNodeStage(Hydfs& hydfs) : RainstormNodeBase(hydfs) {}
    void handleNewStageTask(const rainstorm::NewStageTaskRequest* request);
    void handleUpdateTask(const rainstorm::UpdateTaskSndRequest* request);
    void enqueueIncomingData(const std::vector<KVStruct>& data);
    bool dequeueAcks(std::vector<int>& acks, int task_index);

private:
    void processData() override;
    void sendData(std::size_t downstream_node_index);
    void recoverDataState();
    void loadIds(std::string filename, std::unordered_set<int>& ids);
    void storeIds(std::string filename, std::unordered_set<int>& ids);
    void checkPendingAcks();
    void retryPendingData(const PendingAck& pending);
    void persistNewIds();
    void persistNewOutput(const std::vector<PendingAck>& new_pending_acks);
    void enqueueAcks(const std::vector<std::vector<int>>& acks);

    int stage_index_;
    std::string operator_executable_;
    std::shared_ptr<SafeQueue<std::vector<KVStruct>>> upstream_queue_;
    std::vector<std::shared_ptr<SafeQueue<std::vector<KVStruct>>>> downstream_queues_;
    std::vector<std::shared_ptr<SafeQueue<std::vector<int>>>> ack_queues_;
    std::vector<std::string> downstream_addresses_;
    std::vector<int> downstream_ports_;
    std::string filtered_file_;
    int prev_task_count_;
    bool stateful_ = false;
    bool last_ = false;

    std::mutex state_mtx_;
    std::mutex upstream_mtx_;
    std::unordered_set<int> processed_ids_;
    std::unordered_set<int> new_processed_ids_;
    std::unordered_set<int> new_filtered_ids_;
    std::unordered_set<int> new_acked_ids_;
    std::mutex acked_ids_mtx_;
    std::unordered_map<std::string, int> key_to_aggregate_;

    std::unique_ptr<std::thread> process_thread_;
    std::vector<std::unique_ptr<std::thread>> send_threads_;
    std::mutex thread_mtx_;
};
