#pragma once

#include <chrono>
#include <memory>
#include <string>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include "safequeue.hpp"

// Basic data structure for key-value pairs
struct KVStruct {
    int id;
    std::string key;
    std::string value;
    int task_index;
};

// Structure for tracking pending acknowledgments
struct PendingAck {
    std::chrono::steady_clock::time_point timestamp;
    std::vector<KVStruct> data;
    int task_index;
};

// Structure for task information
struct TaskInfo {
    std::string job_id;
    int stage_index;
    int task_index;
    int task_count;
    std::string operator_executable;
    std::shared_ptr<SafeQueue<std::vector<KVStruct>>> upstream_queue;
    std::vector<std::shared_ptr<SafeQueue<std::vector<KVStruct>>>> downstream_queue;
    std::vector<std::shared_ptr<SafeQueue<std::vector<int>>>> ack_queue;
    std::vector<std::string> downstream_nodes;
    std::string state_output_file;
    std::string processed_file;
    std::string filtered_file;
    int prev_task_count;
    bool stateful = false;
    bool last = false;
};
