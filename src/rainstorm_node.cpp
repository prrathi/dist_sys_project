#include <iostream>
#include <cstdlib> 
#include <string>
#include <thread>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <ifaddrs.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <algorithm>
#include <random>
#include <chrono>
#include <future>
#include <ctime>
#include <filesystem>

#include "rainstorm_node.h"
#include "hydfs.h"


#define BUFFER_SIZE 1024 * 32  // 32KB buffer


void RainStormNode::runHydfs() {
    std::thread listener_thread([this](){ this->hydfs.pipeListener(); });
    std::thread swim_thread([this](){ this->hydfs.swim(); });
    listener_thread.join();
    swim_thread.join();
}

void RainStormNode::runServer() {
    server_.wait();
}

void RainStormNode::HandleNewStageTask(const rainstorm::NewStageTaskRequest* request) {
    std::lock_guard<std::mutex> lock(task_info_mtx_);

    TaskInfo task_info;
    task_info.task_id = std::to_string(request->task_id());
    task_info.stage_number = request->stage_id();
    task_info.operator_executable = request->executable();
    
    // Handle downstream nodes (snd_addresses and snd_ports)
    for (int i = 0; i < request->snd_addresses_size(); i++) {
        std::string addr = request->snd_addresses(i) + ":" + std::to_string(request->snd_ports(i));
        task_info.downstream_nodes.push_back(addr);
        task_info.downstream_queue.push_back(std::make_unique<SafeQueue<std::vector<std::pair<std::string, std::string>>>>());
        task_info.ack_queue.push_back(std::make_unique<SafeQueue<std::vector<std::pair<std::string, std::string>>>>());
    }
    
    // Handle upstream nodes (rcv_addresses and rcv_ports)
    for (int i = 0; i < request->rcv_addresses_size(); i++) {
        std::string addr = request->rcv_addresses(i) + ":" + std::to_string(request->rcv_ports(i));
        task_info.upstream_nodes.push_back(addr);
    }
    
    std::string task_id = task_info.task_id;
    task_info_[task_id] = std::move(task_info);

    // Start processing thread
    std::thread([this, task_id]() {
        std::lock_guard<std::mutex> lock(task_info_mtx_);
        if (task_info_.find(task_id) != task_info_.end()) {
            ProcessData(task_info_[task_id]);
        }
    }).detach();

    // Start data sending threads
    size_t num_downstream = task_info_[task_id].downstream_nodes.size();
    for (size_t i = 0; i < num_downstream; ++i) {
        std::thread([this, task_id, i]() {
            std::lock_guard<std::mutex> lock(task_info_mtx_);
            if (task_info_.find(task_id) != task_info_.end()) {
                SendDataToDownstreamNode(task_info_[task_id], i);
            }
        }).detach();
    }
}

void RainStormNode::EnqueueToBeProcessed(const std::string& task_id, const rainstorm::KV& kv) {
    std::lock_guard<std::mutex> lock(task_info_mtx_);
    if (task_info_.find(task_id) != task_info_.end()) {
        auto& task_info = task_info_[task_id];
        size_t partition = partitionData(kv.key(), task_info.downstream_nodes.size());
        if (partition < task_info.downstream_queue.size()) {
            std::vector<std::pair<std::string, std::string>> data;
            data.emplace_back(kv.key(), kv.value());
            task_info.downstream_queue[partition]->enqueue(std::move(data));
        }
    }
}

bool RainStormNode::DequeueProcessed(std::string& task_id, std::vector<std::pair<std::string, std::string>>& acked_ids) {
    std::lock_guard<std::mutex> lock(task_info_mtx_);
    for (const auto& [id, info] : task_info_) {
        for (const auto& queue : info.ack_queue) {
            std::vector<std::pair<std::string, std::string>> data;
            if (queue->try_dequeue(data)) {
                task_id = id;
                acked_ids = std::move(data);
                return true;
            }
        }
    }
    return false;
}

void RainStormNode::ProcessData(const TaskInfo& task_info) {
    // Process data from upstream nodes
    for (const auto& upstream_node : task_info.upstream_nodes) {
        RainStormClient client(grpc::CreateChannel(upstream_node, grpc::InsecureChannelCredentials()));
        // Process data from this upstream node...
    }
}

void RainStormNode::SendDataToDownstreamNode(const TaskInfo& task_info, size_t downstream_node_index) {
    std::string downstream_address = task_info.downstream_nodes[downstream_node_index];
    RainStormClient client(grpc::CreateChannel(downstream_address, grpc::InsecureChannelCredentials()));
    // Send data to downstream node...
}

size_t RainStormNode::partitionData(const std::string& key, size_t num_partitions) {
    return std::hash<std::string>{}(key) % num_partitions;
}