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
    std::thread server_thread([this](){ this->hydfs.runServer(); });
    listener_thread.join();
    swim_thread.join();
    server_thread.join();
}



void RainStormNode::HandleNewStageTask(const rainstorm::NewStageTaskRequest* request) {
    std::lock_guard<std::mutex> lock(task_info_mtx_);

    TaskInfo task_info;
    task_info.task_id = request->id();
    task_info.stage_number = request->stage_number();
    task_info.operator_executable = request->executable();
    for (const auto& addr : request->next_server_addresses()) {
        task_info.downstream_nodes.push_back(addr);
        task_info.downstream_queue.emplace_back();
        task_info.ack_queue.emplace_back();
    }
    for (const auto& addr : request->prev_server_addresses()) {
        task_info.upstream_nodes.push_back(addr);
    }
    task_info_[task_info.task_id] = task_info;



    // check hydfs to see if log files existed for this task
    // if it is load


    // detach thread to attempt to process data
    std::thread processing_thread([this, task_info]() {
        ProcessData(task_info);
    });
    processing_thread.detach();

    // threads to sending data to downstream nodes
    for (size_t i = 0; i < task_info.downstream_nodes.size(); ++i) {
        std::thread send_data_thread([this, task_info, i]() {
            SendDataToDownstreamNode(task_info, i);
        });
        send_data_thread.detach();
    }
}

// attempt to dequeue 1 chunk of data
void RainStormNode::ProcessData(const TaskInfo& task_info) {
    static const size_t buff_size = 128 * 1024; // 128KB buffer
    char buffer[buff_size];

    while (!should_stop_) {
        std::vector<std::pair<std::string, std::string>> data;
        if (to_be_processed_queue.dequeue(data)) { // blocks until data is available

            std::string command = task_info.operator_executable;
            for (const auto& pair : data) {
                command += " " + pair.first + " " + pair.second;
            }

            // run executable wiht popen
            FILE* pipe = popen(command.c_str(), "r");
            if (!pipe) {
                std::cerr << "Error opening pipe for command: " << command << std::endl;
                should_stop_ = true;
                return;
            }


            std::string all_output;
            // check
            while (fgets(buffer, sizeof(buffer), pipe) != nullptr) {
                all_output += buffer;
            }

            int status = pclose(pipe); // close the pipe

            // process the output back into a vector of key-value pairs
            // ? idk what executables return
            std::vector<std::pair<std::string, std::string>> processed_data;
            std::istringstream output_stream(all_output);
            std::string line;
            while (std::getline(output_stream, line)) {
                // assuming output is in "key value" format, separated by space(s)
                std::istringstream line_stream(line);
                std::string key, value;
                if (line_stream >> key >> value) { // Extract key and value
                    processed_data.push_back({key, value});
                } else {
                    // Handle lines that don't match the "key value" format (if needed)
                    std::cout << "Warning: Invalid output line format: " << line << std::endl;
                }
            }


            // maybe easiest to save locally then push to hydfs

            std::ofstream processed_log_file(task_info.processed_file, std::ios::app); // Open in append mode
            if (processed_log_file.is_open()) {
                for (const auto& pair : data) {
                    processed_log_file << pair.first << " " << pair.second << "\n";
                }
                processed_log_file.close();
            } else {
                std::cout << "Failed to open processed log file: " << task_info.processed_file << std::endl;
            }
            std::ofstream output_log_file(task_info.output_log_file, std::ios::app); // Open in append mode
            if (output_log_file.is_open()) {
                for (const auto& pair : processed_data) {
                    output_log_file << pair.first << " " << pair.second << "\n";
                }
                output_log_file.close();
            } else {
                std::cout << "Failed to open output log file: " << task_info.output_log_file << std::endl;
            }

            // save to hydfs?
            hydfs.createFile(task_info.processed_file, task_info.processed_file);
            hydfs.createFile(task_info.output_log_file, task_info.output_log_file);



            // partition the processed data and enqueue to downstream queues
            std::unordered_map<size_t, std::vector<std::pair<std::string, std::string>>> partitioned_data;
            for (const auto& pair : processed_data) {
                size_t partition_index = partitionData(pair.first, task_info.downstream_nodes.size());
                partitioned_data[partition_index].push_back(pair);
            }

            {
                std::lock_guard<std::mutex> lock(task_info_mtx_);
                            // store input data in processed_tasks
                processed_tasks[task_info.task_id].insert(data.begin(), data.end());
                for (size_t i = 0; i < task_info.downstream_nodes.size(); ++i) {
                    if (partitioned_data.count(i)) {
                        task_info_[task_info.task_id].downstream_queue[i].enqueue(partitioned_data[i]);
                    }
                }
            }
        }
    }
}


void RainStormNode::SendDataToDownstreamNode(const TaskInfo& task_info, size_t downstream_node_index) {
    std::string downstream_address = task_info.downstream_nodes[downstream_node_index];
    RainStormClient client(grpc::CreateChannel(downstream_address, grpc::InsecureChannelCredentials()));
    client.SendDataChunksStage(task_info.task_id, task_info_[task_info.task_id].downstream_queue[downstream_node_index]);
}













size_t RainStormNode::partitionData(const std::string& key, size_t num_partitions) {
    return std::hash<std::string>{}(key) % num_partitions;
}