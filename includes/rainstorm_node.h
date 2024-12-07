#pragma once

#include <condition_variable>
#include <mutex>
#include <string>
#include <vector>
#include <utility>
#include <fstream>
#include <unordered_map>
#include <set>

#include "common.h"
#include "listener.h"
#include "worker.h"
#include "talker.h"
#include "hydfs_server.h"
#include "lru_cache.h"
#include "safequeue.hpp"
#include "hydfs.h"
#include "rainstorm_node_server.h"
#include "rainstorm_service_client.h"


using namespace std;

// Need to think what is the partition key?

// kinda inefficient to use queues like this i guess 
struct TaskInfo {
    string task_id;
    int stage_number;
    string operator_executable;
    vector<string> upstream_nodes;
    vector<string> downstream_nodes;
    vector<SafeQueue<vector<pair<string, string>>>> downstream_queue; //? not sure
    vector<SafeQueue<vector<pair<string, string>>>> ack_queue; //
    string output_log_file;
    string saved_state_file; // depends on operator
    string processed_file;
    string acked_file;
    // what else?
};


class RainStormNode {
public:
    RainStormNode(): rainstorm_node_server(this), should_stop_(false) {}
    ~RainStormNode() {}
    void runHydfs();
    void HandleNewStageTask(const rainstorm::NewStageTaskRequest* request);

private:
void ProcessData(const TaskInfo& task_info);
void SendDataToDownstreamNode(const TaskInfo& task_info, size_t downstream_node_index);
size_t partitionData(const std::string& key, size_t num_partitions);
private:
// when newStageTaskCalled for every next server address wanna make new thread that calls the function that calls SendDataChunks (make sure correct target, info, etc)
// make 2 wrapper functions for src and other stages
// plus the server will create a writer thread (on top of the existing reader thread) for each sendDataChunks 
// so a lot of threads... hmm

// could have a thread running for every task
// careful about sync

// maps prob unecessary
// map of taskid -> processedTasks
// set of processed tasks make sure to dump this, and when get ack, remove from this
unordered_map<string, set<pair<string, string>>> processed_tasks;

// map taskid -> processed stuff that we wanna send down
// every once in a while go through this and partitition into vectors and put into right queue (unless at end)
unordered_map<string, set<pair<string, string>>> acked_data; 

SafeQueue<vector<pair<string, string>>> to_be_processed_queue;

// map from taskid -> information about that task
// upstream nodes, downstream nodes
unordered_map<string, TaskInfo> task_info_;
std::mutex task_info_mtx_;
std::atomic<bool> should_stop_;

// I think flow should go like NewStageTask called -> create new thread  

Hydfs hydfs;

// pass down instance to server so that it can modify the queues and stuff / maybe
RainStormServer rainstorm_node_server;

};
