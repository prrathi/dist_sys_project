#pragma once
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <mutex>
#include <rainstorm_service_client.h>
#include "hydfs.h"

#define DEFAULT_NUM_STAGES 3;

struct TaskInfo {
    int task_index;
    int stage_index;
    std::string operator_executable;   
    std::string vm; // the host to communicate with 
    //std::string port_num; //port num

    // nodes that this task has downstream (potential targets)
    std::vector<int> port_nums; // port nums of downstream targets
    std::vector<std::string> assigned_nodes; // downstream targets
};

struct JobInfo {
    std::string job_id;
    std::string src_file;
    std::string dest_file;
    int num_stages = DEFAULT_NUM_STAGES;
    int num_tasks_per_stage;
    std::vector<TaskInfo> tasks;

    // just for the final stage
    unordered_set<int> seen_kv_ids; // to be used server side SendDataChunksToLeader
    int num_completed_final_task = 0;
};

class RainStormLeader {
public:
    RainStormLeader() : rainstorm_node_server(this) {};
    ~RainStormLeader();
    void runHydfsServer();
    void SubmitJob(const std::string &op1, const std::string &op2, const std::string &src_file, const std::string &dest_file, int num_tasks);
    void HandleNodeFailure(const std::string &failed_node_id);
    void pipeListener();

    JobInfo& GetJobInfo(const std::string& job_id) { return jobs_[job_id]; }
    Hydfs& GetHydfs() { return hydfs; }
    
    std::mutex mtx_;
    // unordered_set<int> seen_kv_ids; // to be used server side SendDataChunksToLeader
    // std::mutex seen_kv_ids_mtx;

private:
    std::vector<std::string> GetAllWorkerVMs();
    std::string GenerateJobId();
    int getUnusedPortNumberForVM(const std::string& vm);
    string getNextVM();
    vector<string> getTargetNodes(const int stage_num,  vector<TaskInfo>& tasks, int num_stages);
    
private:
    const std::string leader_address = "fa24-cs425-5801.cs.illinois.edu"; 
    // job_id -> JobInfo
    std::unordered_map<std::string, JobInfo> jobs_;

    Hydfs hydfs; // setup this  

    string listener_pipe_path = "/tmp/mp4-leader"; 
    // keep track of the ports being used for each vm
    // make sure to delete when failure happens + decrement when

    unordered_map<string, unordered_set<int>> used_ports_per_vm;
    const int initial_port_number = 8083;
    int total_tasks_running_counter = 0; // should decrement when a task finishes nvm dont bother just go up...

    // Example: get all VMs from hydfs or another source
    RainStormServer rainstorm_node_server;
};
