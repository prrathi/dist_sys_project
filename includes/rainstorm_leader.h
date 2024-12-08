#pragma once
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <mutex>
#include "rainstorm_service_client.h"
#include "hydfs.h"
#include "rainstorm_node_server.h"

#define DEFAULT_NUM_STAGES 3

// Renamed to LeaderTaskInfo to avoid conflict with TaskInfo in rainstorm_common.h
struct LeaderTaskInfo {
    int task_index;
    int stage_index;
    std::string operator_executable;   
    std::string vm;
    std::vector<int> port_nums;
    std::vector<std::string> assigned_nodes; 
};

struct JobInfo {
    std::string job_id;
    std::string src_file;
    std::string dest_file;
    int num_stages = DEFAULT_NUM_STAGES;
    int num_tasks_per_stage;
    std::vector<LeaderTaskInfo> tasks;
    std::unordered_set<int> seen_kv_ids; 
    int num_completed_final_task = 0;
};

class RainStormLeader {
public:
    RainStormLeader(); 
    ~RainStormLeader();

    void runHydfsServer();
    void submitJob(const std::string &op1, const std::string &op2, const std::string &src_file, const std::string &dest_file, int num_tasks);
    void HandleNodeFailure(const std::string &failed_node_id);
    void pipeListener();

    JobInfo& GetJobInfo(const std::string& job_id) { return jobs_[job_id]; }
    Hydfs& GetHydfs() { return hydfs; }

    std::mutex mtx_;

private:
    std::vector<std::string> GetAllWorkerVMs();
    std::string GenerateJobId();
    int getUnusedPortNumberForVM(const std::string& vm);
    std::string getNextVM();
    std::vector<std::string> getTargetNodes(int stage_num, std::vector<LeaderTaskInfo>& tasks, int num_stages);

private:
    const std::string leader_address = "fa24-cs425-5801.cs.illinois.edu"; 
    std::unordered_map<std::string, JobInfo> jobs_;
    Hydfs hydfs;  
    std::string listener_pipe_path = "/tmp/mp4-leader";
    std::unordered_map<std::string, std::unordered_set<int>> used_ports_per_vm;
    const int initial_port_number = 8083;
    int total_tasks_running_counter = 0;

    RainStormServer rainstorm_node_server; 
};
