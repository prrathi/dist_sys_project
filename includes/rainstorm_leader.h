#pragma once
#include <string>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <mutex>
#include <utility>
#include "rainstorm_service_client.h"
#include "rainstorm_factory.grpc.pb.h"
#include "hydfs.h"
#include "rainstorm_node_server.h"

#define DEFAULT_NUM_STAGES 3

struct LeaderTaskInfo {
    int task_index;
    int stage_index;
    std::string operator_executable;   
    std::string vm;
    int port_num;
    std::vector<std::string> assigned_nodes; 
    std::vector<int> assigned_ports; 
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

    void runHydfs();
    void runServer() { rainstorm_node_server_.wait(); }
    void submitJob(const std::string &op1, const std::string &op2, const std::string &src_file, const std::string &dest_file, int num_tasks);
    void handleNodeFailure(const std::string &failed_node_id);
    void pipeListener();

    JobInfo& getJobInfo(const std::string& job_id) { return jobs_[job_id]; }
    Hydfs& getHydfs() { return hydfs; }
    std::mutex& getMutex() { return mtx; }

private:
    std::vector<std::string> getAllWorkerVMs();
    std::string generateJobId();
    void submitSingleTask(RainStormClient& client, const LeaderTaskInfo& task, const JobInfo& job);
    std::string getNextVM();
    int getUnusedPortNumberForVM(const std::string& vm);
    std::pair<std::vector<std::string>, std::vector<int>> getTargetNodes(int stage_num, std::vector<LeaderTaskInfo>& tasks, int num_stages);
    void jobCompletionChecker();
    bool isJobCompleted(const std::string& job_id);
    bool CreateServerOnNode(const std::string& node_address, int port);
    bool RemoveServerFromNode(const std::string& node_address, int port);

private:
    const std::string leader_address = "fa24-cs425-5801.cs.illinois.edu"; 
    std::string listener_pipe_path = "/tmp/mp4-leader";
    const int node_factory_port = 8083;
    std::unordered_map<std::string, JobInfo> jobs_;
    std::unordered_map<std::string, std::unordered_set<int>> used_ports_per_vm_;
    int total_tasks_running_counter_ = 0;
    RainStormServer rainstorm_node_server_;
    Hydfs hydfs;
    const unordered_map<std::string, bool> is_exec_agg_ = {
        {"test1_1", false},
        {"test1_2", false},
        {"test2_1", false},
        {"test2_2", true},
    };
    std::mutex mtx;  
};
