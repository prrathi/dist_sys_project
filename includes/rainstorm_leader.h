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

// Renamed to LeaderTaskInfo to avoid conflict with TaskInfo in rainstorm_common.h
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
    ~RainStormLeader();

    void runHydfs();
    void runServer() { rainstorm_node_server_.wait(); }
    void submitJob(const std::string &op1, const std::string &op2, const std::string &src_file, const std::string &dest_file, int num_tasks);
    void handleNodeFailure(const std::string &failed_node_id);
    void pipeListener();

    JobInfo& getJobInfo(const std::string& job_id) { return jobs_[job_id]; }
    Hydfs& getHydfs() { return hydfs; }

    std::mutex mtx_;

private:
    std::vector<std::string> getAllWorkerVMs();
    std::string generateJobId();
    int getUnusedPortNumberForVM(const std::string& vm);
    std::string getNextVM();
    std::pair<std::vector<std::string>, std::vector<int>> getTargetNodes(int stage_num, std::vector<LeaderTaskInfo>& tasks, int num_stages);

private:
    const std::string leader_address = "fa24-cs425-5801.cs.illinois.edu"; 
    std::string listener_pipe_path = "/tmp/mp4-leader";
    const int leader_port = 8083;
    const int node_factory_port = 8083;
    std::mutex mtx_;
    std::unordered_map<std::string, JobInfo> jobs_;
    std::unordered_map<std::string, std::unordered_set<int>> used_ports_per_vm;
    int total_tasks_running_counter = 0;
    RainStormServer rainstorm_node_server_;
    Hydfs hydfs;

    // rainstorm factory client methods
    bool CreateServerOnNode(const std::string& node_address, int port);
    bool RemoveServerFromNode(const std::string& node_address, int port);
};
