#pragma once
#include <string>
#include <unordered_map>
#include <vector>
#include <mutex>
#include "hydfs.h"

struct TaskInfo {
    int stage_id;
    int task_id;
    std::string operator_executable;
    std::string assigned_node;
};

struct JobInfo {
    std::string job_id;
    std::string src_file;
    std::string dest_file;
    int num_tasks_per_stage;
    std::vector<std::vector<TaskInfo>> stages;
};

class RainStormLeader {
public:
    RainStormLeader();
    ~RainStormLeader();

    // Submit a job to the system (2-stage pipeline)
    std::string SubmitJob(const std::string &op1, const std::string &op2,
                          const std::string &src_file, const std::string &dest_file,
                          int num_tasks);

    void HandleNodeFailure(const std::string &failed_node_id);


private:
    std::mutex mtx_;
    // job_id -> JobInfo
    std::unordered_map<std::string, JobInfo> jobs_;

    std::string GenerateJobId();
    std::string GenerateTaskId(const std::string &job_id, int stage_num, int task_index);
    void AssignTasksToNodes(JobInfo &job);

    Hydfs hydfs; // setup this  
    
    // Example: get all VMs from hydfs or another source
    std::vector<std::string> GetAllWorkerVMs();
};
