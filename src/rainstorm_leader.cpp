#include <algorithm>
#include <chrono>
#include <iostream>
#include <random>
#include <string>
#include <vector>

#include "rainstorm_leader.h"
#include "rainstorm_service_client.h"

using namespace std;

vector<string> RainStormLeader::GetAllWorkerVMs() {
    // Dummy function: In a real system, this might query the membership layer.
    return vector<string>();
}

RainStormLeader::RainStormLeader() {}
RainStormLeader::~RainStormLeader() {}

string RainStormLeader::GenerateJobId() {
    static mt19937 gen(random_device{}());
    static uniform_int_distribution<> dist(1000, 999999);
    return "job_" + to_string(dist(gen));
}

string RainStormLeader::GenerateTaskId(const string& job_id, int stage_num, int task_index) {
    return job_id + "-" + to_string(stage_num) + "-" + to_string(task_index);
}

string RainStormLeader::SubmitJob(const string& op1, const string& op2,
                               const string& src_file, const string& dest_file,
                               int num_tasks) {
    std::lock_guard<std::mutex> lock(mtx_);
    string job_id = GenerateJobId();
    JobInfo job;
    job.job_id = job_id;
    job.src_file = src_file;
    job.dest_file = dest_file;
    job.num_tasks_per_stage = num_tasks;

    // Assume 2 stages for simplicity: stage 0 = op1, stage 1 = op2
    job.stages.resize(2);
    for (int stage = 0; stage < 2; stage++) {
        for (int t = 0; t < num_tasks; t++) {
            TaskInfo task;
            task.task_id = GenerateTaskId(job_id, stage, t);
            task.stage_number = stage;
            task.operator_executable = (stage == 0) ? op1 : op2;
            job.stages[stage].push_back(task);
        }
    }

    // Assign tasks to nodes
    AssignTasksToNodes(job);
    jobs_[job_id] = job;

    // Leader would now notify assigned nodes about their tasks using RainStormServiceClient
    // (not shown here, but you would loop through each task, call NewSrcTask or NewStageTask)
    // If stage=0, tasks reading from src_file => NewSrcTask
    // If stage>0, tasks => NewStageTask

    return job_id;
}

void RainStormLeader::AssignTasksToNodes(JobInfo& job) {
    // Round-robin assignment of tasks to available worker VMs
    auto workers = GetAllWorkerVMs();
    int wcount = (int)workers.size();
    if (wcount == 0) {
        std::cerr << "No workers available!\n";
        return;
    }

    int idx = 0;
    for (auto &stage : job.stages) {
        for (auto &task : stage) {
            task.assigned_node.clear();
            task.assigned_node = workers[idx % wcount];
            idx++;
        }
    }
}

void RainStormLeader::HandleNodeFailure(const string& failed_node_id) {
    std::lock_guard<std::mutex> lock(mtx_);
    // On a node failure, we must reschedule tasks from that node to a new node
    // This is a simplified version: reassign all tasks from failed_node_id
    auto workers = GetAllWorkerVMs();
    if (workers.empty()) return;
    int wcount = (int)workers.size();
    int idx = 0;

    for (auto &kv : jobs_) {
        JobInfo &job = kv.second;
        for (auto &stage : job.stages) {
            for (auto &task : stage) {
                if (task.assigned_node && task.assigned_node == failed_node_id) {
                    // pick a different worker
                    // In reality, you might avoid choosing failed_node_id again.
                    idx++; 
                    task.assigned_node = workers[idx % wcount];
                    // Also notify new node and possibly upstream/downstream tasks
                }
            }
        }
    }
}
