#include <algorithm>
#include <chrono>
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
#include <future>
#include <ctime>
#include <filesystem>
#include <set>
#include <random>

#include "rainstorm_leader.h"
#include "rainstorm_service_client.h"

RainStormLeader::RainStormLeader() : rainstorm_node_server_(this) {
    if (std::string(getenv("USER")) == "prathi3" || std::string(getenv("USER")) == "praneet") {
        listener_pipe_path = "/tmp/mp4-leader-prathi3";
    }
    std::cout << "FIFO PATH: " << listener_pipe_path << "\n";
    std::vector<std::string> vms = GetAllWorkerVMs();
    for (const auto& vm : vms) {
        used_ports_per_vm[vm] = {};
    }
}

RainStormLeader::~RainStormLeader() {}

std::string RainStormLeader::GenerateJobId() {
    static std::mt19937 gen(std::random_device{}());
    static std::uniform_int_distribution<> dist(1000, 999999);
    return "job_" + std::to_string(dist(gen));
}

void RainStormLeader::pipeListener() {
    if (mkfifo(listener_pipe_path.c_str(), 0666) == -1 && errno != EEXIST) {
        perror("mkfifo");
        exit(EXIT_FAILURE);
    }

    while (true) {
        int fd = open(listener_pipe_path.c_str(), O_RDONLY);
        if (fd == -1) {
            perror("open");
            exit(EXIT_FAILURE);
        }

        std::string command_string;
        char buffer[256]; 
        ssize_t bytes_read;

        while ((bytes_read = read(fd, buffer, sizeof(buffer))) > 0) {
            command_string.append(buffer, bytes_read);
        }

        if (bytes_read == -1) {
            perror("read");
            close(fd);
            continue;
        }

        close(fd); 

        std::istringstream iss(command_string);
        std::string command_type;
        iss >> command_type;

        std::cout << "Received: " << command_type << std::endl;

        if (command_type == "failure") {
            std::string hostname;
            iss >> hostname;
            HandleNodeFailure(hostname);
            continue;
        }

        std::string op1_exe, op2_exe, hydfs_src, hydfs_dest;
        int num_tasks;

        if (!(iss >> op1_exe >> op2_exe >> hydfs_src >> hydfs_dest >> num_tasks)) {
            std::cout << "Error: Invalid job format." << std::endl;
            continue;
        }

        std::cout << "op1: " << op1_exe << " op2: " << op2_exe << " src: " << hydfs_src << " dest: " << hydfs_dest << " num_tasks: " << num_tasks << std::endl;
        submitJob(op1_exe, op2_exe, hydfs_src, hydfs_dest, num_tasks);
    }
}

void RainStormLeader::submitJob(const std::string &op1, const std::string &op2, const std::string &src_file, const std::string &dest_file, int num_tasks) {
    std::lock_guard<std::mutex> lock(mtx_);
    std::string job_id = GenerateJobId();
    JobInfo job;
    job.job_id = job_id;
    job.src_file = src_file;
    job.dest_file = dest_file;
    job.num_tasks_per_stage = num_tasks;

    for (int stage = 0; stage < job.num_stages; stage++) {
        for (int t = 0; t < num_tasks; t++) {
            LeaderTaskInfo task;
            task.stage_index = stage;
            task.task_index = stage * num_tasks + t;
            if (stage == 0) {
                task.operator_executable = "";
            } else if (stage == 1) {
                task.operator_executable = op1;
            } else {
                task.operator_executable = op2;
            }
            job.tasks.push_back(task);
        }
    }

    std::unordered_set<std::string> chosen_first_stage_targets;
    for (auto& task : job.tasks) {
        std::string host_vm = getNextVM();
        task.vm = host_vm;
        task.assigned_nodes = getTargetNodes(task.stage_index, job.tasks, job.num_stages);

        for (const auto& node : task.assigned_nodes) {
            task.port_nums.push_back(getUnusedPortNumberForVM(node));
        }
    }
    jobs_[job.job_id] = job;

    for (const auto& task : job.tasks) {
        std::string target_Address = task.vm + ":" + std::to_string(getUnusedPortNumberForVM(task.vm));
        RainStormClient client(grpc::CreateChannel(target_Address, grpc::InsecureChannelCredentials()));

        if (task.stage_index == 0) {
            client.NewSrcTask(job.job_id, task.task_index, job.num_tasks_per_stage, job.src_file, task.vm, task.port_nums[0]);
        } else if (task.stage_index == 1) {
            client.NewStageTask(job.job_id, task.stage_index, task.task_index, job.num_tasks_per_stage, task.operator_executable, false, false, task.assigned_nodes, task.port_nums);
        } else if (task.stage_index == 2) {
            client.NewStageTask(job.job_id, task.stage_index, task.task_index, job.num_tasks_per_stage, task.operator_executable, true, true, task.assigned_nodes, task.port_nums);
        } else {
            std::cout << "Error: Invalid stage index." << std::endl;
        }
    }
}

void RainStormLeader::HandleNodeFailure(const std::string& failed_node_id) {
    std::lock_guard<std::mutex> lock(mtx_);
    std::cout << "Handling failure for node: " << failed_node_id << std::endl;
    used_ports_per_vm.erase(failed_node_id);

    for (auto &kv : jobs_) {
        JobInfo &job = kv.second;
        std::unordered_set<std::string> chosen_first_stage_targets;
        LeaderTaskInfo new_task_stuff;
        for (auto &task : job.tasks) {
            if (task.vm == failed_node_id) {
                std::cout << "Reassigning Task ID: " << task.task_index << " from VM: " << failed_node_id << std::endl;
                std::string new_vm = getNextVM();
                task.vm = new_vm;
                task.assigned_nodes = getTargetNodes(task.stage_index, job.tasks, job.num_stages);

                task.port_nums.clear();
                for (const auto& node : task.assigned_nodes) {
                    task.port_nums.push_back(getUnusedPortNumberForVM(node));
                }

                new_task_stuff = task;

                std::cout << "Assigned Task ID: " << task.task_index << " to VM: " << new_vm << std::endl;

                std::string target_Address = task.vm + ":" + std::to_string(getUnusedPortNumberForVM(task.vm));
                RainStormClient client(grpc::CreateChannel(target_Address, grpc::InsecureChannelCredentials()));

                if (new_task_stuff.stage_index == 0) {
                    client.NewSrcTask(job.job_id, new_task_stuff.task_index, job.num_tasks_per_stage, job.src_file, new_task_stuff.vm, new_task_stuff.port_nums[0]);
                } else if (task.stage_index == 1) {
                    client.NewStageTask(job.job_id, new_task_stuff.stage_index, new_task_stuff.task_index, job.num_tasks_per_stage, task.operator_executable, false, false, task.assigned_nodes, task.port_nums);
                } else if (task.stage_index == 2) {
                    client.NewStageTask(job.job_id, new_task_stuff.stage_index, new_task_stuff.task_index, job.num_tasks_per_stage, task.operator_executable, true, true, task.assigned_nodes, task.port_nums);
                } else {
                    std::cout << "Error: Invalid stage index." << std::endl;
                }
            }
        }

        for (auto &task : job.tasks) {
            auto it_target = std::find(task.assigned_nodes.begin(), task.assigned_nodes.end(), failed_node_id);
            if (it_target != task.assigned_nodes.end()) {
                int index = (int)std::distance(task.assigned_nodes.begin(), it_target);
                std::string target_Address = task.vm + ":" + std::to_string(getUnusedPortNumberForVM(task.vm));
                RainStormClient client(grpc::CreateChannel(target_Address, grpc::InsecureChannelCredentials()));
                bool update_success = client.UpdateSrcTaskSend(index, new_task_stuff.vm, getUnusedPortNumberForVM(new_task_stuff.vm));
                if (update_success) {
                    std::cout << "Successfully updated sending stream for Task ID: " << task.task_index << " at index: " << index << std::endl;
                } else {
                    std::cout << "Failed to update sending stream for Task ID: " << task.task_index << " at index: " << index << std::endl;
                }
            }
        }
    }
}

std::vector<std::string> RainStormLeader::GetAllWorkerVMs() {
    return hydfs.getVMs();
}

int RainStormLeader::getUnusedPortNumberForVM(const std::string& vm) {
    if (used_ports_per_vm.find(vm) == used_ports_per_vm.end()) {
        used_ports_per_vm[vm] = {};
    }

    for (int i = initial_port_number; i < 65000; ++i) {
        if (used_ports_per_vm[vm].find(i) == used_ports_per_vm[vm].end()) {
            used_ports_per_vm[vm].insert(i);
            return i;
        }
    }

    std::cout << "Ran out of ports?" << std::endl; 
    used_ports_per_vm[vm].insert(initial_port_number + 1);
    return initial_port_number + 1;
}

void RainStormLeader::runHydfs() {
    std::thread listener_thread([this](){ this->hydfs.pipeListener(); });
    std::thread leader_listener_thread([this](){ this->pipeListener(); });
    std::thread swim_thread([this](){ this->hydfs.swim(); });
    std::thread server_thread([this](){ this->hydfs.runServer(); });
    listener_thread.join();
    leader_listener_thread.join();
    swim_thread.join();
    server_thread.join();
}

std::string RainStormLeader::getNextVM() {
    std::vector<std::string> vms = GetAllWorkerVMs();
    std::string vm = vms[total_tasks_running_counter % vms.size()];
    total_tasks_running_counter++;
    return vm;
}

std::vector<std::string> RainStormLeader::getTargetNodes(int stage_num, std::vector<LeaderTaskInfo>& tasks, int num_stages) {
    std::vector<std::string> targetNodes;
    int targetStage = (stage_num + 1) % num_stages;
    for (const auto& task : tasks) {
        if (targetStage == 0) {
            targetNodes.push_back(leader_address);
            return targetNodes;
        }
        if (task.stage_index == targetStage) {
            targetNodes.push_back(task.vm);
        }
    }
    return targetNodes;
}
