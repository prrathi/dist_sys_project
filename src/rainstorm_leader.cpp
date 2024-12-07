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



RainStormLeader::RainStormLeader() {
    if (string(getenv("USER")) == "prathi3" || string(getenv("USER")) == "praneet") {
        listener_pipe_path = "/tmp/mp4-leader-prathi3";
    }
    cout << "FIFO PATH: " << listener_pipe_path << "\n";
    vector<string> vms = GetAllWorkerVMs();
    for (const auto& vm : vms) {
        used_ports_per_vm[vm] = {};
    }
}
RainStormLeader::~RainStormLeader() {}

string RainStormLeader::GenerateJobId() {
    static mt19937 gen(random_device{}());
    static uniform_int_distribution<> dist(1000, 999999);
    return "job_" + to_string(dist(gen));
}

// need to test this cause it may or may not be sent a new line.
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
        string command_type;
        iss >> command_type;

        std::cout << "Received: " << command_type << std::endl;

        if (command_type == "failure") {
            string hostname;
            iss >> hostname;
            HandleNodeFailure(hostname);
            continue;
        }

        std::string op1_exe, op2_exe, hydfs_src, hydfs_dest;
        int num_tasks;

        if (!(iss >> op1_exe >> op2_exe >> hydfs_src >> hydfs_dest >> num_tasks)) {
            std::cout << "Error: Invalid job format." << std::endl;
            continue;  // Go to next loop iteration if parsing fails
        }

        std::cout << "op1: " << op1_exe << " op2: " << op2_exe << " src: " << hydfs_src << " dest: " << hydfs_dest << " num_tasks: " << num_tasks  << std::endl;
        SubmitJob(op1_exe, op2_exe, hydfs_src, hydfs_dest, num_tasks);
    }
}



void RainStormLeader::SubmitJob(const std::string &op1, const std::string &op2, const std::string &src_file, const std::string &dest_file, int num_tasks) {
    std::lock_guard<std::mutex> lock(mtx_);
    string job_id = GenerateJobId();
    JobInfo job;
    job.job_id = job_id;
    job.src_file = src_file;
    job.dest_file = dest_file;
    job.num_tasks_per_stage = num_tasks;

    // stages 0 1 2, stage 0 for src node
    for (int stage = 0; stage < job.num_stages; stage++) {
        for (int t = 0; t < num_tasks; t++) {
            TaskInfo task;
            task.stage_index = stage;
            task.task_index = stage * num_tasks + t;
            if (stage == 0) {
                task.operator_executable = ""; // nothing
            } else if (stage == 1) {
                task.operator_executable = op1;
            } else {
                task.operator_executable = op2;
            }
            //job.tasks.push_back(task); // insert beginning cuz want to build connections end -> src 
            job.tasks.insert(job.tasks.begin(), task);
        }
    }

    // Round-robin assignment of tasks to available worker VMs
    unordered_set<string> chosen_first_stage_targets;
    for (auto& task : job.tasks) {
        string host_vm = getNextVM();
        task.vm = host_vm;
        task.assigned_nodes = getTargetNodes(task.stage_index, job.tasks, job.num_stages);

        // have src stage only have 1 target
        for (const auto& node : task.assigned_nodes) {
            if (task.stage_index == 0 && chosen_first_stage_targets.find(node) == chosen_first_stage_targets.end()) {
                task.assigned_nodes = {node};
                chosen_first_stage_targets.insert(node);
                break;
            }
        }

        // for each vm figure out available port num
        for (const auto& node : task.assigned_nodes) {
            task.port_nums.push_back(getUnusedPortNumberForVM(node));
        }
    }
    jobs_[job.job_id] = job;

    for (const auto& task : job.tasks) {

        string target_Address = task.vm + ":" + std::to_string(getUnusedPortNumberForVM(task.vm)); 
        RainStormClient client(grpc::CreateChannel(target_Address, grpc::InsecureChannelCredentials()));

        // assuming the second one is stateful, first not, 0 is suourc
        if (task.stage_index == 0) {
            client.NewSrcTask(job.job_id, task.task_index, job.num_tasks_per_stage, job.src_file, task.vm, task.port_nums[0]);
        } else if (task.stage_index == 1) {
            client.NewStageTask(job.job_id, task.stage_index, task.task_index, job.num_tasks_per_stage, task.operator_executable, false, false, task.assigned_nodes, task.port_nums);
        } else if (task.stage_index == 2) {
            client.NewStageTask(job.job_id, task.stage_index, task.task_index, job.num_tasks_per_stage, task.operator_executable, true, true, task.assigned_nodes, task.port_nums);
        } else {
            std::cout << "Error: Invalid stage index." << std::endl; // assuming only 2 stages
        }
        

    }
}

void RainStormLeader::HandleNodeFailure(const string& failed_node_id) {
    std::lock_guard<std::mutex> lock(mtx_);

    std::cout << "Handling failure for node: " << failed_node_id << std::endl;

    // remove the failed VM from used_ports_per_vm
    auto it = used_ports_per_vm.find(failed_node_id);
    if (it != used_ports_per_vm.end()) {
        used_ports_per_vm.erase(it);
    } 

    // retrieve all active worker VMs 
    std::vector<std::string> workers = GetAllWorkerVMs();

    // iterate through all jobs and tasks find tasks on failed vm and reassign to new vm

    TaskInfo new_task_stuff;

    // should only be one failed node cuz 1 per update
    // should probably check this again
    for (auto &kv : jobs_) {
        JobInfo &job = kv.second;
        unordered_set<string> chosen_first_stage_targets;
        for (auto &task : job.tasks) {
            // if task was scheduled on a failed vm
            if (task.vm == failed_node_id) {
                std::cout << "Reassigning Task ID: " << task.task_index << " from VM: " << failed_node_id << std::endl;

                std::string new_vm = getNextVM();

                task.vm = new_vm;
                task.assigned_nodes = getTargetNodes(task.stage_index, job.tasks, job.num_stages);

                    // have src stage only have 1 target
                for (const auto& node : task.assigned_nodes) {
                    if (task.stage_index == 0 && chosen_first_stage_targets.find(node) == chosen_first_stage_targets.end()) {
                        // this shouldnt occur, dont think source stage will fail, anyways 
                        task.assigned_nodes = {node};
                        chosen_first_stage_targets.insert(node);
                        break;
                    }
                }
                task.port_nums = {};
                // for each vm figure out available port num
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
                    std::cout << "Error: Invalid stage index." << std::endl; // assuming only 2 stages
                }


            }
        }
    }

    // any node that has the failed node as downstream target needs to have that target changed to the newly assigned task, update them
    for (auto &kv : jobs_) {
        JobInfo &job = kv.second;
        for (auto &task : job.tasks) {

            auto it_target = std::find(task.assigned_nodes.begin(), task.assigned_nodes.end(), failed_node_id);
            if (it_target != task.assigned_nodes.end()) {
                // calculate the index of the failed_node_id
                int index = std::distance(task.assigned_nodes.begin(), it_target);

                // sending to the vm that needs to be alerted
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
    
    std::cout << "Ran out of ports? " << std::endl; 

    used_ports_per_vm[vm] = {initial_port_number + 1};
    return initial_port_number + 1;
}


void RainStormLeader::runHydfsServer() {
    std::thread listener_thread([this](){ this->hydfs.pipeListener(); });
    std::thread swim_thread([this](){ this->hydfs.swim(); });
    std::thread server_thread([this](){ this->hydfs.runServer(); });
    listener_thread.join();
    swim_thread.join();
    server_thread.join();
}

string RainStormLeader::getNextVM() {
    vector<string> vms = GetAllWorkerVMs();
    string vm = vms[total_tasks_running_counter % vms.size()];
    total_tasks_running_counter++;
    return vm;
}

// can talk about this later, assuming stage 0 means leader. (a lil weird)
vector<string> RainStormLeader::getTargetNodes(const int stage_num,  vector<TaskInfo>& tasks, int num_stages) {
    vector<string> targetNodes;
    int targetStage = (stage_num + 1) % num_stages; // num_stages = 0 means leader not src, nort
    for (const auto& task : tasks) {
        // have nothing -> last stage
        if (task.stage_index == targetStage && targetStage != 0) {
            targetNodes.push_back(task.vm);
        }
        if (targetStage == 0) {
            targetNodes.push_back(leader_address);
            return targetNodes;
        }
    }
    return targetNodes;
}
