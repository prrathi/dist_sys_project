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

using namespace std;

RainStormLeader::RainStormLeader() : rainstorm_node_server_(this) {
    if (string(getenv("USER")) == "prathi3" || string(getenv("USER")) == "praneet") {
        listener_pipe_path = "/tmp/mp4-leader-prathi3";
    }
    cout << "FIFO PATH: " << listener_pipe_path << "\n";
    vector<string> vms = getAllWorkerVMs();
    for (const auto& vm : vms) {
        used_ports_per_vm[vm] = {};
    }
}

RainStormLeader::~RainStormLeader() {}

string RainStormLeader::generateJobId() {
    static mt19937 gen(random_device{}());
    static uniform_int_distribution<> dist(1000, 999999);
    return "job_" + to_string(dist(gen));
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

        string command_string;
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

        istringstream iss(command_string);
        string command_type;
        iss >> command_type;

        cout << "Received: " << command_type << endl;

        if (command_type == "failure") {
            string hostname;
            iss >> hostname;
            handleNodeFailure(hostname);
            continue;
        }

        string op1_exe, op2_exe, hydfs_src, hydfs_dest;
        int num_tasks;

        if (!(iss >> op1_exe >> op2_exe >> hydfs_src >> hydfs_dest >> num_tasks)) {
            cout << "Error: Invalid job format." << endl;
            continue;
        }

        cout << "op1: " << op1_exe << " op2: " << op2_exe << " src: " << hydfs_src << " dest: " << hydfs_dest << " num_tasks: " << num_tasks << endl;
        submitJob(op1_exe, op2_exe, hydfs_src, hydfs_dest, num_tasks);
    }
}

void RainStormLeader::submitJob(const string &op1, const string &op2, const string &src_file, const string &dest_file, int num_tasks) {
    lock_guard<mutex> lock(mtx_);
    string job_id = generateJobId();
    JobInfo job;
    job.job_id = job_id;
    job.src_file = src_file;
    job.dest_file = dest_file;
    job.num_tasks_per_stage = num_tasks;

    // we have single port per task including all receiving from previous stage. this is distinct from fixed port per vm to spin up new rainstormserver for new task
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
            task.vm = getNextVM();
            task.port_num = getUnusedPortNumberForVM(task.vm);
            job.tasks.push_back(task);
        }
    }
    for (auto& task : job.tasks) {
        auto assigned = getTargetNodes(task.stage_index, job.tasks, job.num_stages);
        task.assigned_nodes = assigned.first;
        task.assigned_ports = assigned.second;
    }

    jobs_[job.job_id] = job;

    for (const auto& task : job.tasks) {
        if (!createServerOnNode(task.vm, task.port_num)) {
            cerr << "Failed to create server for task " << task.task_index << endl;
            continue;
        }
        string target_Address = task.vm + ":" + to_string(task.port_num);
        RainStormClient client(grpc::CreateChannel(target_Address, grpc::InsecureChannelCredentials()));

        if (task.stage_index == 0) {
            client.NewSrcTask(job.job_id, task.task_index, job.num_tasks_per_stage, job.src_file, task.vm, task.assigned_ports[task.task_index]);
        } else if (task.stage_index == 1) {
            client.NewStageTask(job.job_id, task.stage_index, task.task_index, job.num_tasks_per_stage, task.operator_executable, false, false, task.assigned_nodes, task.assigned_ports);
        } else if (task.stage_index == 2) {
            client.NewStageTask(job.job_id, task.stage_index, task.task_index, job.num_tasks_per_stage, task.operator_executable, true, true, task.assigned_nodes, task.assigned_ports);
        } else {
            cout << "Error: Invalid stage index." << endl;
        }
    }
}

void RainStormLeader::handleNodeFailure(const string& failed_node_id) {
    lock_guard<mutex> lock(mtx_);
    cout << "Handling failure for node: " << failed_node_id << endl;
    used_ports_per_vm.erase(failed_node_id);

    for (auto &kv : jobs_) {
        JobInfo &job = kv.second;
        for (auto &task : job.tasks) {
            if (task.vm == failed_node_id) {
                cout << "Reassigning Task ID: " << task.task_index << " from VM: " << failed_node_id << endl;
                removeServerOnNode(task.vm, task.port_num);
                string new_vm = getNextVM();
                task.vm = new_vm;
                task.port_num = getUnusedPortNumberForVM(task.vm);
                auto assigned = getTargetNodes(task.stage_index, job.tasks, job.num_stages);
                task.assigned_nodes = assigned.first;
                task.assigned_ports = assigned.second;

                cout << "Assigned Task ID: " << task.task_index << " to VM: " << new_vm << endl;

                if (!createServerOnNode(task.vm, task.port_num)) {
                    cerr << "Failed to create server for replacement task " << task.task_index << endl;
                    continue;
                }
                string target_Address = task.vm + ":" + to_string(task.port_num);
                RainStormClient client(grpc::CreateChannel(target_Address, grpc::InsecureChannelCredentials()));

                if (task.stage_index == 0) {
                    client.NewSrcTask(job.job_id, task.task_index, job.num_tasks_per_stage, job.src_file, task.vm, task.assigned_ports[task.task_index]);
                } else if (task.stage_index == 1) {
                    client.NewStageTask(job.job_id, task.stage_index, task.task_index, job.num_tasks_per_stage, task.operator_executable, false, false, task.assigned_nodes, task.assigned_ports);
                } else if (task.stage_index == 2) {
                    client.NewStageTask(job.job_id, task.stage_index, task.task_index, job.num_tasks_per_stage, task.operator_executable, true, true, task.assigned_nodes, task.assigned_ports);
                } else {
                    cout << "Error: Invalid stage index." << endl;
                }
            }
        }

        for (auto &task : job.tasks) {
            auto it_target = find(task.assigned_nodes.begin(), task.assigned_nodes.end(), failed_node_id);
            if (it_target != task.assigned_nodes.end()) {
                string target_Address = task.vm + ":" + to_string(task.port_num);
                RainStormClient client(grpc::CreateChannel(target_Address, grpc::InsecureChannelCredentials()));
                int diff_index = (int)distance(task.assigned_nodes.begin(), it_target);
                int new_task_index = (task.stage_index + 1) * task.num_tasks_per_stage + diff_index;
                bool update_success = client.UpdateSrcTaskSend(diff_index, job.tasks[new_task_index].vm, job.tasks[new_task_index].port_num);
                if (update_success) {
                    cout << "Successfully updated sending stream for Task ID: " << task.task_index << " at index: " << index << endl;
                } else {
                    cout << "Failed to update sending stream for Task ID: " << task.task_index << " at index: " << index << endl;
                }
            }
        }
    }
}

vector<string> RainStormLeader::getAllWorkerVMs() {
    return hydfs.getVMs();
}

int RainStormLeader::getUnusedPortNumberForVM(const string& vm) {
    if (used_ports_per_vm.find(vm) == used_ports_per_vm.end()) {
        used_ports_per_vm[vm] = {};
    }

    for (int i = node_factory_port; i < 65000; ++i) {
        if (used_ports_per_vm[vm].find(i) == used_ports_per_vm[vm].end()) {
            used_ports_per_vm[vm].insert(i);
            return i;
        }
    }

    cout << "Ran out of ports?" << endl; 
    used_ports_per_vm[vm].insert(node_factory_port + 1);
    return node_factory_port + 1;
}

bool RainStormLeader::createServerOnNode(const string& node_address, int port) {
    string target_address = node_address + ":" + to_string(SERVER_FACTORY_PORT);
    auto channel = grpc::CreateChannel(target_address, grpc::InsecureChannelCredentials());
    auto stub = rainstorm_factory::RainstormFactoryService::NewStub(channel);

    grpc::ClientContext context;
    rainstorm_factory::NewServerRequest request;
    rainstorm_factory::OperationStatus response;

    request.set_port(port);
    grpc::Status status = stub->CreateServer(&context, request, &response);

    if (!status.ok() || response.status() != rainstorm_factory::SUCCESS) {
        cerr << "Failed to create server on " << node_address << ":" << port 
             << " Error: " << response.message() << endl;
        return false;
    }
    return true;
}

bool RainStormLeader::removeServerFromNode(const string& node_address, int port) {
    string target_address = node_address + ":" + to_string(SERVER_FACTORY_PORT);
    auto channel = grpc::CreateChannel(target_address, grpc::InsecureChannelCredentials());
    auto stub = rainstorm_factory::RainstormFactoryService::NewStub(channel);

    grpc::ClientContext context;
    rainstorm_factory::RemoveServerRequest request;
    rainstorm_factory::OperationStatus response;

    request.set_port(port);
    grpc::Status status = stub->RemoveServer(&context, request, &response);

    if (!status.ok() || response.status() != rainstorm_factory::SUCCESS) {
        cerr << "Failed to remove server from " << node_address << ":" << port 
             << " Error: " << response.message() << endl;
        return false;
    }
    return true;
}

void RainStormLeader::runHydfs() {
    thread listener_thread([this](){ this->hydfs.pipeListener(); });
    thread leader_listener_thread([this](){ this->pipeListener(); });
    thread swim_thread([this](){ this->hydfs.swim(); });
    thread server_thread([this](){ this->hydfs.runServer(); });
    listener_thread.join();
    leader_listener_thread.join();
    swim_thread.join();
    server_thread.join();
}

string RainStormLeader::getNextVM() {
    vector<string> vms = getAllWorkerVMs();
    string vm = vms[total_tasks_running_counter % vms.size()];
    total_tasks_running_counter++;
    return vm;
}

pair<vector<string>, vector<int>> RainStormLeader::getTargetNodes(int stage_num, vector<LeaderTaskInfo>& tasks, int num_stages) {
    int targetStage = (stage_num + 1) % num_stages;
    if (targetStage == 0) {
        return {vector<string>{leader_address}, vector<int>{getUnusedPortNumberForVM(leader_address)}};
    }
    vector<string> target_nodes;
    vector<int> target_ports;
    for (const auto& task : tasks) {
        if (task.stage_index == targetStage) {
            target_nodes.push_back(task.vm);
            target_ports.push_back(task.port_num);
        }
    }
    return {target_nodes, target_ports};
}
