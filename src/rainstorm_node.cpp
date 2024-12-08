#include <chrono>
#include <cstdio>
#include <functional>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>
#include <thread>
#include <vector>
#include <fstream>
#include <filesystem>
#include <mutex>

#include "rainstorm_node.h"
#include "rainstorm_node_server.h"
#include "rainstorm_service_client.h"

using namespace std;
using namespace chrono;

const seconds RainStormNode::ACK_TIMEOUT(30);
const seconds RainStormNode::PERSIST_INTERVAL(10);


RainStormNode::RainStormNode()
    : should_stop_(false), factory_server_(nullptr) {
    std::thread factory_thread(&RainStormNode::runFactoryServer, this);
    factory_thread.detach();
}

void RainStormNode::runFactoryServer() {
    std::string server_address = "0.0.0.0:" + std::to_string(factory_port_);
    RainstormFactoryServiceImpl service(this);
    
    grpc::ServerBuilder builder;
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    
    factory_server_ = builder.BuildAndStart();
    std::cout << "Factory Server listening on port " << factory_port_ << std::endl;
    factory_server_->Wait();
}

grpc::Status RainstormFactoryServiceImpl::CreateServer(grpc::ServerContext* context, 
    const rainstorm_factory::ServerRequest* request, rainstorm_factory::OperationStatus* response) {
    
    auto status = node_->createServer(request->port());
    response->set_status(status);
    
    switch (status) {
        case rainstorm_factory::SUCCESS:
            response->set_message("Server created successfully");
            break;
        case rainstorm_factory::ALREADY_EXISTS:
            response->set_message("Port already in use");
            break;
        case rainstorm_factory::INVALID:
        default:
            response->set_message("Internal error creating server");
            break;
    }
    return grpc::Status::OK;
}

grpc::Status RainstormFactoryServiceImpl::RemoveServer(grpc::ServerContext* context,
    const rainstorm_factory::ServerRequest* request, rainstorm_factory::OperationStatus* response) {
    
    auto status = node_->removeServer(request->port());
    response->set_status(status);
    
    switch (status) {
        case rainstorm_factory::SUCCESS:
            response->set_message("Server removed successfully");
            break;
        case rainstorm_factory::NOT_FOUND:
            response->set_message("Server not found on specified port");
            break;
        case rainstorm_factory::INVALID:
        default:
            response->set_message("Internal error removing server");
            break;
    }
    return grpc::Status::OK;
}

rainstorm_factory::StatusCode RainStormNode::createServer(int port) {
    std::lock_guard<std::mutex> lock(servers_mutex_);
    
    if (rainstorm_servers_.find(port) != rainstorm_servers_.end()) {
        return rainstorm_factory::ALREADY_EXISTS;
    }
    
    try {
        std::string server_address = "0.0.0.0:" + std::to_string(port);
        grpc::ServerBuilder builder;
        builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
        // Add RainStorm service
        RainStormServer server(this);
        builder.RegisterService(&server);
        
        auto new_server = builder.BuildAndStart();
        if (!new_server) {
            return rainstorm_factory::INVALID;
        }
        
        rainstorm_servers_[port] = std::move(new_server);
        std::cout << "RainStorm Server started on port " << port << std::endl;
        return rainstorm_factory::SUCCESS;
    } catch (...) {
        std::cerr << "Exception while creating server on port " << port << std::endl;
        return rainstorm_factory::INVALID;
    }
}

rainstorm_factory::StatusCode RainStormNode::removeServer(int port) {
    std::lock_guard<std::mutex> lock(servers_mutex_);
    
    auto it = rainstorm_servers_.find(port);
    if (it == rainstorm_servers_.end()) {
        return rainstorm_factory::NOT_FOUND;
    }
    
    try {
        if (it->second) {
            it->second->Shutdown();
            std::cout << "RainStorm Server on port " << port << " shutdown" << std::endl;
        }
        rainstorm_servers_.erase(it);
        return rainstorm_factory::SUCCESS;
    } catch (...) {
        std::cerr << "Exception while removing server on port " << port << std::endl;
        return rainstorm_factory::INVALID;
    }
}

void RainStormNode::runHydfs() {
    thread listener_thread([this](){ this->hydfs_.pipeListener(); });
    thread swim_thread([this](){ this->hydfs_.swim(); });
    listener_thread.join();
    swim_thread.join();
}

void RainStormNode::handleNewStageTask(const rainstorm::NewStageTaskRequest* request) {
    lock_guard<mutex> lock(task_mtx_);

    current_task_.job_id = request->job_id();
    current_task_.stage_index = request->stage_index();
    current_task_.task_index = request->task_index();
    current_task_.task_count = request->task_count();
    current_task_.operator_executable = request->executable();
    current_task_.upstream_queue = make_shared<SafeQueue<vector<KVStruct>>>();
    current_task_.stateful = request->stateful();
    current_task_.last = request->last();
    if (current_task_.stage_index == 0) {
        current_task_.prev_task_count = 1;
    } else {
        current_task_.prev_task_count = current_task_.task_count;
    }

    current_task_.processed_file = current_task_.job_id + "_" + to_string(current_task_.stage_index) + "_" + to_string(current_task_.task_index) + "_processed.log";
    current_task_.filtered_file = current_task_.job_id + "_" + to_string(current_task_.stage_index) + "_" + to_string(current_task_.task_index) + "_filtered.log";
    current_task_.state_output_file = current_task_.job_id + "_" + to_string(current_task_.stage_index) + "_" + to_string(current_task_.task_index) + "_output.log";

    if (!filesystem::exists(current_task_.processed_file)) {
        ofstream(current_task_.processed_file, ios::out | ios::trunc).close();
    }
    hydfs_.createFile(current_task_.processed_file, current_task_.processed_file);

    if (!filesystem::exists(current_task_.filtered_file)) {
        ofstream(current_task_.filtered_file, ios::out | ios::trunc).close();
    }
    hydfs_.createFile(current_task_.filtered_file, current_task_.filtered_file);

    if (!filesystem::exists(current_task_.state_output_file)) {
        ofstream(current_task_.state_output_file, ios::out | ios::trunc).close();
    }
    hydfs_.createFile(current_task_.state_output_file, current_task_.state_output_file);

    hydfs_.getFile(current_task_.processed_file, current_task_.processed_file);
    hydfs_.getFile(current_task_.filtered_file, current_task_.filtered_file);
    hydfs_.getFile(current_task_.state_output_file, current_task_.state_output_file);

    for (int i = 0; i < request->snd_addresses_size(); i++) {
        current_task_.downstream_nodes.push_back(request->snd_addresses(i) + ":" + to_string(request->snd_ports(i)));
        current_task_.downstream_queue.push_back(make_shared<SafeQueue<vector<KVStruct>>>());
    }
    for (int i = 0; i < current_task_.prev_task_count; i++) {
        current_task_.ack_queue.push_back(make_shared<SafeQueue<vector<int>>>());
    }

    thread(&RainStormNode::processData, this).detach();
    for (size_t i = 0; i < current_task_.downstream_nodes.size(); i++) {
        thread(&RainStormNode::sendData, this, i).detach();
    }
}

void RainStormNode::enqueueIncomingData(const vector<KVStruct>& data) {
    lock_guard<mutex> lock(upstream_mtx_);
    if (current_task_.upstream_queue) {
        current_task_.upstream_queue->enqueue(data);
    }
}

bool RainStormNode::dequeueAcks(vector<int>& acks, int task_index) {
    lock_guard<mutex> lock(acked_ids_mtx_);
    return current_task_.ack_queue[task_index]->dequeue(acks);
}

void RainStormNode::checkPendingAcks() {
    lock_guard<mutex> lock(pending_ack_mtx_);
    auto now = steady_clock::now();
    vector<int> to_retry;

    for (auto it = pending_acked_dict_.begin(); it != pending_acked_dict_.end();) {
        int pending_id = (it->second.data.empty()) ? -1 : it->second.data.front().id;
        if (new_acked_ids_.count(pending_id)) {
            it = pending_acked_dict_.erase(it);
        } else if (now - it->second.timestamp > ACK_TIMEOUT) {
            to_retry.push_back(pending_id);
            it->second.timestamp = now;
            ++it;
        } else {
            ++it;
        }
    }

    for (int id : to_retry) {
        for (auto &kv : pending_acked_dict_) {
            if (!kv.second.data.empty() && kv.second.data.front().id == id) {
                retryPendingData(kv.second);
                break;
            }
        }
    }
}

void RainStormNode::retryPendingData(const PendingAck& pending) {
    if (pending.data.empty()) return;
    int partition = partitionData(pending.data[0].key, current_task_.task_count);
    cerr << "Retrying data for ID " << pending.data[0].id 
         << " to partition " << partition << endl;
    if (partition < (int)current_task_.downstream_queue.size()) {
        current_task_.downstream_queue[partition]->enqueue(pending.data);
    }
}

void RainStormNode::loadIds(string filename, unordered_set<int>& ids) {
    lock_guard<mutex> lock(task_mtx_);
    ifstream processed_file(filename);
    string line;
    while (getline(processed_file, line)) {
        if(!line.empty()) ids.insert(stoi(line));
    }
}

void RainStormNode::storeIds(string filename, unordered_set<int>& ids) {
    lock_guard<mutex> lock(task_mtx_);
    ofstream processed_file(filename);
    for (int id : ids) {
        processed_file << id << "\n";
    }
    processed_file.close();
}

void RainStormNode::persistNewIds() {
    lock_guard<mutex> lock(task_mtx_);
    storeIds("temp_processed_ids.txt", new_processed_ids_);
    processed_ids_.merge(new_processed_ids_);
    hydfs_.appendFile("temp_processed_ids.txt", current_task_.processed_file);
    remove("temp_processed_ids.txt");

    storeIds("temp_filtered_ids.txt", new_filtered_ids_);
    processed_ids_.merge(new_filtered_ids_);
    hydfs_.appendFile("temp_filtered_ids.txt", current_task_.processed_file);
    hydfs_.appendFile("temp_filtered_ids.txt", current_task_.filtered_file);
    remove("temp_filtered_ids.txt");
}

void RainStormNode::persistNewOutput(const vector<PendingAck>& new_pending_acks) {
    ofstream temp_stream("temp_state_output.txt");
    for (const auto& pending : new_pending_acks) {
        if (pending.data.empty()) continue;
        for (const auto& kv : pending.data) {
            temp_stream << kv.id << "\n\n"
                        << kv.key << "\n\n"
                        << kv.value << "\n\n"
                        << kv.task_index << "\n\n";
        }
    }
    temp_stream.close();
    hydfs_.appendFile("temp_state_output.txt", current_task_.state_output_file);
    remove("temp_state_output.txt");
}

void RainStormNode::recoverDataState() {
    // Load filtered IDs
    unordered_set<int> filtered_ids;
    loadIds(current_task_.filtered_file, filtered_ids);

    // Load next stage's processed IDs
    unordered_set<int> next_stage_processed;
    for (int task_index = 0; task_index < current_task_.downstream_nodes.size(); task_index++) {
        string next_stage_file = current_task_.job_id + "_" + to_string(current_task_.stage_index + 1) + "_" + to_string(task_index) + "_processed.log";
        string temp_file = "temp_" + next_stage_file;
        hydfs_.getFile(next_stage_file, temp_file);
        if (filesystem::exists(temp_file)) {
            loadIds(temp_file, next_stage_processed);
            filesystem::remove(temp_file);
        }
    }

    // Get IDs that were processed but not filtered and not processed by next stage
    unordered_set<int> to_recover;
    for (auto id : processed_ids_) {
        if (filtered_ids.count(id) == 0 && next_stage_processed.count(id) == 0) {
            to_recover.insert(id);
        }
    }

    if (to_recover.empty()) {
        return;
    }

    // Recover KVstructs from output file
    ifstream output_file(current_task_.state_output_file, ios::binary | ios::ate);
    if (!output_file) return;

    size_t file_size = (size_t)output_file.tellg();
    const size_t CHUNK_SIZE = 4096;
    vector<char> buffer(CHUNK_SIZE);
    size_t current_pos = file_size;
    string leftover;
    vector<KVStruct> recovered_kvs;

    while (current_pos > 0 && !to_recover.empty()) {
        size_t read_size = min(CHUNK_SIZE, current_pos);
        current_pos -= read_size;
        output_file.seekg(current_pos);
        output_file.read(buffer.data(), read_size);
        string chunk(buffer.data(), read_size);
        chunk = chunk + leftover;
        leftover.clear();

        size_t pos = chunk.length();
        while (pos > 0 && !to_recover.empty()) {
            int newline_pairs_found = 0;
            size_t search_pos = pos;
            while (newline_pairs_found < 4 && search_pos > 0) {
                size_t newline_pair = chunk.rfind("\n\n", search_pos - 1);
                if (newline_pair == string::npos) break;
                newline_pairs_found++;
                search_pos = newline_pair;
            }

            if (newline_pairs_found < 4) {
                leftover = chunk.substr(0, pos);
                break;
            }

            // search_pos is start of record
            string record = chunk.substr(search_pos, pos - search_pos);
            pos = search_pos;

            vector<string> parts;
            {
                size_t start = 0;
                while (start < record.size()) {
                    size_t end = record.find("\n\n", start);
                    if (end == string::npos) {
                        if (start < record.size()) {
                            string part = record.substr(start);
                            if (!part.empty()) parts.push_back(part);
                        }
                        break;
                    }
                    string part = record.substr(start, end - start);
                    if (!part.empty()) parts.push_back(part);
                    start = end + 2;
                }
            }

            if (parts.size() >= 4) {
                try {
                    int id = stoi(parts[0]);
                    if (to_recover.count(id) > 0) {
                        KVStruct kv;
                        kv.id = id;
                        kv.key = parts[1];
                        kv.value = parts[2];
                        kv.task_index = stoi(parts[3]);
                        if (current_task_.stateful) {
                            int val = stoi(kv.value);
                            key_to_aggregate_[kv.key] = val;
                        }
                        recovered_kvs.push_back(kv);
                        to_recover.erase(id);
                    }
                } catch (...) {
                    continue;
                }
            }
        }
    }

    if (!recovered_kvs.empty()) {
        vector<vector<KVStruct>> partitioned_kvs((size_t)current_task_.task_count);
        for (auto kv : recovered_kvs) {
            if (current_task_.stateful) {
                int val = stoi(kv.value);
                key_to_aggregate_[kv.key] = val;
                kv.value = to_string(val);
            }
            if (kv.task_index >= 0 && kv.task_index < current_task_.task_count) {
                partitioned_kvs[(size_t)kv.task_index].push_back(kv);
            }
        }
        for (int i = 0; i < current_task_.task_count; i++) {
            if (!partitioned_kvs[i].empty()) {
                current_task_.downstream_queue[i]->enqueue(std::move(partitioned_kvs[i]));
            }
        }
    }
}

void RainStormNode::enqueueAcks(const vector<vector<int>>& acks) {
    for (int i = 0; i < current_task_.prev_task_count; i++) {
        if (!acks.empty()) {
            current_task_.ack_queue[i]->enqueue(acks[i]);
        }
    }
}

void RainStormNode::processData() {
    loadIds(current_task_.processed_file, processed_ids_);
    recoverDataState();

    while (!should_stop_) {
        checkPendingAcks();

        vector<KVStruct> data;
        if (!current_task_.upstream_queue->dequeue(data)) {
            this_thread::sleep_for(milliseconds(10));
            continue;
        }
        string command = current_task_.operator_executable + " ";
        vector<vector<int>> to_ack(current_task_.prev_task_count);

        for (auto &kv : data) {
            if (processed_ids_.count(kv.id) > 0 ||
                new_processed_ids_.count(kv.id) > 0 || 
                new_filtered_ids_.count(kv.id) > 0 || 
                new_acked_ids_.count(kv.id) > 0) {
                to_ack[kv.task_index].push_back(kv.id);
                continue;
            }
            
            string escaped_value = kv.value;
            size_t pos = 0;
            while ((pos = escaped_value.find("\"", pos)) != string::npos) {
                escaped_value.replace(pos, 1, "\\\"");
                pos += 2;
            }
            command += "]][" + to_string(kv.id) + 
                      "]][" + kv.key + 
                      "]][\"" + escaped_value + "\"]][" + 
                      to_string(kv.task_index);
        }

        if (command == current_task_.operator_executable + " ") {
            enqueueAcks(to_ack); 
            continue;
        }

        FILE* pipe = popen(command.c_str(), "r");
        if (!pipe) {
            cerr << "Error opening pipe for command: " << command << endl;
            continue;
        }

        static const size_t buff_size = 128 * 1024;
        char buffer[buff_size];
        string all_output;
        while (fgets(buffer, buff_size, pipe) != nullptr) {
            all_output += buffer;
        }
        pclose(pipe);

        istringstream output_stream(all_output);
        string line;
        vector<PendingAck> new_pending_acks((size_t)current_task_.task_count);

        while (getline(output_stream, line)) {
            if (line.empty()) continue;
            size_t current_pos = 0;
            size_t line_length = line.length();
            // Parsing logic: line contains multiple kv records separated by "]][" tokens
            // Format: id]][key]][value]][task_index]][

            while (true) {
                // find sequences
                size_t pos1 = line.find("]][", current_pos);
                if (pos1 == string::npos) break;
                size_t pos2 = line.find("]][", pos1 + 3);
                if (pos2 == string::npos) break;
                size_t pos3 = line.find("]][", pos2 + 3);
                if (pos3 == string::npos) break;
                size_t pos4 = line.find("]][", pos3 + 3);

                KVStruct kv;
                try {
                    int id_val = stoi(line.substr(current_pos, pos1 - current_pos));
                    string key = line.substr(pos1 + 3, pos2 - pos1 - 3);
                    string value = line.substr(pos2 + 3, pos3 - pos2 - 3);
                    int prev_task_index;
                    if (pos4 != string::npos) {
                        prev_task_index = stoi(line.substr(pos3 + 3, pos4 - pos3 - 3));
                        current_pos = pos4 + 2;
                    } else {
                        prev_task_index = stoi(line.substr(pos3 + 3));
                        current_pos = line_length;
                    }

                    kv.id = id_val;
                    kv.key = key;
                    kv.value = value;
                    kv.task_index = current_task_.task_index;

                    if (current_task_.stateful) {
                        int val = stoi(kv.value);
                        key_to_aggregate_[kv.key] += val;
                        kv.value = to_string(key_to_aggregate_[kv.key]);
                    }

                    if (kv.value == "FILTERED_OUT") {
                        new_filtered_ids_.insert(kv.id);
                        to_ack[prev_task_index].push_back(kv.id);
                    } else {
                        new_processed_ids_.insert(kv.id);
                        int partition = partitionData(kv.key, current_task_.task_count);
                        new_pending_acks[(size_t)partition].data.push_back(kv);
                    }

                } catch (const std::exception &e) {
                    cerr << "Error parsing KV data: " << e.what() << endl;
                    break;
                }

                if (pos4 == string::npos) {
                    break;
                }
            }
        }

        // add processed data to outgoing data queue
        persistNewIds();
        persistNewOutput(new_pending_acks);
        {
            std::lock_guard<std::mutex> lock(pending_ack_mtx_);
            for (auto &pa : new_pending_acks) {
                if (!pa.data.empty()) {
                    int id = pa.data.front().id;
                    pending_acked_dict_[id] = pa;
                }
            }
        }
        for (int i = 0; i < current_task_.task_count; i++) {
            if (!new_pending_acks[(size_t)i].data.empty()) {
                current_task_.downstream_queue[(size_t)i]->enqueue(new_pending_acks[(size_t)i].data);
            }
        }

        enqueueAcks(to_ack);

        // Check if all previous stage tasks are complete
        vector<int> prev_stage_indexs;
        if (current_task_.stage_index == 1) {
            prev_stage_indexs = {current_task_.task_index};
        } else {
            for (int prev_task = 0; prev_task < current_task_.task_count; prev_task++) {
                prev_stage_indexs.push_back(prev_task);
            }
        }
        bool all_prev_complete = true;
        for (int prev_task : prev_stage_indexs) {
            string fin_file = current_task_.job_id + "_" + to_string(current_task_.stage_index - 1) + "_" + to_string(prev_task) + "_fin.log";
            string temp_fin = "temp_" + fin_file;
            
            hydfs_.getFile(fin_file, temp_fin);
            if (!filesystem::exists(temp_fin)) {
                all_prev_complete = false;
                break;
            }
            
            ifstream fin_stream(temp_fin);
            string content;
            getline(fin_stream, content);
            fin_stream.close();
            filesystem::remove(temp_fin);
            
            if (content != "1") {
                all_prev_complete = false;
                break;
            }
        }

        if (all_prev_complete) {
            // Check if all our non-filtered IDs are processed in next stage
            unordered_set<int> all_next_stage_indexs;
            for (int next_task = 0; next_task < current_task_.task_count; next_task++) {
                string next_processed_file = current_task_.job_id + "_" + 
                                            to_string(current_task_.stage_index + 1) + "_" + 
                                            to_string(next_task) + "_processed.log";
                string temp_next = "temp_" + next_processed_file;
                
                hydfs_.getFile(next_processed_file, temp_next);
                if (filesystem::exists(temp_next)) {
                    unordered_set<int> next_ids;
                    loadIds(temp_next, next_ids);
                    all_next_stage_indexs.merge(next_ids);
                    filesystem::remove(temp_next);
                }
            }

            // Check if all our processed IDs are in next stage
            bool all_processed = true;
            for (int id : processed_ids_) {
                if (all_next_stage_indexs.find(id) == all_next_stage_indexs.end()) {
                    all_processed = false;
                    break;
                }
            }

            if (all_processed) {
                // Write our own completion file
                string our_fin = current_task_.job_id + "_" + 
                                to_string(current_task_.stage_index) + "_" + 
                                to_string(current_task_.task_index) + "_fin.log";
                ofstream fin_out("temp_fin.log");
                fin_out << "1" << endl;
                fin_out.close();
                hydfs_.createFile("temp_fin.log", our_fin);
                filesystem::remove("temp_fin.log");
                
                should_stop_ = true;
            }
        }
    }
}

void RainStormNode::sendData(std::size_t downstream_node_index) {
    string downstream_address = current_task_.downstream_nodes[downstream_node_index];
    RainStormClient client(grpc::CreateChannel(downstream_address, grpc::InsecureChannelCredentials()));
    client.SendDataChunks(current_task_.downstream_queue[downstream_node_index], new_acked_ids_, acked_ids_mtx_, current_task_.task_index);
}

int RainStormNode::partitionData(const string& key, size_t num_partitions) {
    return (int)(std::hash<std::string>{}(key) % num_partitions);
}
