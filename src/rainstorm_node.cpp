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

#include "rainstorm_node.h"
#include "rainstorm_node_server.h"
#include "rainstorm_service_client.h"

using namespace std;
using namespace chrono;

const chrono::seconds RainStormNode::ACK_TIMEOUT(30);
const chrono::seconds RainStormNode::PERSIST_INTERVAL(10);

RainStormNode::RainStormNode(const string& server_address)
    : should_stop_(false)
    , rainstorm_node_server_(server_address, this) {
}

void RainStormNode::runHydfs() {
    thread listener_thread([this](){ this->hydfs_.pipeListener(); });
    thread swim_thread([this](){ this->hydfs_.swim(); });
    thread server_thread([this](){ this->hydfs_.runServer(); });
    listener_thread.join();
    swim_thread.join();
    server_thread.join();
}

void RainStormNode::handleNewStageTask(const rainstorm::NewStageTaskRequest* request) {
    lock_guard<mutex> lock(task_mtx_);

    current_task_.job_id = request->job_id();
    current_task_.stage_id = request->stage_id();
    current_task_.task_id = request->task_id();
    current_task_.task_count = request->task_count();
    current_task_.operator_executable = request->executable();
    current_task_.upstream_queue = make_shared<SafeQueue<vector<KVStruct>>>();
    current_task_.stateful = request->stateful();
    current_task_.last = request->last();

    current_task_.processed_file = current_task_.job_id + "_" + to_string(current_task_.stage_id) + "_" + to_string(current_task_.task_id) + "_processed.log";
    current_task_.filtered_file = current_task_.job_id + "_" + to_string(current_task_.stage_id) + "_" + to_string(current_task_.task_id) + "_filtered.log";
    current_task_.state_output_file = current_task_.job_id + "_" + to_string(current_task_.stage_id) + "_" + to_string(current_task_.task_id) + "_output.log";

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

bool RainStormNode::dequeueAcks(vector<int>& acks) {
    lock_guard<mutex> lock(acked_ids_mtx_);
    if (!current_task_.ack_queue.empty()) {
        return current_task_.ack_queue[0]->dequeue(acks);
    }
    return false;
}

void RainStormNode::checkPendingAcks() {
    lock_guard<mutex> lock(pending_ack_mtx_);
    auto now = chrono::steady_clock::now();
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

void RainStormNode::loadProcessedIds() {
    lock_guard<mutex> lock(task_mtx_);
    ifstream processed_file(current_task_.processed_file);
    string line;
    while (getline(processed_file, line)) {
        if(!line.empty()) processed_ids_.insert(stoi(line));
    }
}

void RainStormNode::persistNewIds() {
    lock_guard<mutex> lock(task_mtx_);

    ofstream temp_p_file("temp_processed_ids.txt");
    for (int id : new_processed_ids_) {
        temp_p_file << id << "\n";
        processed_ids_.insert(id);
    }
    for (int id : new_filtered_ids_) {
        temp_p_file << id << "\n";
        processed_ids_.insert(id);
    }
    temp_p_file.close();
    hydfs_.appendFile("temp_processed_ids.txt", current_task_.processed_file);
    remove("temp_processed_ids.txt");
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
    {
        ifstream filtered_file(current_task_.filtered_file);
        string line;
        while (getline(filtered_file, line)) {
            if (!line.empty()) {
                filtered_ids.insert(stoi(line));
            }
        }
    }

    // Get IDs that were processed but not filtered
    unordered_set<int> to_recover;
    for (auto id : processed_ids_) {
        if (filtered_ids.count(id) == 0) {
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

void RainStormNode::processData() {
    loadProcessedIds();
    recoverDataState();

    while (!should_stop_) {
        checkPendingAcks();

        vector<KVStruct> data;
        if (!current_task_.upstream_queue->dequeue(data)) {
            this_thread::sleep_for(chrono::milliseconds(10));
            continue;
        }

        string command = current_task_.operator_executable;
        vector<int> to_ack;

        for (auto &kv : data) {
            if (processed_ids_.count(kv.id) > 0 ||
                new_processed_ids_.count(kv.id) > 0 || 
                new_filtered_ids_.count(kv.id) > 0 || 
                new_acked_ids_.count(kv.id) > 0) {
                to_ack.push_back(kv.id);
                continue;
            }
            command += "]][" + to_string(kv.id) + "]][" + kv.key + "]][" + kv.value + "]][" + to_string(kv.task_index);
        }

        if (command == current_task_.operator_executable) {
            if (!to_ack.empty() && !current_task_.ack_queue.empty()) {
                current_task_.ack_queue[0]->enqueue(to_ack);
            }
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
        for (auto &pa : new_pending_acks) {
            pa.timestamp = steady_clock::now();
        }

        while (getline(output_stream, line)) {
            if (line.empty()) continue;
            size_t current_pos = 0;
            size_t line_length = line.length();
            // Parsing logic: line contains multiple kv records separated by "]][" tokens
            // Format: id]][key]][value]][task_index
            // Actually from code: command building was id]][key]][value]][task_index repeated times

            while (true) {
                // find sequences
                size_t pos1 = line.find("]][", current_pos);
                if (pos1 == string::npos) break;
                size_t pos2 = line.find("]][", pos1 + 3);
                if (pos2 == string::npos) break;
                size_t pos3 = line.find("]][", pos2 + 3);
                if (pos3 == string::npos) break;
                // last might not have a trailing delimiter
                size_t pos4 = line.find("]][", pos3 + 3);

                KVStruct kv;
                try {
                    int id_val = stoi(line.substr(current_pos, pos1 - current_pos));
                    string key = line.substr(pos1 + 3, pos2 - pos1 - 3);
                    string value = line.substr(pos2 + 3, pos3 - pos2 - 3);
                    int task_idx;
                    if (pos4 != string::npos) {
                        task_idx = stoi(line.substr(pos3 + 3, pos4 - pos3 - 3));
                        current_pos = pos4 + 2;
                    } else {
                        task_idx = stoi(line.substr(pos3 + 3));
                        current_pos = line_length;
                    }

                    kv.id = id_val;
                    kv.key = key;
                    kv.value = value;
                    kv.task_index = task_idx;

                    if (current_task_.stateful) {
                        int val = stoi(kv.value);
                        key_to_aggregate_[kv.key] += val;
                        kv.value = to_string(key_to_aggregate_[kv.key]);
                    }

                    if (kv.value == "FILTERED_OUT") {
                        new_filtered_ids_.insert(kv.id);
                        to_ack.push_back(kv.id);
                    } else {
                        new_processed_ids_.insert(kv.id);
                        int partition = partitionData(kv.key, current_task_.task_count);
                        if (partition < (int)new_pending_acks.size()) {
                            new_pending_acks[(size_t)partition].data.push_back(kv);
                        }
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

        if (!new_pending_acks.empty()) {
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
        }

        if (!to_ack.empty() && !current_task_.ack_queue.empty()) {
            current_task_.ack_queue[0]->enqueue(to_ack);
        }

        for (int i = 0; i < current_task_.task_count; i++) {
            if (!new_pending_acks[(size_t)i].data.empty()) {
                current_task_.downstream_queue[(size_t)i]->enqueue(new_pending_acks[(size_t)i].data);
            }
        }
    }
}

void RainStormNode::sendData(std::size_t downstream_node_index) {
    string downstream_address = current_task_.downstream_nodes[downstream_node_index];
    RainStormClient client(grpc::CreateChannel(downstream_address, grpc::InsecureChannelCredentials()));
    client.SendDataChunks(current_task_.downstream_queue[downstream_node_index], new_acked_ids_, acked_ids_mtx_);
}

int RainStormNode::partitionData(const string& key, size_t num_partitions) {
    return (int)(std::hash<std::string>{}(key) % num_partitions);
}
