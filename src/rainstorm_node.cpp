#include <chrono>
#include <cstdio>
#include <functional>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include "rainstorm_node.h"
#include "rainstorm_node_server.h"
#include "rainstorm_service_client.h"
#include "safequeue.hpp"

using namespace std;
using namespace chrono;

#define BUFFER_SIZE 1024 * 32  // 32KB buffer

const chrono::seconds RainStormNode::ACK_TIMEOUT(30);
const chrono::seconds RainStormNode::PERSIST_INTERVAL(10);

string to_string(int32 i) {
    ostringstream ss;
    ss << i;
    return ss.str();
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
    current_task_.upstream_queue = make_unique<SafeQueue<vector<KVStruct>>>();
    current_task_.stateful = request->stateful();
    current_task_.last = request->last();

    current_task_.processed_file = request->job_id() + "_" + to_string(current_task_.stage_id) + "_" + to_string(current_task_.task_id) + "_processed.log";
    current_task_.filtered_file = request->job_id() + "_" + to_string(current_task_.stage_id) + "_" + to_string(current_task_.task_id) + "_filtered.log";
    current_task_.state_output_file = request->job_id() + "_" + to_string(current_task_.stage_id) + "_" + to_string(current_task_.task_id) + "_output.log";
    if (!filesystem::exists(current_task_.processed_file) || filesystem::file_size(current_task_.processed_file) == 0) {
        ofstream(current_task_.processed_file, ios::out | ios::trunc).close();
    }
    hydfs_.createFile(current_task_.processed_file, current_task_.processed_file);
    if (!filesystem::exists(current_task_.filtered_file) || filesystem::file_size(current_task_.filtered_file) == 0) {
        ofstream(current_task_.filtered_file, ios::out | ios::trunc).close();
    }
    hydfs_.createFile(current_task_.filtered_file, current_task_.filtered_file);
    if (!filesystem::exists(current_task_.state_output_file) || filesystem::file_size(current_task_.state_output_file) == 0) {
        ofstream(current_task_.state_output_file, ios::out | ios::trunc).close();
    }
    hydfs_.createFile(current_task_.processed_file, current_task_.processed_file);
    hydfs_.createFile(current_task_.filtered_file, current_task_.filtered_file);
    hydfs_.createFile(current_task_.state_output_file, current_task_.state_output_file);
    hydfs_.getFile(current_task_.processed_file, current_task_.processed_file);
    hydfs_.getFile(current_task_.filtered_file, current_task_.filtered_file);
    hydfs_.getFile(current_task_.state_output_file, current_task_.state_output_file);

    // Initialize downstream queues and ack queues
    for (size_t i = 0; i < request->snd_addresses_size(); i++) {
        current_task_.downstream_nodes.push_back(request->snd_addresses(i) + ":" + to_string(request->snd_ports(i)));
        current_task_.downstream_queue.push_back(make_unique<SafeQueue<vector<KVStruct>>>());
        current_task_.ack_queue.push_back(make_unique<SafeQueue<vector<int>>>());
    }
    
    thread(&RainStormNode::sendData, this, ref(current_task_)).detach();
    thread(&RainStormNode::processData, this, ref(current_task_)).detach();
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
        if (new_acked_ids_.count(it->first)) {
            it = pending_acked_dict_.erase(it);
        } else if (now - it->second.timestamp > ACK_TIMEOUT) {
            to_retry.push_back(it->first);
            it->second.timestamp = now;
            ++it;
        } else {
            ++it;
        }
    }
    
    for (int id : to_retry) {
        retryPendingData(pending_acked_dict_[id]);
    }
}

void RainStormNode::retryPendingData(const PendingAck& pending) {
    lock_guard<mutex> lock(pending_ack_mtx_);
    if (!pending.data.empty()) {
        int partition = partitionData(pending.data[0].key, current_task_.task_count);
        if (partition < current_task_.downstream_queue.size()) {
            cerr << "Retrying data for ID " << pending.id 
                 << " to partition " << partition << endl;
            current_task_.downstream_queue[partition]->enqueue(pending.data);
        }
    }
}

void RainStormNode::loadProcessedIds() {
    lock_guard<mutex> lock(task_mtx_);
    ifstream processed_file(current_task_.processed_file);
    string line;
    while (getline(processed_file, line)) {
        processed_ids_.insert(stoi(line));
    }
    processed_file.close();
}

void RainStormNode::persistNewIds() {
    lock_guard<mutex> lock(task_mtx_);

    ofstream temp_p_file("temp_processed_ids.txt");
    for (int id : new_processed_ids_) {
        temp_p_file << id << "\n";
        processed_ids_.insert(id);
    }
    ofstream temp_f_file("temp_filtered_ids.txt");
    for (int id : new_filtered_ids_) {
        temp_p_file << id << "\n";
        temp_f_file << id << "\n";
        processed_ids_.insert(id);
    }
    temp_p_file.close();
    hydfs_.appendFile("temp_processed_ids.txt", current_task_.processed_file);
    remove("temp_processed_ids.txt");
    temp_f_file.close();
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
    for (const auto& id : processed_ids_) {
        if (filtered_ids.count(id) == 0) {
            to_recover.insert(id);
        }
    }

    // If not the last stage, remove IDs that were successfully processed by next stage
    if (!current_task_.last) {
        for (int i = 0; i < current_task_.task_count; i++) {
            string next_stage_file = current_task_.job_id + "_" + 
                                   to_string(current_task_.stage_id + 1) + "_" + 
                                   to_string(i) + "_processed.log";
            
            if (hydfs_.getFile(next_stage_file, next_stage_file)) {
                ifstream next_stage_processed(next_stage_file);
                string line;
                while (getline(next_stage_processed, line)) {
                    if (!line.empty()) {
                        to_recover.erase(stoi(line));
                    }
                }
                remove(next_stage_file.c_str());
            }
        }
    }

    if (to_recover.empty()) {
        return;
    }

    // Recover KVstructs by reading the output file incrementally from end
    vector<KVStruct> recovered_kvs;
    ifstream output_file(current_task_.state_output_file, ios::binary | ios::ate);
    if (!output_file) return;

    const size_t CHUNK_SIZE = 4096;  // Read 4KB at a time
    vector<char> buffer(CHUNK_SIZE);
    string leftover;  // Holds partial record from previous chunk
    size_t file_size = output_file.tellg();
    size_t current_pos = file_size;

    while (current_pos > 0 && !to_recover.empty()) {
        size_t read_size = min(CHUNK_SIZE, current_pos);
        current_pos -= read_size;
        output_file.seekg(current_pos);
        output_file.read(buffer.data(), read_size);
        string chunk(buffer.data(), read_size);
        chunk = chunk + leftover;
        leftover.clear();

        // Process records in this chunk
        size_t pos = chunk.length();
        while (pos > 0 && !to_recover.empty()) {
            // Find the start of the last complete record (4 double newlines)
            size_t record_end = pos;
            int newline_pairs_found = 0;
            size_t search_pos = record_end;
            
            while (newline_pairs_found < 4 && search_pos > 0) {
                size_t newline_pair = chunk.rfind("\n\n", search_pos - 1);
                if (newline_pair == string::npos) break;
                newline_pairs_found++;
                search_pos = newline_pair;
            }

            if (newline_pairs_found < 4) {
                // Incomplete record, save for next iteration
                leftover = chunk.substr(0, record_end);
                break;
            }

            pos = search_pos;
            string record = chunk.substr(pos, record_end - pos);
            vector<string> parts;
            size_t start = 0;
            
            // Split on double newlines to match the saving format
            while (start < record.length()) {
                size_t end = record.find("\n\n", start);
                if (end == string::npos) {
                    if (start < record.length()) {
                        string part = record.substr(start);
                        if (!part.empty()) {
                            parts.push_back(part);
                        }
                    }
                    break;
                }
                string part = record.substr(start, end - start);
                if (!part.empty()) {
                    parts.push_back(part);
                }
                start = end + 2;
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
                        recovered_kvs.push_back(kv);
                        to_recover.erase(id);
                    }
                } catch (const exception& e) {
                    cerr << "Error parsing record: " << e.what() << endl;
                }
            }
        }
    }

    if (!recovered_kvs.empty()) {
        vector<vector<KVStruct>> partitioned_kvs(current_task_.task_count);
        for (const auto& kv : recovered_kvs) {
            if (current_task_.stateful) {key_to_aggregate_[kv.key] = kv.value;}
            if (kv.task_index >= 0 && kv.task_index < current_task_.task_count) {
                partitioned_kvs[kv.task_index].push_back(kv);
            } else {
                cerr << "Recovered invalid task index: " << kv.task_index << endl;
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
    static const size_t buff_size = 128 * 1024; // 128KB buffer
    char buffer[buff_size];
    
    loadProcessedIds();

    // for now we perform blocking recovery at start
    recoverData();

    while (!should_stop_) {
        checkPendingAcks();

        vector<KVStruct> data;
        if (!current_task_.upstream_queue->dequeue(data)) {
            this_thread::sleep_for(chrono::milliseconds(10));
            continue;
        }

        string command = current_task_.operator_executable;
        vector<int> to_ack;
        
        for (const auto& kv : data) {
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
        string all_output;
        while (fgets(buffer, buff_size, pipe) != nullptr) {
            all_output += buffer;
        }
        pclose(pipe);

        istringstream output_stream(all_output);
        string line;
        vector<PendingAck> new_pending_acks(current_task_.task_count);
        while (getline(output_stream, line)) {
            if (line.empty()) continue;
            try {
                size_t current_pos = 0;
                size_t line_length = line.length();
                while (current_pos < line_length) {
                    size_t pos1 = line.find("]][", current_pos);
                    if (pos1 == string::npos) break;
                    size_t pos2 = line.find("]][", pos1 + 3);
                    if (pos2 == string::npos) break;
                    size_t pos3 = line.find("]][", pos2 + 3);
                    if (pos3 == string::npos) break;
                    size_t pos4 = line.find("]][", pos3 + 3);

                    KVStruct kv;
                    try {
                        kv.id = stoi(line.substr(current_pos, pos1 - current_pos));
                        kv.key = line.substr(pos1 + 3, pos2 - pos1 - 3);
                        kv.value = line.substr(pos2 + 3, pos3 - pos2 - 3);
                        if (pos4 != string::npos) {
                            kv.task_index = stoi(line.substr(pos3 + 3, pos4 - pos3 - 3));
                        } else {
                            kv.task_index = stoi(line.substr(pos3 + 3));
                        }
                    } catch (const std::exception& e) {
                        cerr << "Error parsing KV data: " << e.what() << endl;
                        cerr << "Line segment: " << line.substr(current_pos, pos4 == string::npos ? string::npos : pos4 - current_pos) << endl;
                        break;
                    }
                    if (current_task_.stateful) {
                        if (key_to_aggregate_.count(kv.key) == 0) {
                            key_to_aggregate_[kv.key] = 0;
                        }
                        key_to_aggregate_[kv.key] += kv.value;
                        kv.value = key_to_aggregate_[kv.key];
                    }

                    lock_guard<mutex> lock(acked_ids_mtx_);
                    if (kv.value == "FILTERED_OUT") {
                        new_filtered_ids_.insert(kv.id);
                        to_ack.push_back(kv.id);
                    } else {
                        new_processed_ids_.insert(kv.id);
                        kv.task_index = partitionData(kv.key, current_task_.task_count);
                        if (new_pending_acks[kv.task_index].data.empty()) {
                            new_pending_acks[kv.task_index].id = kv.id;
                        }
                        new_pending_acks[kv.task_index].data.push_back(kv);
                    }
                    current_pos = (pos4 == string::npos) ? line_length : pos4;
                }
            } catch (const std::exception& e) {
                cerr << "Error processing line: " << e.what() << endl;
                cerr << "Full line: " << line << endl;
                continue;
            }
        }

        if (!new_pending_acks.empty()) {
            persistNewIds();
            persistNewOutput(new_pending_acks);
            {
                std::lock_guard<std::mutex> lock(pending_ack_mtx_);
                for (const auto& pa : new_pending_acks) {
                    pending_acked_dict_[pa.id] = pa;
                }
            }
        }

        // Send acks for processed/filtered items
        if (!to_ack.empty() && !current_task_.ack_queue.empty()) {
            current_task_.ack_queue[0]->enqueue(to_ack);
        }

        // Send partitioned data to downstream nodes
        for (int i = 0; i < current_task_.task_count; i++) {
            if (!new_pending_acks[i].data.empty()) {
                new_pending_acks[i].time = std::chrono::steady_clock::now();
                current_task_.downstream_queue[i]->enqueue(new_pending_acks[i].data);
                new_pending_acks[i].data.clear();
            }
        }
    }
}

void RainStormNode::sendData(size_t downstream_node_index) {
    string downstream_address = current_task_.downstream_nodes[downstream_node_index];
    RainStormClient client(grpc::CreateChannel(downstream_address, grpc::InsecureChannelCredentials()));
    client.SendDataChunksStage(current_task_.downstream_queue[downstream_node_index], new_acked_ids_, acked_ids_mtx_);
}

RainStormNode::RainStormNode(const string& server_address)
    : should_stop_(false)
    , rainstorm_node_server_(server_address, this) {
}

int RainStormNode::partitionData(const string& key, size_t num_partitions) {
    return hash<string>{}(key) % num_partitions;
}