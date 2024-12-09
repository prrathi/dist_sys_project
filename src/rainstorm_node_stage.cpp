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
#include <pthread.h>

#include "rainstorm_node.h"
#include "rainstorm_service_client.h"

using namespace std;
using namespace chrono;

void RainstormNodeStage::handleNewStageTask(const rainstorm::NewStageTaskRequest* request) {
    lock_guard<mutex> lock(state_mtx_);
    
    job_id_ = request->job_id();
    stage_index_ = request->stage_index();
    task_index_ = request->task_index();
    task_count_ = request->task_count();
    operator_executable_ = request->executable();
    upstream_queue_ = make_shared<SafeQueue<vector<KVStruct>>>();
    stateful_ = request->stateful();
    last_ = request->last();
    if (stage_index_ == 0) {
        prev_task_count_ = 1;
    } else {
        prev_task_count_ = task_count_;
    }

    processed_file_ = job_id_ + "_" + to_string(stage_index_) + "_" + to_string(task_index_) + "_processed.log";
    filtered_file_ = job_id_ + "_" + to_string(stage_index_) + "_" + to_string(task_index_) + "_filtered.log";
    state_output_file_ = job_id_ + "_" + to_string(stage_index_) + "_" + to_string(task_index_) + "_output.log";

    if (!filesystem::exists(processed_file_)) {
        ofstream(processed_file_, ios::out | ios::trunc).close();
    }
    hydfs_.createFile(processed_file_, processed_file_);

    if (!filesystem::exists(filtered_file_)) {
        ofstream(filtered_file_, ios::out | ios::trunc).close();
    }
    hydfs_.createFile(filtered_file_, filtered_file_);

    if (!filesystem::exists(state_output_file_)) {
        ofstream(state_output_file_, ios::out | ios::trunc).close();
    }
    hydfs_.createFile(state_output_file_, state_output_file_);

    hydfs_.getFile(processed_file_, processed_file_, true);
    hydfs_.getFile(filtered_file_, filtered_file_, true);
    hydfs_.getFile(state_output_file_, state_output_file_, true);

    for (int i = 0; i < request->snd_addresses_size(); i++) {
        downstream_addresses_.push_back(request->snd_addresses(i));
        downstream_ports_.push_back(request->snd_ports(i));
        downstream_queues_.push_back(make_shared<SafeQueue<vector<KVStruct>>>());
    }
    for (int i = 0; i < prev_task_count_; i++) {
        ack_queues_.push_back(make_shared<SafeQueue<vector<int>>>());
    }

    downstream_queues_.resize(request->snd_addresses_size());
    downstream_addresses_.resize(request->snd_addresses_size());
    downstream_ports_.resize(request->snd_ports_size());
    ack_queues_.resize(request->snd_addresses_size());
    send_threads_.resize(request->snd_addresses_size());

    for (int i = 0; i < request->snd_addresses_size(); i++) {
        downstream_queues_[i] = make_shared<SafeQueue<vector<KVStruct>>>();
        ack_queues_[i] = make_shared<SafeQueue<vector<int>>>();
        downstream_addresses_[i] = request->snd_addresses(i);
        downstream_ports_[i] = request->snd_ports(i);
        send_threads_[i] = make_unique<thread>(&RainstormNodeStage::sendData, this, i);
    }

    process_thread_ = make_unique<thread>(&RainstormNodeStage::processData, this);
}

void RainstormNodeStage::handleUpdateTask(const rainstorm::UpdateTaskSndRequest* request) {
    lock_guard<mutex> lock(state_mtx_);
    
    size_t downstream_index = request->index();
    if (downstream_index >= send_threads_.size()) return;

    if (send_threads_[downstream_index]) {
        pthread_cancel(send_threads_[downstream_index]->native_handle());
    }

    downstream_addresses_[downstream_index] = request->snd_address();
    downstream_ports_[downstream_index] = request->snd_port();
    send_threads_[downstream_index] = make_unique<thread>(&RainstormNodeStage::sendData, this, downstream_index);
}

void RainstormNodeStage::enqueueIncomingData(const vector<KVStruct>& data) {
    lock_guard<mutex> lock(upstream_mtx_);
    if (upstream_queue_) {
        upstream_queue_->enqueue(data);
    }
}

bool RainstormNodeStage::dequeueAcks(vector<int>& acks, int task_index) {
    lock_guard<mutex> lock(acked_ids_mtx_);
    return ack_queues_[task_index]->dequeue(acks);
}

void RainstormNodeStage::checkPendingAcks() {
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

void RainstormNodeStage::retryPendingData(const PendingAck& pending) {
    if (pending.data.empty()) return;
    int partition = partitionData(pending.data[0].key, task_count_);
    cerr << "Retrying data for ID " << pending.data[0].id 
         << " to partition " << partition << endl;
    if (partition < (int)downstream_queues_.size()) {
        downstream_queues_[partition]->enqueue(pending.data);
    }
}

void RainstormNodeStage::loadIds(string filename, unordered_set<int>& ids) {
    lock_guard<mutex> lock(state_mtx_);
    ifstream processed_file(filename);
    string line;
    while (getline(processed_file, line)) {
        if(!line.empty()) ids.insert(stoi(line));
    }
}

void RainstormNodeStage::storeIds(string filename, unordered_set<int>& ids) {
    lock_guard<mutex> lock(state_mtx_);
    ofstream processed_file(filename);
    for (int id : ids) {
        processed_file << id << "\n";
    }
    processed_file.close();
}

void RainstormNodeStage::persistNewIds() {
    lock_guard<mutex> lock(state_mtx_);
    storeIds("temp_processed_ids.txt", new_processed_ids_);
    processed_ids_.merge(new_processed_ids_);
    hydfs_.appendFile("temp_processed_ids.txt", processed_file_);
    remove("temp_processed_ids.txt");

    storeIds("temp_filtered_ids.txt", new_filtered_ids_);
    processed_ids_.merge(new_filtered_ids_);
    hydfs_.appendFile("temp_filtered_ids.txt", processed_file_);
    hydfs_.appendFile("temp_filtered_ids.txt", filtered_file_);
    remove("temp_filtered_ids.txt");
}

void RainstormNodeStage::persistNewOutput(const vector<PendingAck>& new_pending_acks) {
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
    hydfs_.appendFile("temp_state_output.txt", state_output_file_);
    remove("temp_state_output.txt");
}

void RainstormNodeStage::recoverDataState() {
    // Load filtered IDs
    unordered_set<int> filtered_ids;
    loadIds(filtered_file_, filtered_ids);

    // Load next stage's processed IDs
    unordered_set<int> next_stage_processed;
    for (int task_index = 0; task_index < downstream_addresses_.size(); task_index++) {
        string next_stage_file = job_id_ + "_" + to_string(stage_index_ + 1) + "_" + to_string(task_index) + "_processed.log";
        string temp_file = "temp_" + next_stage_file;
        hydfs_.getFile(next_stage_file, temp_file, true);
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
    ifstream output_file(state_output_file_, ios::binary | ios::ate);
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
                        if (stateful_) {
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
        vector<vector<KVStruct>> partitioned_kvs((size_t)task_count_);
        for (auto kv : recovered_kvs) {
            if (stateful_) {
                int val = stoi(kv.value);
                key_to_aggregate_[kv.key] = val;
                kv.value = to_string(val);
            }
            if (kv.task_index >= 0 && kv.task_index < task_count_) {
                partitioned_kvs[(size_t)kv.task_index].push_back(kv);
            }
        }
        for (int i = 0; i < task_count_; i++) {
            if (!partitioned_kvs[i].empty()) {
                downstream_queues_[i]->enqueue(std::move(partitioned_kvs[i]));
            }
        }
    }
}

void RainstormNodeStage::enqueueAcks(const vector<vector<int>>& acks) {
    for (int i = 0; i < prev_task_count_; i++) {
        if (!acks.empty()) {
            ack_queues_[i]->enqueue(acks[i]);
        }
    }
}

void RainstormNodeStage::processData() {
    loadIds(processed_file_, processed_ids_);
    recoverDataState();
    vector<thread> sender_threads;
    for (int i = 0; i < task_count_; i++) {
        sender_threads.emplace_back(&RainstormNodeStage::sendData, this, i);
    }

    while (!should_stop_) {
        checkPendingAcks();

        vector<KVStruct> data;
        {
            lock_guard<mutex> lock(state_mtx_);
            upstream_queue_->dequeue(data);
        }

        if (!data.empty()) {
            cout << "Processing data for stage " << stage_index_ << " task " << task_index_ << endl;
            string input_data;
            vector<vector<int>> to_ack(prev_task_count_);

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
                input_data += "]][" + to_string(kv.id) + 
                             "]][" + kv.key + 
                             "]][" + escaped_value + 
                             "]][" + to_string(kv.task_index);
            }

            if (input_data.empty()) {
                enqueueAcks(to_ack); 
                continue;
            }
            string command = "echo \"" + input_data + "\" | " + operator_executable_;
            
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
            vector<PendingAck> new_pending_acks((size_t)task_count_);

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
                        kv.task_index = task_index_;

                        if (stateful_) {
                            int val = stoi(kv.value);
                            key_to_aggregate_[kv.key] += val;
                            kv.value = to_string(key_to_aggregate_[kv.key]);
                        }

                        if (kv.value == "FILTERED_OUT") {
                            new_filtered_ids_.insert(kv.id);
                            to_ack[kv.task_index].push_back(kv.id);
                        } else {
                            new_processed_ids_.insert(kv.id);
                            int partition = partitionData(kv.key, task_count_);
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
            for (int i = 0; i < task_count_; i++) {
                if (!new_pending_acks[(size_t)i].data.empty()) {
                    downstream_queues_[(size_t)i]->enqueue(new_pending_acks[(size_t)i].data);
                }
            }

            enqueueAcks(to_ack);

            // Check if all previous stage tasks are complete
            vector<int> prev_stage_indexs;
            if (stage_index_ == 1) {
                prev_stage_indexs = {task_index_};
            } else {
                for (int prev_task = 0; prev_task < task_count_; prev_task++) {
                    prev_stage_indexs.push_back(prev_task);
                }
            }
            bool all_prev_complete = true;
            for (int prev_task : prev_stage_indexs) {
                string fin_file = job_id_ + "_" + to_string(stage_index_ - 1) + "_" + to_string(prev_task) + "_fin.log";
                string temp_fin = "temp_" + fin_file;
                
                hydfs_.getFile(fin_file, temp_fin, true);
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
                for (int next_task = 0; next_task < task_count_; next_task++) {
                    string next_processed_file = job_id_ + "_" + to_string(stage_index_ + 1) + "_" + to_string(next_task) + "_processed.log";
                    string temp_next = "temp_" + next_processed_file;
                    
                    hydfs_.getFile(next_processed_file, temp_next, true);
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
                    string our_fin = job_id_ + "_" + 
                                    to_string(stage_index_) + "_" + 
                                    to_string(task_index_) + "_fin.log";
                    ofstream fin_out("temp_fin.log");
                    fin_out << "1" << endl;
                    fin_out.close();
                    hydfs_.createFile("temp_fin.log", our_fin);
                    filesystem::remove("temp_fin.log");
                    
                    should_stop_ = true;
                }
            }
        }
        this_thread::sleep_for(milliseconds(100));
    }
    
    for (auto& thread : sender_threads) {
        thread.join();
    }
}

void RainstormNodeStage::sendData(size_t downstream_node_index) {
    pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, nullptr);
    pthread_setcanceltype(PTHREAD_CANCEL_ASYNCHRONOUS, nullptr);

    RainStormClient client(grpc::CreateChannel(
        downstream_addresses_[downstream_node_index] + ":" + 
        to_string(downstream_ports_[downstream_node_index]), 
        grpc::InsecureChannelCredentials()
    ));
    
    client.SendDataChunks(
        downstream_ports_[downstream_node_index],
        downstream_queues_[downstream_node_index],
        new_acked_ids_,
        acked_ids_mtx_,
        task_index_
    );
}
