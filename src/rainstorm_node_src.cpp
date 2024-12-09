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

void RainstormNodeSrc::handleNewSrcTask(const rainstorm::NewSrcTaskRequest* request) {
    lock_guard<mutex> lock(state_mutex_);
    
    job_id_ = request->job_id();
    task_index_ = request->task_index();
    task_count_ = request->task_count();
    input_file_ = request->src_filename();
    downstream_address_ = request->snd_address();
    downstream_port_ = request->snd_port();
    processed_file_ = job_id_ + "_0_" + to_string(task_index_) + "_processed.log";
    next_processed_file_ = job_id_ + "_1_" + to_string(task_index_) + "_processed.log";
    downstream_queue_ = make_shared<SafeQueue<vector<KVStruct>>>();

    process_thread_ = make_unique<thread>(&RainstormNodeSrc::processData, this);
    send_thread_ = make_unique<thread>(&RainstormNodeSrc::sendData, this);
}

void RainstormNodeSrc::handleUpdateTask(const rainstorm::UpdateTaskSndRequest* request) {
    lock_guard<mutex> lock(state_mutex_);
    if (send_thread_) {
        pthread_cancel(send_thread_->native_handle());
    }
    downstream_address_ = request->snd_address();
    downstream_port_ = request->snd_port();
    send_thread_ = make_unique<thread>(&RainstormNodeSrc::sendData, this);
}

void RainstormNodeSrc::loadIds(string filename, unordered_set<int>& ids) {
    lock_guard<mutex> lock(state_mutex_);
    ifstream processed_file(filename);
    string line;
    while (getline(processed_file, line)) {
        if(!line.empty()) ids.insert(stoi(line));
    }
}

void RainstormNodeSrc::persistNewOutput(const vector<KVStruct>& batch) {
    if (batch.empty()) return;
    ofstream temp_ids("temp_processed_ids.txt");
    for (const auto& kv : batch) {
        temp_ids << kv.id << "\n";
    }
    temp_ids.close();
    hydfs_.appendFile("temp_processed_ids.txt", processed_file_);
    remove("temp_processed_ids.txt");
}

void RainstormNodeSrc::processData() {
    cout << "Processing data called at src: " << input_file_ << file_read_ << endl;
    unordered_set<int> next_ids;
    string temp_next = "temp_" + next_processed_file_;
    hydfs_.getFile(temp_next, next_processed_file_, true);
    if (filesystem::exists(temp_next)) {
        cout << "Loading next ids" << endl;
        loadIds(temp_next, next_ids);
        filesystem::remove(temp_next);
    }

    if (!file_read_) {
        string temp_input = "temp_" + input_file_;
        cout << "temp_input: " << temp_input << endl;
        //hydfs_.getFile(input_file_, temp_input, true); fuck me
        hydfs_.getFile(temp_input, input_file_, true);
        cout << "temp_input: " << temp_input << " " << input_file_ << endl;
        ifstream file(temp_input);
        if (!file.is_open()) {
            cout << "Failed to open file: " << temp_input << endl;
            return;
        }

        string line;
        int id = 0;
        
        vector<KVStruct> batch;
        while (getline(file, line)) {
            if (line.empty()) continue;
            
            KVStruct kv;
            kv.id = id++;
            if (next_ids.find(kv.id) != next_ids.end()) {
                continue;
            }
            kv.key = to_string(kv.id);
            kv.value = line;
            kv.task_index = task_index_;
            
            int partition = partitionData(kv.key, task_count_);
            if (partition == task_index_) {
                batch.push_back(kv);
                if (batch.size() >= 100) {
                    PendingAck pending;
                    pending.timestamp = steady_clock::now();
                    pending.data = batch;
                    pending.task_index = task_index_;
                    
                    {
                        lock_guard<mutex> lock(pending_ack_mtx_);
                        pending_acked_dict_[batch.front().id] = pending;
                    }
                    
                    persistNewOutput(batch);
                    cout << "enqueuing batch 1" << batch.size() << endl;
                    downstream_queue_->enqueue(batch);
                    batch.clear();
                }
            }
        }
        cout << "batch size: " << batch.size() << endl;
        if (!batch.empty()) {
            PendingAck pending;
            pending.timestamp = steady_clock::now();
            pending.data = batch;
            pending.task_index = task_index_;
            
            {
                lock_guard<mutex> lock(pending_ack_mtx_);
                pending_acked_dict_[batch.front().id] = pending;
            }
            
            persistNewOutput(batch);
            cout << "enqueuing batch 2" << batch.size() << endl;
            downstream_queue_->enqueue(batch);
        }
        
        file.close();
        filesystem::remove(temp_input);
        file_read_ = true;
    }

    while (!should_stop_) {
        auto now = steady_clock::now();
        vector<KVStruct> to_retry;
        {
            lock_guard<mutex> lock(pending_ack_mtx_);
            for (auto it = pending_acked_dict_.begin(); it != pending_acked_dict_.end();) {
                if (now - it->second.timestamp > ACK_TIMEOUT) {
                    if (!isAcked(it->first)) {
                        to_retry.insert(to_retry.end(), 
                                      it->second.data.begin(), 
                                      it->second.data.end());
                    }
                    it = pending_acked_dict_.erase(it);
                } else {
                    ++it;
                }
            }
        }

        if (!to_retry.empty()) {
            PendingAck pending;
            pending.timestamp = steady_clock::now();
            pending.data = to_retry;
            pending.task_index = task_index_;
            
            {
                lock_guard<mutex> lock(pending_ack_mtx_);
                pending_acked_dict_[to_retry.front().id] = pending;
            }
            cout << "enqueuing batch 3" << to_retry.size() << endl;
            downstream_queue_->enqueue(to_retry);
        }

        hydfs_.getFile(temp_next, next_processed_file_, true);
        next_ids.clear();
        if (filesystem::exists(temp_next)) {
            loadIds(temp_next, next_ids);
            filesystem::remove(temp_next);
            
            bool all_processed = true;
            {
                lock_guard<mutex> lock(state_mutex_);
                for (int id : acked_ids_) {
                    if (next_ids.find(id) == next_ids.end()) {
                        all_processed = false;
                        break;
                    }
                }
            }

            if (all_processed && file_read_) {
                string our_fin = job_id_ + "_0_" + to_string(task_index_) + "_fin.log";
                ofstream fin_out("temp_fin.log");
                fin_out << "1" << endl;
                fin_out.close();
                hydfs_.createFile("temp_fin.log", our_fin);
                filesystem::remove("temp_fin.log");
                
                should_stop_ = true;
            }
        }

        this_thread::sleep_for(milliseconds(500));
    }
    
    send_thread_->join();
}

void RainstormNodeSrc::sendData() {
    // Enable cancellation
    pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, nullptr);
    pthread_setcanceltype(PTHREAD_CANCEL_ASYNCHRONOUS, nullptr);

    while (!should_stop_) {
        RainStormClient client(grpc::CreateChannel(
            downstream_address_ + ":" + to_string(downstream_port_), 
            grpc::InsecureChannelCredentials()
        ));
        cout << "Source Sending data to downstream node" << downstream_address_ + ":" + to_string(downstream_port_) << endl;
        client.SendDataChunks(
            downstream_port_, 
            downstream_queue_, 
            acked_ids_, 
            pending_ack_mtx_, 
            task_index_
        );

        if (!should_stop_) {
            this_thread::sleep_for(chrono::milliseconds(100));
        }
    }
}

bool RainstormNodeSrc::isAcked(int id) {
    lock_guard<mutex> lock(state_mutex_);
    return acked_ids_.find(id) != acked_ids_.end();
}
