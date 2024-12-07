#include <iostream>
#include <fstream>
#include <filesystem>
#include <chrono>
#include <iomanip>
#include <sstream>
#include <random>
#include <thread>
#include <grpcpp/grpcpp.h>
#include "rainstorm_node_server.h"
#include "rainstorm_leader.h"

// i think 8082 being used?
static const int GRPC_PORT_SERVER = 8083; 


using namespace std;

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;
using grpc::ServerReader;
using grpc::ServerWriter;



RainStormServer::RainStormServer(RainStormNode* node) : node_(node) {};
RainStormServer::RainStormServer(RainStormLeader* leader) : leader_node_(leader) {};

RainStormServer::RainStormServer() {
    char hostname[256];
    if (gethostname(hostname, sizeof(hostname)) != 0) {
        perror("gethostname");
        exit(1);
    }
    string hostname_str = hostname;
    server_address_ = hostname_str + ":" + to_string(GRPC_PORT_SERVER);

    ServerBuilder builder;
    builder.AddListeningPort(server_address_, grpc::InsecureServerCredentials());
    builder.RegisterService(this);
    server_ = builder.BuildAndStart();
}

RainStormServer::~RainStormServer() {
    if (server_) {
        server_->Shutdown();
    }
}

void RainStormServer::wait() {
    if (server_) server_->Wait();
}

grpc::Status RainStormServer::NewSrcTask(grpc::ServerContext* context,
                                          const rainstorm::NewSrcTaskRequest* request,
                                          rainstorm::OperationStatus* response) {
    std::lock_guard<std::mutex> lock(global_mtx_);
    std::cout << "NewSrcTask: " << request->id() << " " << request->src_filename() << std::endl;
    response->set_status(rainstorm::SUCCESS);
    response->set_message("Source task created");
    return grpc::Status::OK;
}

grpc::Status RainStormServer::NewStageTask(grpc::ServerContext* context,
                                            const rainstorm::NewStageTaskRequest* request,
                                            rainstorm::OperationStatus* response) {

    std::lock_guard<std::mutex> lock(global_mtx_); //?
    std::cout << "NewStageTask: " << request->id() << " next: " << request->next_server_addresses().size() << " prev: " << request->prev_server_addresses().size() << std::endl;
    node_->HandleNewStageTask(request);
    response->set_status(rainstorm::SUCCESS);
    response->set_message("Stage task created");
    return grpc::Status::OK;
}

grpc::Status RainStormServer::UpdateSrcTaskSend(grpc::ServerContext* context,
                                                 const rainstorm::UpdateSrcTaskSendRequest* request,
                                                 rainstorm::OperationStatus* response) {
    std::lock_guard<std::mutex> lock(global_mtx_);
    std::cout << "UpdateSrcTaskSend: " << request->id() << " new_next: " << request->new_next_server_address() << std::endl;
    response->set_status(rainstorm::SUCCESS);
    return grpc::Status::OK;
}

grpc::Status RainStormServer::UpdateDstTaskRecieve(grpc::ServerContext* context,
                                                    const rainstorm::UpdateDstTaskRecieveRequest* request,
                                                    rainstorm::OperationStatus* response) {
    std::lock_guard<std::mutex> lock(global_mtx_);
    std::cout << "UpdateDstTaskRecieve: " << request->id() << " new_next: " << request->new_next_server_address() << " new_prev: " << request->new_prev_server_address() << std::endl;
    response->set_status(rainstorm::SUCCESS);
    return grpc::Status::OK;
}

void RainStormServer::SendDataChunksReader(grpc::ServerReaderWriter<rainstorm::StreamDataChunk, rainstorm::StreamDataChunk>* stream) {
    rainstorm::StreamDataChunk chunk;
    std::string task_id;
    bool got_id = false;
    while (stream->Read(&chunk)) {
        if (!got_id && chunk.has_id()) {
            task_id = chunk.id();
            got_id = true;
            std::cout << "SendDataChunksReader start for task: " << task_id << std::endl;
        } else {
            for (auto &p : chunk.pairs()) {
                std::cout << "Received kv pair: " << p.key() << " = " << p.value() << std::endl;
                node_->EnqueueToBeProcessed(task_id, p);
            }
        }
    }
}

void RainStormServer::SendDataChunksWriter(grpc::ServerReaderWriter<rainstorm::StreamDataChunk, rainstorm::StreamDataChunk>* stream) {
    rainstorm::StreamDataChunk response_chunk;
    while (true) {
        std::string task_id;
        std::vector<std::pair<std::string, std::string>> acked_ids;
        if (node_->DequeueProcessed(task_id, acked_ids)) {
            if (!task_id.empty()) {
                response_chunk.set_id(task_id);
                for (auto &a : acked_ids) {
                    auto p = response_chunk.add_pairs();
                    p->set_key(a.first);
                    p->set_value(a.second);
                }
                stream->Write(response_chunk);
            }
        } else {
            break;
        }
    }
}

grpc::Status RainStormServer::SendDataChunks(grpc::ServerContext* context,
                                              grpc::ServerReaderWriter<rainstorm::StreamDataChunk, rainstorm::StreamDataChunk>* stream) {
    std::thread reader_thread(&RainStormServer::SendDataChunksReader, this, stream);
    std::thread writer_thread(&RainStormServer::SendDataChunksWriter, this, stream);
    reader_thread.join();
    writer_thread.join();
    return grpc::Status::OK;
}

grpc::Status RainStormServer::SendDataChunksToLeader(grpc::ServerContext* context,
                                                      grpc::ServerReaderWriter<rainstorm::AckDataChunk, rainstorm::StreamDataChunkLeader>* stream) {
    SafeQueue<std::vector<int>> ack_queue;
    std::atomic<bool> done_reading(false);

    std::string job_id;
    std::thread reader_thread([&]() {
        rainstorm::StreamDataChunkLeader stream_chunk;
        while (stream->Read(&stream_chunk)) {
            std::vector<int> ack_ids;
            std::vector<pair<string, string>> uniq_kvs;
            std::lock_guard<std::mutex> lock(leader_node_->mtx_); // lock for entire loop
            for (const auto& data_chunk : stream_chunk.chunks()) {
                if (data_chunk.has_job_id()) {
                    std::cout << "Leader Recieved Job ID " << data_chunk.job_id() << std::endl;
                    job_id = data_chunk.job_id();
                }
                if (data_chunk.has_pair()) {
                    const rainstorm::KV& kv = data_chunk.pair();

                    // convert ID to string for consistent storage
                    // check if the ID has already been processed
                    if (leader_node_->GetJobInfo(job_id).seen_kv_ids.find(kv.id()) != leader_node_->GetJobInfo(job_id).seen_kv_ids.end()) {
                        ack_ids.push_back(kv.id());
                        continue;
                    }

                    // print out to main console and store
                    std::cout << kv.key() << ":" <<  kv.value() << "\n";
                    uniq_kvs.push_back({kv.key(), kv.value()});
                    ack_ids.push_back(kv.id());
                    leader_node_->GetJobInfo(job_id).seen_kv_ids.insert(kv.id());
                }
    
                if (data_chunk.has_finished()) {
                    std::cout << "Received 'finished' signal." << std::endl;
                    return;
                }
            }
            if (!uniq_kvs.empty()) {
                lock_guard<mutex> lock(leader_node_->mtx_);
                ofstream ofs;
                ofs.open(leader_node_->GetJobInfo(job_id).dest_file, ios::out | ios::app); 
                for (const auto& [key, value] : uniq_kvs) {
                    ofs << key << ":" << value << "\n";
                }
                ofs.close();
            }
            // if we got some data store.
            if (!ack_ids.empty()) {
                ack_queue.enqueue(std::move(ack_ids));
            }
        }

        // Indicate that reading is done
        done_reading = true;
        ack_queue.set_finished();
    });

    // Write Thread
    std::thread writer_thread([&]() {
        vector<int> acks; 
        while (ack_queue.dequeue(acks)) {
            rainstorm::AckDataChunk ack_chunk;
            for (const auto& id : acks) {
                ack_chunk.add_id(id);
            }

            // Write the AckDataChunk back to the client
            if (!stream->Write(ack_chunk)) {
                std::cout << "Failed to write AckDataChunk to the stream." << std::endl;
                continue;
            }
            
            // received finished signal from // 
            if (done_reading) {
                std::cout << "Write thread exiting as reading is done." << std::endl;
                return;
            }
        }

    });

    // Wait for both threads to finish
    reader_thread.join();
    writer_thread.join();

    // write the final file to hydfs when all streams have finished sending data
    {
        lock_guard<mutex> lock(leader_node_->mtx_);
        if (leader_node_->GetJobInfo(job_id).num_completed_final_task == leader_node_->GetJobInfo(job_id).num_tasks_per_stage) {
            leader_node_->GetHydfs().createFile(leader_node_->GetJobInfo(job_id).dest_file, leader_node_->GetJobInfo(job_id).dest_file);
        } else {
            leader_node_->GetJobInfo(job_id).num_completed_final_task++;
        }
    }

    // Finalize the stream
    return grpc::Status::OK;
}