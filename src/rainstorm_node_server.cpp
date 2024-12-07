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

static const int GRPC_PORT_SERVER = 8083;

using namespace std;
namespace fs = std::filesystem;

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;
using grpc::ServerReader;
using grpc::ServerWriter;
using grpc::ServerReaderWriter;
using rainstorm::StatusCode;
using rainstorm::OperationStatus;
using rainstorm::NewSrcTaskRequest;
using rainstorm::NewStageTaskRequest;
using rainstorm::UpdateTaskSndRequest;
using rainstorm::AckDataChunk;
using rainstorm::StreamDataChunk;



RainStormServer::RainStormServer(RainStormNode* node) : node_(node) {};
RainStormServer::RainStormServer(RainStormLeader* leader) : leader_node_(leader) {};

RainStormServer::RainStormServer() {
    char hostname[256];
    if (gethostname(hostname, sizeof(hostname)) != 0) {
        perror("gethostname");
        exit(1);
    }
    ServerBuilder builder;
    builder.AddListeningPort(server_address_, grpc::InsecureServerCredentials());
    builder.RegisterService(this);
    server_ = builder.BuildAndStart();
}

RainStormServer::~RainStormServer() {
    if (server_) server_->Shutdown();
}

void RainStormServer::wait() {
    if (server_) server_->Wait();
}

Status RainStormServer::NewSrcTask(ServerContext* context,
                                          const NewSrcTaskRequest* request,
                                          OperationStatus* response) {
    lock_guard<mutex> lock(global_mtx_);
    cout << "NewSrcTask: " << request->query_id() << " " << request->src_filename() << endl;
    response->set_status(StatusCode::SUCCESS);
    return grpc::Status::OK;
}

grpc::Status RainStormServer::NewStageTask(grpc::ServerContext* context,
                                            const NewStageTaskRequest* request,
                                            OperationStatus* response) {
    if (node_) {
        lock_guard<mutex> lock(global_mtx_);
        node_->handleNewStageTask(request);
        response->set_status(rainstorm::StatusCode::SUCCESS);
    } else {
        response->set_status(rainstorm::StatusCode::INVALID);
        response->set_message("Node not initialized");
    }
    return grpc::Status::OK;
}

grpc::Status RainStormServer::UpdateTaskSnd(grpc::ServerContext* context,
                                           const UpdateTaskSndRequest* request,
                                           OperationStatus* response) {
    lock_guard<mutex> lock(global_mtx_);
    cout << "UpdateTaskSnd: index=" << request->index() << " addr=" << request->snd_address() << ":" << request->snd_port() << endl;
    response->set_status(StatusCode::SUCCESS);
    return grpc::Status::OK;
}

KVStruct RainStormServer::protoToKVStruct(const rainstorm::KV& proto_kv) {
    KVStruct kv;
    kv.id = proto_kv.id();
    kv.key = proto_kv.key();
    kv.value = proto_kv.value();
    kv.task_index = proto_kv.task_index();
    return kv;
}

rainstorm::KV RainStormServer::kvStructToProto(const KVStruct& kv) {
    rainstorm::KV proto_kv;
    proto_kv.set_id(kv.id);
    proto_kv.set_key(kv.key);
    proto_kv.set_value(kv.value);
    proto_kv.set_task_index(kv.task_index);
    return proto_kv;
}

void RainStormServer::SendDataChunksReader(grpc::ServerReaderWriter<AckDataChunk, StreamDataChunk>* stream) {
    StreamDataChunk chunk;
    while (stream->Read(&chunk)) {
        if (!chunk.has_pair()) continue;

        KVStruct kv = protoToKVStruct(chunk.pair());
        if (node_) {
            node_->enqueueIncomingData({kv});
        }
    }
}

void RainStormServer::SendDataChunksWriter(grpc::ServerReaderWriter<AckDataChunk, StreamDataChunk>* stream) {
    while (true) {
        vector<int> acks;
        bool got_acks = false;
        
        // Try to get acks from the task
        if (node_ && node_->dequeueAcks(acks)) {
            got_acks = true;
        }
        
        if (got_acks) {
            AckDataChunk response_chunk;
            for (const auto& acked_id : acks) {
                DataChunk* chunk = response_chunk.add_chunks();
                chunk->set_id(acked_id);
            }
            if (!stream->Write(response_chunk)) {
                return;
            }
        }
        
        this_thread::sleep_for(chrono::milliseconds(10));
    }
}

Status RainStormServer::SendDataChunks(ServerContext* context,
                                     ServerReaderWriter<AckDataChunk, StreamDataChunk>* stream) {
    thread reader_thread(&RainStormServer::SendDataChunksReader, this, stream);
    thread writer_thread(&RainStormServer::SendDataChunksWriter, this, stream);
    
    reader_thread.join();
    writer_thread.join();
    
    return Status::OK;
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
