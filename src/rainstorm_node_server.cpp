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
#include "rainstorm_node.h"

using grpc::ServerBuilder;
using grpc::Status;
using grpc::ServerContext;
using grpc::ServerReaderWriter;

RainStormServer::RainStormServer(const std::string& server_address, INodeServerInterface* node_interface)
    : server_address_(server_address),
      node_interface_(node_interface),
      node_(nullptr),
      leader_node_(nullptr)
{
    ServerBuilder builder;
    grpc::ChannelArguments args;
    args.SetInt(GRPC_ARG_ALLOW_REUSEPORT, 1);
    builder.AddListeningPort(server_address_, grpc::InsecureServerCredentials());
    builder.AddChannelArgument(GRPC_ARG_ALLOW_REUSEPORT, 1);
    builder.RegisterService(this);
    server_ = builder.BuildAndStart();
    if (!server_) {
        cerr << "Failed to start server on " << server_address_ << endl;
        exit(1);
    }
    cout << "gRPC Server listening on " << server_address_ << endl;
}

RainStormServer::RainStormServer(RainStormNode* node)
    : node_interface_(node),
      node_(node),
      leader_node_(nullptr)
{
    // node-based constructor
}

RainStormServer::RainStormServer(RainStormLeader* leader)
    : node_interface_(nullptr),
      node_(nullptr),
      leader_node_(leader)
{
    // leader-based constructor
}

RainStormServer::~RainStormServer() {
    if (server_) server_->Shutdown();
}

void RainStormServer::wait() {
    if (server_) server_->Wait();
}

void RainStormServer::shutdown() {
    if (server_) server_->Shutdown();
}

Status RainStormServer::NewSrcTask(ServerContext* context,
                                   const rainstorm::NewSrcTaskRequest* request,
                                   rainstorm::OperationStatus* response) {
    (void)context; // unused
    std::lock_guard<std::mutex> lock(global_mtx_);
    std::cout << "NewSrcTask: " << request->job_id() << " " << request->src_filename() << std::endl;
    response->set_status(rainstorm::SUCCESS);
    return grpc::Status::OK;
}

Status RainStormServer::NewStageTask(ServerContext* context,
                                     const rainstorm::NewStageTaskRequest* request,
                                     rainstorm::OperationStatus* response) {
    (void)context; // unused
    std::lock_guard<std::mutex> lock(global_mtx_);
    if (node_) {
        node_->handleNewStageTask(request);
        response->set_status(rainstorm::SUCCESS);
    } else {
        response->set_status(rainstorm::INVALID);
        response->set_message("Node not initialized");
    }
    return grpc::Status::OK;
}

Status RainStormServer::UpdateTaskSnd(ServerContext* context,
                                      const rainstorm::UpdateTaskSndRequest* request,
                                      rainstorm::OperationStatus* response) {
    (void)context; // unused
    std::lock_guard<std::mutex> lock(global_mtx_);
    std::cout << "UpdateTaskSnd: index=" << request->index() << " addr=" << request->snd_address() << ":" << request->snd_port() << std::endl;
    response->set_status(rainstorm::SUCCESS);
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

void RainStormServer::SendDataChunksReader(ServerReaderWriter<rainstorm::AckDataChunk, rainstorm::StreamDataChunk>* stream) {
    rainstorm::StreamDataChunk chunk;
    while (stream->Read(&chunk)) {
        for (const auto& data_chunk : chunk.chunks()) {
            if (data_chunk.has_pair()) {
                KVStruct kv = protoToKVStruct(data_chunk.pair());
                if (node_interface_) {
                    node_interface_->enqueueIncomingData({kv});
                }
            }
            // Handle finished if needed
        }
    }
}

void RainStormServer::SendDataChunksWriter(ServerReaderWriter<rainstorm::AckDataChunk, rainstorm::StreamDataChunk>* stream) {
    while (true) {
        std::vector<int> acks;
        bool got_acks = false;
        if (node_interface_ && node_interface_->dequeueAcks(acks)) {
            got_acks = true;
        }

        if (got_acks) {
            rainstorm::AckDataChunk response_chunk;
            for (auto acked_id : acks) {
                response_chunk.add_id(acked_id);
            }
            if (!stream->Write(response_chunk)) {
                return;
            }
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
}

Status RainStormServer::SendDataChunks(ServerContext* context,
                                       ServerReaderWriter<rainstorm::AckDataChunk, rainstorm::StreamDataChunk>* stream) {
    (void)context; // unused
    std::thread reader_thread(&RainStormServer::SendDataChunksReader, this, stream);
    std::thread writer_thread(&RainStormServer::SendDataChunksWriter, this, stream);

    reader_thread.join();
    writer_thread.join();

    return Status::OK;
}

Status RainStormServer::SendDataChunksToLeader(ServerContext* context,
                                               ServerReaderWriter<rainstorm::AckDataChunk, rainstorm::StreamDataChunkLeader>* stream) {
    (void)context; // unused
    SafeQueue<std::vector<int>> ack_queue;
    std::atomic<bool> done_reading(false);

    std::string job_id;
    std::thread reader_thread([&]() {
        rainstorm::StreamDataChunkLeader stream_chunk;
        while (stream->Read(&stream_chunk)) {
            std::vector<int> ack_ids;
            std::vector<std::pair<std::string, std::string>> uniq_kvs;
            {
                std::lock_guard<std::mutex> lock(leader_node_->mtx_);
                for (const auto& data_chunk : stream_chunk.chunks()) {
                    if (data_chunk.has_job_id()) {
                        job_id = data_chunk.job_id();
                        std::cout << "Leader Received Job ID: " << job_id << std::endl;
                    }
                    if (data_chunk.has_pair()) {
                        const rainstorm::KV& kv = data_chunk.pair();
                        if (leader_node_->GetJobInfo(job_id).seen_kv_ids.find(kv.id()) != leader_node_->GetJobInfo(job_id).seen_kv_ids.end()) {
                            ack_ids.push_back(kv.id());
                            continue;
                        }
                        std::cout << kv.key() << ":" << kv.value() << "\n";
                        uniq_kvs.push_back({kv.key(), kv.value()});
                        ack_ids.push_back(kv.id());
                        leader_node_->GetJobInfo(job_id).seen_kv_ids.insert(kv.id());
                    }

                    if (data_chunk.has_finished()) {
                        std::cout << "Received 'finished' signal." << std::endl;
                    }
                }

                if (!uniq_kvs.empty()) {
                    std::ofstream ofs;
                    ofs.open(leader_node_->GetJobInfo(job_id).dest_file, std::ios::out | std::ios::app); 
                    for (const auto& kvp : uniq_kvs) {
                        ofs << kvp.first << ":" << kvp.second << "\n";
                    }
                    ofs.close();
                }
            }

            if (!ack_ids.empty()) {
                ack_queue.enqueue(std::move(ack_ids));
            }
        }

        done_reading = true;
        ack_queue.set_finished();
    });

    std::thread writer_thread([&]() {
        std::vector<int> acks; 
        while (ack_queue.dequeue(acks)) {
            rainstorm::AckDataChunk ack_chunk;
            for (auto id : acks) {
                ack_chunk.add_id(id);
            }

            if (!stream->Write(ack_chunk)) {
                std::cout << "Failed to write AckDataChunk to the stream." << std::endl;
                continue;
            }

            if (done_reading) {
                std::cout << "Write thread exiting as reading is done." << std::endl;
                return;
            }
        }
    });

    reader_thread.join();
    writer_thread.join();

    {
        std::lock_guard<std::mutex> lock(leader_node_->mtx_);
        if (leader_node_->GetJobInfo(job_id).num_completed_final_task == leader_node_->GetJobInfo(job_id).num_tasks_per_stage) {
            leader_node_->GetHydfs().createFile(leader_node_->GetJobInfo(job_id).dest_file, leader_node_->GetJobInfo(job_id).dest_file);
        } else {
            leader_node_->GetJobInfo(job_id).num_completed_final_task++;
        }
    }

    return grpc::Status::OK;
}
