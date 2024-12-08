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

RainStormServer::RainStormServer(RainStormNode* node)
    : node_(node),
      leader_node_(nullptr)
{
    // node-based constructor
}

RainStormServer::RainStormServer(RainStormLeader* leader)
    : node_(nullptr),
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
                if (node_) {
                    node_->enqueueIncomingData({kv});
                }
            }
        }
    }
}

void RainStormServer::SendDataChunksWriter(ServerReaderWriter<rainstorm::AckDataChunk, rainstorm::StreamDataChunk>* stream, int task_index) {
    while (true) {
        std::vector<int> acks;
        bool got_acks = false;
        if (node_ && node_->dequeueAcks(acks, task_index)) {
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
    rainstorm::StreamDataChunk initial_msg;
    if (!stream->Read(&initial_msg)) {
        return Status(grpc::StatusCode::INVALID_ARGUMENT, "Failed to read initial message with task_index");
    }
    if (initial_msg.chunks_size() == 0 || !initial_msg.chunks(0).has_task_index()) {
        return Status(grpc::StatusCode::INVALID_ARGUMENT, "Initial message missing task_index");
    }
    int task_index = initial_msg.chunks(0).task_index();
    
    std::thread reader_thread(&RainStormServer::SendDataChunksReader, this, stream);
    std::thread writer_thread(&RainStormServer::SendDataChunksWriter, this, stream, task_index);

    reader_thread.join();
    writer_thread.join();

    return Status::OK;
}

void RainStormServer::SendDataChunksLeaderReader(
    ServerReaderWriter<rainstorm::AckDataChunk, rainstorm::StreamDataChunkLeader>* stream,
    SafeQueue<std::vector<int>>& ack_queue,
    std::atomic<bool>& done_reading,
    const std::string job_id) {
    
    rainstorm::StreamDataChunkLeader stream_chunk;
    while (stream->Read(&stream_chunk)) {
        std::vector<int> ack_ids;
        std::vector<std::pair<std::string, std::string>> uniq_kvs;
        {
            std::lock_guard<std::mutex> lock(leader_node_->mtx_);
            for (const auto& data_chunk : stream_chunk.chunks()) {
                if (data_chunk.has_pair()) {
                    const rainstorm::KV& kv = data_chunk.pair();
                    if (leader_node_->getJobInfo(job_id).seen_kv_ids.find(kv.id()) != leader_node_->getJobInfo(job_id).seen_kv_ids.end()) {
                        ack_ids.push_back(kv.id());
                        continue;
                    }
                    std::cout << kv.key() << ":" << kv.value() << "\n";
                    uniq_kvs.push_back({kv.key(), kv.value()});
                    ack_ids.push_back(kv.id());
                    leader_node_->getJobInfo(job_id).seen_kv_ids.insert(kv.id());
                }

                if (data_chunk.has_finished()) {
                    std::cout << "Received 'finished' signal." << std::endl;
                }
            }

            if (!uniq_kvs.empty()) {
                std::ofstream ofs;
                ofs.open(leader_node_->getJobInfo(job_id).dest_file, std::ios::out | std::ios::app); 
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
}

void RainStormServer::SendDataChunksLeaderWriter(
    ServerReaderWriter<rainstorm::AckDataChunk, rainstorm::StreamDataChunkLeader>* stream,
    SafeQueue<std::vector<int>>& ack_queue,
    std::atomic<bool>& done_reading,
    const std::string job_id) {
    
    std::vector<int> acks;
    std::string processed_file = job_id + "_3_0_processed.log";
    std::string temp_file = "temp_" + processed_file;
    std::ofstream processed_stream(temp_file, std::ios::app);
    
    while (ack_queue.dequeue(acks)) {
        rainstorm::AckDataChunk ack_chunk;
        for (auto id : acks) {
            ack_chunk.add_id(id);
            processed_stream << "ID:" << id << std::endl;
        }
        processed_stream.flush();
        leader_node_->getHydfs().appendFile(temp_file, processed_file);
        std::ofstream(temp_file, std::ios::trunc).close();

        if (!stream->Write(ack_chunk)) {
            std::cout << "Failed to write AckDataChunk to the stream." << std::endl;
            continue;
        }

        if (done_reading) {
            std::cout << "Write thread exiting as reading is done." << std::endl;
            processed_stream.close();
            std::filesystem::remove(temp_file);
            return;
        }
    }
    
    processed_stream.close();
    std::filesystem::remove(temp_file);
}

Status RainStormServer::SendDataChunksToLeader(ServerContext* context,
                                              ServerReaderWriter<rainstorm::AckDataChunk, rainstorm::StreamDataChunkLeader>* stream) {
    SafeQueue<std::vector<int>> ack_queue;
    std::atomic<bool> done_reading(false);
    
    rainstorm::StreamDataChunkLeader initial_msg;
    if (!stream->Read(&initial_msg)) {
        return Status(grpc::StatusCode::INVALID_ARGUMENT, "Failed to read initial message with job_id");
    }
    if (initial_msg.chunks_size() == 0 || !initial_msg.chunks(0).has_job_id()) {
        return Status(grpc::StatusCode::INVALID_ARGUMENT, "Initial message missing job_id");
    }
    std::string job_id = initial_msg.chunks(0).job_id();

    std::thread reader_thread(&RainStormServer::SendDataChunksLeaderReader, 
                            this, stream, std::ref(ack_queue), 
                            std::ref(done_reading), job_id);
    std::thread writer_thread(&RainStormServer::SendDataChunksLeaderWriter,
                            this, stream, std::ref(ack_queue),
                            std::ref(done_reading), job_id);

    reader_thread.join();
    writer_thread.join();

    {
        std::lock_guard<std::mutex> lock(leader_node_->mtx_);
        if (leader_node_->getJobInfo(job_id).num_completed_final_task == 
            leader_node_->getJobInfo(job_id).num_tasks_per_stage) {
            leader_node_->getHydfs().createFile(leader_node_->getJobInfo(job_id).dest_file, leader_node_->getJobInfo(job_id).dest_file);
        } else {
            leader_node_->getJobInfo(job_id).num_completed_final_task++;
        }
    }

    return Status::OK;
}
