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
#include "rainstorm_factory_server.h"
#include "rainstorm_leader.h"

using grpc::ServerBuilder;
using grpc::Status;
using grpc::ServerContext;
using grpc::ServerReaderWriter;
using rainstorm::NewSrcTaskRequest;
using rainstorm::NewStageTaskRequest;
using rainstorm::OperationStatus;
using rainstorm::StreamDataChunk;
using rainstorm::AckDataChunk;

RainStormServer::RainStormServer(RainstormFactory* factory, int server_port) 
    : factory_(factory), leader_(nullptr), server_port_(server_port)
{
    initializeServer();
}

RainStormServer::RainStormServer(RainStormLeader* leader, int server_port)
    : factory_(nullptr), leader_(leader), server_port_(server_port)
{
    initializeServer();
}

void RainStormServer::initializeServer() {
    char hostname[256];
    if (gethostname(hostname, sizeof(hostname)) != 0) {
        perror("gethostname");
        exit(1);
    }
    std::string hostname_str = hostname;
    server_address_ = hostname_str + ":" + std::to_string(server_port_);

    ServerBuilder builder;
    grpc::ChannelArguments args;
    args.SetInt(GRPC_ARG_ALLOW_REUSEPORT, 1);
    builder.AddListeningPort(server_address_, grpc::InsecureServerCredentials());
    builder.AddChannelArgument(GRPC_ARG_ALLOW_REUSEPORT, 1);
    builder.RegisterService(this);
    server_ = builder.BuildAndStart();
    if (!server_) {
        std::cerr << "Failed to start rainstorm server on " << server_address_ << std::endl;
        exit(1);
    }
    std::cout << "Rainstorm gRPC Server listening on " << server_address_ << std::endl;
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
    cout << "\n=== RainStormServer NewSrcTask Request ===" << endl;
    cout << "Peer: " << context->peer() << endl;
    cout << "Port: " << request->port() << endl;
    cout << "Job ID: " << request->job_id() << endl;
    cout << "Task Index: " << request->task_index() << endl;
    cout << "Task Count: " << request->task_count() << endl;
    cout << "Source File: " << request->src_filename() << endl;
    cout << "Send Address: " << request->snd_address() << endl;
    cout << "Send Port: " << request->snd_port() << endl;

    std::lock_guard<std::mutex> lock(global_mtx_);
    if (factory_) {
        cout << "NewSrcTask: inside factory for port: " << request->port() << "on node: " << server_address_ << endl;
        if (auto node = dynamic_cast<RainstormNodeSrc*>(factory_->getNode(request->port()))) {
            cout << "Found source node, handling task" << endl;
            node->handleNewSrcTask(request);
            response->set_status(rainstorm::SUCCESS);
        } else {
            cout << "Node not found or not a source node" << endl;
            response->set_status(rainstorm::INVALID);
            response->set_message("Node not found or not a source node");
        }
    } else if (leader_) {
        cout << "Leader not used here" << endl;
        response->set_status(rainstorm::INVALID);
        response->set_message("Leader not used here");
    } else {
        cout << "Server not properly initialized" << endl;
        response->set_status(rainstorm::INVALID);
        response->set_message("Server not properly initialized");
    }
    cout << "=== End NewSrcTask Request ===\n" << endl;
    return Status::OK;
}

Status RainStormServer::NewStageTask(ServerContext* context,
                                     const rainstorm::NewStageTaskRequest* request,
                                     rainstorm::OperationStatus* response) {
    cout << "\n=== RainStormServer NewStageTask Request ===" << endl;
    cout << "Peer: " << context->peer() << endl;
    cout << "Port: " << request->port() << endl;
    cout << "Job ID: " << request->job_id() << endl;
    cout << "Stage Index: " << request->stage_index() << endl;
    cout << "Task Index: " << request->task_index() << endl;
    cout << "Task Count: " << request->task_count() << endl;
    cout << "Executable: " << request->executable() << endl;
    cout << "Is Stateful: " << request->stateful() << endl;
    cout << "Is Last: " << request->last() << endl;
    cout << "Send Addresses: ";
    for (const auto& addr : request->snd_addresses()) {
        cout << addr << " ";
    }
    cout << endl;
    cout << "Send Ports: ";
    for (const auto& port : request->snd_ports()) {
        cout << port << " ";
    }
    cout << endl;

    std::lock_guard<std::mutex> lock(global_mtx_);
    if (factory_) {
        cout << "Looking up stage node for port " << request->port() << endl;
        if (auto node = dynamic_cast<RainstormNodeStage*>(factory_->getNode(request->port()))) {
            cout << "Found stage node, handling task" << endl;
            node->handleNewStageTask(request);
            response->set_status(rainstorm::SUCCESS);
        } else {
            cout << "Node not found or not a stage node" << endl;
            response->set_status(rainstorm::INVALID);
            response->set_message("Node not found or not a stage node");
        }
    } else if (leader_) {
        cout << "Leader not used here" << endl;
        response->set_status(rainstorm::INVALID);
        response->set_message("Leader not used here");
    } else {
        cout << "Server not properly initialized" << endl;
        response->set_status(rainstorm::INVALID);
        response->set_message("Server not properly initialized");
    }
    cout << "=== End NewStageTask Request ===\n" << endl;
    return Status::OK;
}

Status RainStormServer::UpdateTaskSnd(ServerContext* context,
                                      const rainstorm::UpdateTaskSndRequest* request,
                                      rainstorm::OperationStatus* response) {
    std::lock_guard<std::mutex> lock(global_mtx_);
    if (factory_) {
        if (auto node = factory_->getNode(request->port())) {
            node->handleUpdateTask(request);
            response->set_status(rainstorm::SUCCESS);
        } else {
            response->set_status(rainstorm::INVALID);
            response->set_message("Node not found");
        }
    } else if (leader_) {
        response->set_status(rainstorm::INVALID);
        response->set_message("Leader not used here");
    } else {
        response->set_status(rainstorm::INVALID);
        response->set_message("Server not properly initialized");
    }
    return Status::OK;
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

void RainStormServer::SendDataChunksReader(ServerReaderWriter<rainstorm::AckDataChunk, rainstorm::StreamDataChunk>* stream, int port) {
    rainstorm::StreamDataChunk chunk;
    while (stream->Read(&chunk)) {
        std::vector<KVStruct> batch;
        for (const auto& data_chunk : chunk.chunks()) {
            if (data_chunk.has_pair()) {
                KVStruct kv = protoToKVStruct(data_chunk.pair());
                batch.push_back(kv);
            }
        }
        if (!batch.empty()) {
            bool success = false;
            if (factory_) {
                if (auto node = dynamic_cast<RainstormNodeStage*>(factory_->getNode(port))) {
                    node->enqueueIncomingData(batch);
                    success = true;
                }
            } else if (leader_) {
                cerr << "Leader not used here" << endl; 
            }

            rainstorm::AckDataChunk ack;
            stream->Write(ack); // ??????????????
        }
    }
}

void RainStormServer::SendDataChunksWriter(ServerReaderWriter<rainstorm::AckDataChunk, rainstorm::StreamDataChunk>* stream, int task_index, int port) {
    while (true) {
        std::vector<int> acks;
        bool got_acks = false;
        if (factory_) {
            if (auto node = dynamic_cast<RainstormNodeStage*>(factory_->getNode(port))) {
                if (node->dequeueAcks(acks, task_index)) {
                    got_acks = true;
                }
            }
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
    cout << "never got here" << endl;
    if (!stream->Read(&initial_msg)) {
        std::cout << "Failed to read initial message" << std::endl;
        return Status(grpc::StatusCode::INVALID_ARGUMENT, "Failed to read initial message");
    }

    if (initial_msg.chunks_size() == 0 || 
        !initial_msg.chunks(0).has_port() || 
        !initial_msg.chunks(0).has_task_index()) {
        cout << "Initial message missing port or task_index" << endl;
        return Status(grpc::StatusCode::INVALID_ARGUMENT, "Initial message missing port or task_index");
    }
    int port = initial_msg.chunks(0).port();
    int task_index = initial_msg.chunks(0).task_index();
    if (factory_) {
        if (!factory_->getNode(port)) {
            cout << "Node not found for port: " << port << endl;
            return Status(grpc::StatusCode::NOT_FOUND, "Node not found for port: " + std::to_string(port));
        }
    }
    cout << "in server send data chunks" << endl;
    std::atomic<bool> is_done(false);
    std::mutex write_mutex;

    // Single Writer Thread
    std::thread writer_thread([&] {
        while (!is_done.load()) {
            std::vector<int> acks;
            bool got_acks = false;
            if (factory_) {
                if (auto node = dynamic_cast<RainstormNodeStage*>(factory_->getNode(port))) {
                    if (node->dequeueAcks(acks, task_index)) {
                        got_acks = true;
                    }
                }
            }

            if (got_acks) {
                rainstorm::AckDataChunk response_chunk;
                for (auto acked_id : acks) {
                    response_chunk.add_id(acked_id);
                }
                std::lock_guard<std::mutex> lock(write_mutex);
                if (!stream->Write(response_chunk)) {
                    // Handle write failure if necessary
                    break;
                }
            }

            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
    });

    // Reader Logic within the same thread
    cout << "got to reader logic" << endl;
    rainstorm::StreamDataChunk chunk;
    while (stream->Read(&chunk)) {
        cout << "Chunk read: " << chunk.chunks_size() << endl;
        std::vector<KVStruct> batch;
        bool finished = false;
        for (const auto& data_chunk : chunk.chunks()) {
            if (data_chunk.has_pair()) {
                KVStruct kv = protoToKVStruct(data_chunk.pair());
                batch.push_back(kv);
            }
            if (data_chunk.has_finished() && data_chunk.finished()) {
                finished = true;
            }
        }
        if (!batch.empty()) {
            if (factory_) {
                if (auto node = dynamic_cast<RainstormNodeStage*>(factory_->getNode(port))) {
                    node->enqueueIncomingData(batch);
                }
            } else if (leader_) {
                cerr << "Leader not used here" << endl; 
            }
        }
        if (finished) {
            break;
        }
    }

    // Signal writer thread to terminate
    is_done.store(true);
    writer_thread.join();
    cout << "should not have reached here" << endl;
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
            std::lock_guard<std::mutex> lock(leader_->getMutex());
            for (const auto& data_chunk : stream_chunk.chunks()) {
                if (data_chunk.has_pair()) {
                    const rainstorm::KV& kv = data_chunk.pair();
                    if (leader_->getJobInfo(job_id).seen_kv_ids.find(kv.id()) != leader_->getJobInfo(job_id).seen_kv_ids.end()) {
                        ack_ids.push_back(kv.id());
                        continue;
                    }
                    std::cout << kv.key() << ":" << kv.value() << "\n";
                    uniq_kvs.push_back({kv.key(), kv.value()});
                    ack_ids.push_back(kv.id());
                    leader_->getJobInfo(job_id).seen_kv_ids.insert(kv.id());
                }

                if (data_chunk.has_finished()) {
                    std::cout << "Received 'finished' signal." << std::endl;
                }
            }

            if (!uniq_kvs.empty()) {
                std::ofstream ofs;
                ofs.open(leader_->getJobInfo(job_id).dest_file, std::ios::out | std::ios::app); 
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
        {
            std::lock_guard<std::mutex> lock(leader_->getMutex());
            for (auto id : acks) {
                ack_chunk.add_id(id);
                processed_stream << id << "\n";
            }
            processed_stream.flush();
            leader_->getHydfs().appendFile(temp_file, processed_file);
            std::ofstream(temp_file, std::ios::trunc).close();

            if (!stream->Write(ack_chunk)) {
                std::cout << "Failed to write AckDataChunk to the stream." << std::endl;
                continue;
            }
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
    cout << "Data got to leader" << endl;
    rainstorm::StreamDataChunkLeader initial_msg;
    if (!stream->Read(&initial_msg)) {
        std::cout << "Failed to read initial message with job_id" << std::endl;
        return Status(grpc::StatusCode::INVALID_ARGUMENT, "Failed to read initial message with job_id");
    }
    if (initial_msg.chunks_size() == 0 || !initial_msg.chunks(0).has_job_id()) {
        std::cout << "Initial message missing job_id" << std::endl;
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
        std::lock_guard<std::mutex> lock(leader_->getMutex());
        if (leader_->getJobInfo(job_id).num_completed_final_task == 
            leader_->getJobInfo(job_id).num_tasks_per_stage) {
            leader_->getHydfs().createFile(leader_->getJobInfo(job_id).dest_file, leader_->getJobInfo(job_id).dest_file);
        } else {
            leader_->getJobInfo(job_id).num_completed_final_task++;
        }
    }

    return Status::OK;
}
