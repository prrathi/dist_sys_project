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

// i think 8082 being used?
static const int GRPC_PORT_SERVER = 8083; 


using namespace std;

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;
using grpc::ServerReader;
using grpc::ServerWriter;


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

