#include <grpcpp/grpcpp.h>
#include "rainstorm_node_server.h"
#include <iostream>

using namespace std;

RainStormServer::RainStormServer(const std::string& server_address)
    : server_address_(server_address) {
    grpc::ServerBuilder builder;
    builder.AddListeningPort(server_address_, grpc::InsecureServerCredentials());
    builder.RegisterService(this);
    server_ = builder.BuildAndStart();
    cout << "Server listening on " << server_address_ << endl;
}

RainStormServer::~RainStormServer() {
    if (server_) server_->Shutdown();
}

void RainStormServer::wait() {
    if (server_) server_->Wait();
}

grpc::Status RainStormServer::NewSrcTask(grpc::ServerContext* context,
                                        const rainstorm::NewSrcTaskRequest* request,
                                        rainstorm::OperationStatus* response) {
    lock_guard<mutex> lock(global_mtx_);
    cout << "NewSrcTask: " << request->query_id() << " " << request->src_filename() << endl;
    response->set_status(rainstorm::SUCCESS);
    response->set_message("");
    return grpc::Status::OK;
}

grpc::Status RainStormServer::NewStageTask(grpc::ServerContext* context,
                                          const rainstorm::NewStageTaskRequest* request,
                                          rainstorm::OperationStatus* response) {
    lock_guard<mutex> lock(global_mtx_);
    cout << "NewStageTask: " << request->query_id() << " next: " << request->snd_addresses_size() << " prev: " << request->rcv_addresses_size() << endl;
    response->set_status(rainstorm::SUCCESS);
    response->set_message("");
    return grpc::Status::OK;
}

grpc::Status RainStormServer::NewTgtTask(grpc::ServerContext* context,
                                        const rainstorm::NewTgtTaskRequest* request,
                                        rainstorm::OperationStatus* response) {
    lock_guard<mutex> lock(global_mtx_);
    cout << "NewTgtTask: " << request->query_id() << endl;
    response->set_status(rainstorm::SUCCESS);
    response->set_message("");
    return grpc::Status::OK;
}

grpc::Status RainStormServer::UpdateTaskSnd(grpc::ServerContext* context,
                                           const rainstorm::UpdateTaskSndRequest* request,
                                           rainstorm::OperationStatus* response) {
    lock_guard<mutex> lock(global_mtx_);
    cout << "UpdateTaskSnd: index=" << request->index() << " addr=" << request->snd_address() << ":" << request->snd_port() << endl;
    response->set_status(rainstorm::SUCCESS);
    return grpc::Status::OK;
}

grpc::Status RainStormServer::UpdateTaskRcv(grpc::ServerContext* context,
                                           const rainstorm::UpdateTaskRcvRequest* request,
                                           rainstorm::OperationStatus* response) {
    lock_guard<mutex> lock(global_mtx_);
    cout << "UpdateTaskRcv: index=" << request->index() << " addr=" << request->rcv_address() << ":" << request->rcv_port() << endl;
    response->set_status(rainstorm::SUCCESS);
    return grpc::Status::OK;
}

grpc::Status RainStormServer::SendDataChunks(grpc::ServerContext* context,
                                            grpc::ServerReaderWriter<rainstorm::AckDataChunk,
                                                                   rainstorm::StreamDataChunk>* stream) {
    rainstorm::StreamDataChunk chunk;
    while (stream->Read(&chunk)) {
        cout << "Received chunk with " << chunk.chunks_size() << " items" << endl;
    }
    
    rainstorm::AckDataChunk response;
    stream->Write(response);
    return grpc::Status::OK;
}
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
    if (server_) server_->Shutdown();
}

void RainStormServer::wait() {
    if (server_) server_->Wait();
}

grpc::Status RainStormServer::NewSrcTask(grpc::ServerContext* context,
                                          const rainstorm::NewSrcTaskRequest* request,
                                          rainstorm::OperationStatus* response) {
    lock_guard<mutex> lock(global_mtx_);
    cout << "NewSrcTask: " << request->query_id() << " " << request->src_filename() << endl;
    response->set_status(rainstorm::SUCCESS);
    response->set_message("");
    return grpc::Status::OK;
}

grpc::Status RainStormServer::NewStageTask(grpc::ServerContext* context,
                                            const rainstorm::NewStageTaskRequest* request,
                                            rainstorm::OperationStatus* response) {

    lock_guard<mutex> lock(global_mtx_); //?
    cout << "NewStageTask: " << request->query_id() << " next: " << request->snd_addresses_size() << " prev: " << request->rcv_addresses_size() << endl;
    node_->HandleNewStageTask(request);
    response->set_status(rainstorm::SUCCESS);
    response->set_message("");
    return grpc::Status::OK;
}

grpc::Status RainStormServer::NewTgtTask(grpc::ServerContext* context,
                                        const rainstorm::NewTgtTaskRequest* request,
                                        rainstorm::OperationStatus* response) {
    lock_guard<mutex> lock(global_mtx_);
    cout << "NewTgtTask: " << request->query_id() << endl;
    response->set_status(rainstorm::SUCCESS);
    response->set_message("");
    return grpc::Status::OK;
}

grpc::Status RainStormServer::UpdateTaskSnd(grpc::ServerContext* context,
                                           const rainstorm::UpdateTaskSndRequest* request,
                                           rainstorm::OperationStatus* response) {
    lock_guard<mutex> lock(global_mtx_);
    cout << "UpdateTaskSnd: index=" << request->index() << " addr=" << request->snd_address() << ":" << request->snd_port() << endl;
    response->set_status(rainstorm::SUCCESS);
    return grpc::Status::OK;
}

grpc::Status RainStormServer::UpdateTaskRcv(grpc::ServerContext* context,
                                           const rainstorm::UpdateTaskRcvRequest* request,
                                           rainstorm::OperationStatus* response) {
    lock_guard<mutex> lock(global_mtx_);
    cout << "UpdateTaskRcv: index=" << request->index() << " addr=" << request->rcv_address() << ":" << request->rcv_port() << endl;
    response->set_status(rainstorm::SUCCESS);
    return grpc::Status::OK;
}

void RainStormServer::SendDataChunksReader(grpc::ServerReaderWriter<rainstorm::AckDataChunk, rainstorm::StreamDataChunk>* stream) {
    rainstorm::StreamDataChunk chunk;
    string task_id;
    bool got_id = false;
    while (stream->Read(&chunk)) {
        if (!got_id && chunk.has_id()) {
            task_id = chunk.id();
            got_id = true;
            continue;
        }
        if (got_id && chunk.has_pair()) {
            const auto& p = chunk.pair();
            node_->EnqueueToBeProcessed(task_id, p);
        }
    }
}

void RainStormServer::SendDataChunksWriter(grpc::ServerReaderWriter<rainstorm::AckDataChunk, rainstorm::StreamDataChunk>* stream) {
    rainstorm::StreamDataChunk response_chunk;
    while (true) {
        string task_id;
        vector<pair<string, string>> acked_ids;
        if (node_->DequeueProcessed(task_id, acked_ids)) {
            rainstorm::DataChunk response_chunk;
            response_chunk.set_id(task_id);
            for (const auto& acked_id : acked_ids) {
                rainstorm::DataChunk* chunk = response_chunk.add_chunks();
                chunk->set_id(acked_id);
            }
            stream->Write(response_chunk);
        } else {
            break;
        }
    }
}

grpc::Status RainStormServer::SendDataChunks(grpc::ServerContext* context,
                                            grpc::ServerReaderWriter<rainstorm::AckDataChunk, rainstorm::StreamDataChunk>* stream) {
    thread reader_thread(&RainStormServer::SendDataChunksReader, this, stream);
    thread writer_thread(&RainStormServer::SendDataChunksWriter, this, stream);
    reader_thread.join();
    writer_thread.join();
    return grpc::Status::OK;
}
