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
using rainstorm::StatusCode;
using rainstorm::OperationStatus;
using rainstorm::NewSrcTaskRequest;
using rainstorm::NewStageTaskRequest;
using rainstorm::UpdateTaskSndRequest;
using rainstorm::AckDataChunk;
using rainstorm::StreamDataChunk;

RainStormServer::RainStormServer(const string& server_address, INodeServerInterface* node) {
    node_ = node;
    if (server_address != "") {
        server_address_ = server_address;
    } else {
        char hostname[256];
        if (gethostname(hostname, sizeof(hostname)) != 0) {
            perror("gethostname");
            exit(1);
        }
        string hostname_str = hostname;
        server_address_ = hostname_str + ":" + to_string(GRPC_PORT_SERVER);
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
