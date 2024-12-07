#pragma once

#include <string>
#include <vector>
#include <unordered_map>
#include <memory>
#include <mutex>
#include <grpcpp/grpcpp.h>
#include "rainstorm.grpc.pb.h"
#include "rainstorm_common.h"
#include "safequeue.hpp"


class RainStormLeader;
class RainStormNode;

class RainStormServer final : public rainstorm::RainstormService::Service {
public:
    RainStormServer(const std::string& server_address, INodeServerInterface* node_interface);
    ~RainStormServer();
    RainStormServer(RainStormNode* node);
    RainStormServer(RainStormLeader* leader);

    void wait();
    void shutdown();
    
    grpc::Status NewSrcTask(grpc::ServerContext* context,
                           const rainstorm::NewSrcTaskRequest* request,
                           rainstorm::OperationStatus* response) override;

    grpc::Status NewStageTask(grpc::ServerContext* context,
                             const rainstorm::NewStageTaskRequest* request,
                             rainstorm::OperationStatus* response) override;

    grpc::Status UpdateTaskSnd(grpc::ServerContext* context,
                              const rainstorm::UpdateTaskSndRequest* request,
                              rainstorm::OperationStatus* response) override;

    grpc::Status SendDataChunks(grpc::ServerContext* context,
                               grpc::ServerReaderWriter<rainstorm::AckDataChunk,
                                rainstorm::StreamDataChunk>* stream) override;

    grpc::Status SendDataChunksToLeader(grpc::ServerContext* context,
                               grpc::ServerReaderWriter<rainstorm::AckDataChunk,
                                rainstorm::StreamDataChunkLeader>* stream) override;

private:
    void SendDataChunksReader(grpc::ServerReaderWriter<rainstorm::AckDataChunk,
                                                      rainstorm::StreamDataChunk>* stream);
    void SendDataChunksWriter(grpc::ServerReaderWriter<rainstorm::AckDataChunk,
                                                      rainstorm::StreamDataChunk>* stream);

    // Helper methods for protocol buffer conversion
    KVStruct protoToKVStruct(const rainstorm::KV& proto_kv);
    rainstorm::KV kvStructToProto(const KVStruct& kv);

    std::string server_address_;
    std::unique_ptr<grpc::Server> server_;
    INodeServerInterface* node_; 
    std::mutex global_mtx_;
    RainStormNode* node_;
    RainStormLeader* leader_node_;
};
