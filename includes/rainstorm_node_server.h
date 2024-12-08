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
    RainStormServer(RainStormNode* node);
    RainStormServer(RainStormLeader* leader);
    ~RainStormServer();

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

    KVStruct protoToKVStruct(const rainstorm::KV& proto_kv);
    rainstorm::KV kvStructToProto(const KVStruct& kv);

private:
    std::string server_address_;
    std::unique_ptr<grpc::Server> server_;
    std::mutex global_mtx_;

    RainStormNode* node_ = nullptr;
    RainStormLeader* leader_node_ = nullptr;
};
