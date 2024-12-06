#pragma once

#include <string>
#include <vector>
#include <unordered_map>
#include <memory>
#include <mutex>
#include <grpcpp/grpcpp.h>
#include "rainstorm.grpc.pb.h"
#include "rainstorm_node.h"

class RainStormServer : public rainstorm::RainstormService::Service {
public:
    RainStormServer();
    ~RainStormServer();
    RainStormServer(RainStormNode* node) : node_(node) {};

    void wait();

    grpc::Status NewSrcTask(grpc::ServerContext* context,
                            const rainstorm::NewSrcTaskRequest* request,
                            rainstorm::OperationStatus* response) override;

    grpc::Status NewStageTask(grpc::ServerContext* context,
                              const rainstorm::NewStageTaskRequest* request,
                              rainstorm::OperationStatus* response) override;

    grpc::Status UpdateSrcTaskSend(grpc::ServerContext* context,
                                   const rainstorm::UpdateSrcTaskSendRequest* request,
                                   rainstorm::OperationStatus* response) override;

    grpc::Status UpdateDstTaskRecieve(grpc::ServerContext* context,
                                      const rainstorm::UpdateDstTaskRecieveRequest* request,
                                      rainstorm::OperationStatus* response) override;

    grpc::Status SendDataChunks(grpc::ServerContext* context,
                                grpc::ServerReaderWriter<rainstorm::StreamDataChunk, rainstorm::StreamDataChunk>* stream) override;

private:

    void SendDataChunksWriter(grpc::ServerReaderWriter<rainstorm::StreamDataChunk, rainstorm::StreamDataChunk>* stream);
    void SendDataChunksReader(grpc::ServerReaderWriter<rainstorm::StreamDataChunk, rainstorm::StreamDataChunk>* stream);

    std::string server_address_;
    std::unique_ptr<grpc::Server> server_;
    std::mutex global_mtx_;
    RainStormNode* node_;
};
