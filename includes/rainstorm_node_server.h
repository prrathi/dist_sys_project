#pragma once

#include <string>
#include <vector>
#include <unordered_map>
#include <memory>
#include <mutex>
#include <grpcpp/grpcpp.h>
#include "rainstorm.grpc.pb.h"

class RainStormServer final : public rainstorm::RainstormService::Service {
public:
    RainStormServer(const std::string& server_address);
    ~RainStormServer();

    void wait();
    
    grpc::Status NewSrcTask(grpc::ServerContext* context,
                           const rainstorm::NewSrcTaskRequest* request,
                           rainstorm::OperationStatus* response) override;

    grpc::Status NewStageTask(grpc::ServerContext* context,
                             const rainstorm::NewStageTaskRequest* request,
                             rainstorm::OperationStatus* response) override;

    grpc::Status NewTgtTask(grpc::ServerContext* context,
                           const rainstorm::NewTgtTaskRequest* request,
                           rainstorm::OperationStatus* response) override;

    grpc::Status UpdateTaskSnd(grpc::ServerContext* context,
                              const rainstorm::UpdateTaskSndRequest* request,
                              rainstorm::OperationStatus* response) override;

    grpc::Status UpdateTaskRcv(grpc::ServerContext* context,
                              const rainstorm::UpdateTaskRcvRequest* request,
                              rainstorm::OperationStatus* response) override;

    grpc::Status SendDataChunks(grpc::ServerContext* context,
                               grpc::ServerReaderWriter<rainstorm::AckDataChunk,
                                                      rainstorm::StreamDataChunk>* stream) override;

private:
    void SendDataChunksReader(grpc::ServerReaderWriter<rainstorm::AckDataChunk,
                                                      rainstorm::StreamDataChunk>* stream);
    void SendDataChunksWriter(grpc::ServerReaderWriter<rainstorm::AckDataChunk,
                                                      rainstorm::StreamDataChunk>* stream);

    std::string server_address_;
    std::unique_ptr<grpc::Server> server_;
    std::mutex global_mtx_;
};