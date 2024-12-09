#pragma once

#include <string>
#include <vector>
#include <unordered_map>
#include <memory>
#include <mutex>
#include <grpcpp/grpcpp.h>

#include "rainstorm.grpc.pb.h"
#include "rainstorm_common.h"
#include "rainstorm_node.h"

class RainstormFactory;
class RainStormLeader;
class RainStormNodeBase;

class RainStormServer final : public rainstorm::RainstormService::Service {
public:
    explicit RainStormServer(RainstormFactory* factory, int server_port);
    explicit RainStormServer(RainStormLeader* leader, int server_port);
    ~RainStormServer();

    void initializeServer();
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
    void SendDataChunksReader(grpc::ServerReaderWriter<rainstorm::AckDataChunk, rainstorm::StreamDataChunk>* stream,
                              int port);
    void SendDataChunksWriter(grpc::ServerReaderWriter<rainstorm::AckDataChunk,
                              rainstorm::StreamDataChunk>* stream,
                              int task_index,
                              int port);

    void SendDataChunksLeaderReader(
        grpc::ServerReaderWriter<rainstorm::AckDataChunk, rainstorm::StreamDataChunkLeader>* stream,
        SafeQueue<std::vector<int>>& ack_queue,
        std::atomic<bool>& done_reading,
        const std::string job_id);
    void SendDataChunksLeaderWriter(
        grpc::ServerReaderWriter<rainstorm::AckDataChunk, rainstorm::StreamDataChunkLeader>* stream,
        SafeQueue<std::vector<int>>& ack_queue,
        std::atomic<bool>& done_reading,
        const std::string job_id);

    KVStruct protoToKVStruct(const rainstorm::KV& proto_kv);
    rainstorm::KV kvStructToProto(const KVStruct& kv);

private:
    std::string server_address_;
    int server_port_;
    std::unique_ptr<grpc::Server> server_;
    std::mutex global_mtx_;

    RainstormFactory* factory_ = nullptr; 
    RainStormLeader* leader_ = nullptr; 
};
