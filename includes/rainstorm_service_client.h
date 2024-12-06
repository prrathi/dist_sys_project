#pragma once

#include <string>
#include <memory>
#include <grpcpp/grpcpp.h>
#include <vector>

#include "rainstorm_node.h"
#include "rainstorm.grpc.pb.h"
#include "safequeue.hpp"

class RainStormClient {
public:
    RainStormClient(std::shared_ptr<grpc::Channel> channel);
    RainStormClient(std::shared_ptr<grpc::Channel> channel, RainStormNode rainstorm_node);
    bool NewSrcTask(const std::string &id, const std::string &src_filename);
    bool NewStageTask(const std::string &id, const std::string &next_server_address, const std::string &prev_server_address);
    bool UpdateSrcTaskSend(const std::string &id, const std::string &new_next_server_address);
    bool UpdateDstTaskRecieve(const std::string &id, const std::string &new_next_server_address, const std::string &new_prev_server_address);
    bool SendDataChunksSrc(const std::string &task_id, const string& srcfile);
    bool SendDataChunksStage(const std::string &task_id, SafeQueue<vector<pair<string, string>>>& queue);

private:
    std::unique_ptr<rainstorm::RainstormService::Stub> stub_;
    RainStormNode rainstorm_node_;
};
