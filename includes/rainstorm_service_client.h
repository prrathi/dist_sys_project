#pragma once

#include <string>
#include <memory>
#include <grpcpp/grpcpp.h>
#include <vector>

#include "rainstorm.grpc.pb.h"
#include "safequeue.hpp"

class RainStormClient {
public:
    RainStormClient(std::shared_ptr<grpc::Channel> channel);
    bool NewSrcTask(const std::string &id, const std::string &src_filename);
    bool NewStageTask(const std::string &id, const std::string &next_server_address, const std::string &prev_server_address);
    bool UpdateTaskSnd(int32_t index, const std::string& snd_address, int32_t snd_port);
    bool SendDataChunks(std::shared_ptr<SafeQueue<vector<KVStruct>>> queue, std::unordered_set<int>& acked_ids, std::mutex& acked_ids_mutex);
    bool SendDataChunksLeader(shared_ptr<SafeQueue<vector<KVStruct>>> queue, unordered_set<int>& acked_ids, mutex& acked_ids_mutex, string job_id);

private:
    std::unique_ptr<rainstorm::RainstormService::Stub> stub_;
};
