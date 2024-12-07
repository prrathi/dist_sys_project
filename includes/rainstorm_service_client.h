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
    bool NewSrcTask(const std::string &job_id, 
                    int32_t task_id, 
                    int32_t task_count, 
                    const std::string &src_filename, 
                    const std::string &snd_address, 
                    int32_t snd_port);

    bool NewStageTask(const std::string &job_id, 
                      int32_t stage_id, 
                      int32_t task_id, 
                      int32_t task_count, 
                      const std::string &executable, 
                      bool stateful, 
                      bool last, 
                      const std::vector<std::string> &snd_addresses, 
                      const std::vector<int32_t> &snd_ports);
    bool UpdateSrcTaskSend(int32_t index, const std::string &snd_address, int32_t snd_port);
    bool SendDataChunksSrc(const std::string &task_id, const string& srcfile);
    bool SendDataChunksStage(const std::string &task_id, SafeQueue<vector<pair<string, string>>>& queue);

    // send data chunks to the leader on the client side it is same as SendDataChunksStage
    bool SendDataChunksLeader(const std::string &task_id, SafeQueue<vector<pair<string, string>>>& queue);

private:
    std::unique_ptr<rainstorm::RainstormService::Stub> stub_;
};
