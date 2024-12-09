#pragma once

#include <string>
#include <memory>
#include <grpcpp/grpcpp.h>
#include <vector>
#include <unordered_set>
#include <mutex>
#include "rainstorm.grpc.pb.h"
#include "safequeue.hpp"
#include "rainstorm_common.h"

class RainStormClient {
public:
    RainStormClient(std::shared_ptr<grpc::Channel> channel);
    bool NewSrcTask(int32_t port, 
                    const std::string &job_id, 
                    int32_t task_index, 
                    int32_t task_count, 
                    const std::string &src_filename, 
                    const std::string &snd_address, 
                    int32_t snd_port);

    bool NewStageTask(int32_t port, 
                      const std::string &job_id, 
                      int32_t stage_index, 
                      int32_t task_index, 
                      int32_t task_count, 
                      const std::string &executable, 
                      bool stateful, 
                      bool last, 
                      const std::vector<std::string> &snd_addresses, 
                      const std::vector<int32_t> &snd_ports);
    bool UpdateSrcTaskSend(int32_t port, int32_t index, const std::string &snd_address, int32_t snd_port);
    bool SendDataChunks(int32_t port, std::shared_ptr<SafeQueue<std::vector<KVStruct>>> queue, std::unordered_set<int>& acked_ids, std::mutex& acked_ids_mutex, int task_index);
    bool SendDataChunksLeader(std::shared_ptr<SafeQueue<std::vector<KVStruct>>> queue, std::unordered_set<int>& acked_ids, std::mutex& acked_ids_mutex, std::string job_id);

private:
    void writeToStream(grpc::ClientReaderWriter<rainstorm::StreamDataChunk, rainstorm::AckDataChunk>* stream,
                      int32_t port, int task_index, 
                      std::shared_ptr<SafeQueue<std::vector<KVStruct>>> queue);
    
    void readFromStream(grpc::ClientReaderWriter<rainstorm::StreamDataChunk, rainstorm::AckDataChunk>* stream,
                       std::unordered_set<int>& acked_ids, 
                       std::mutex& acked_ids_mutex);
    std::unique_ptr<rainstorm::RainstormService::Stub> stub_;
};
