#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <utility>
#include <vector>
#include <unordered_set>
#include <mutex>
#include "rainstorm_common.h"
#include "rainstorm_service_client.h"

using namespace std;

RainStormClient::RainStormClient(shared_ptr<grpc::Channel> channel)
    : stub_(rainstorm::RainstormService::NewStub(channel)) {}

bool RainStormClient::NewSrcTask(int port, 
                                 const std::string &job_id, 
                                 int32_t task_index, 
                                 int32_t task_count, 
                                 const std::string &src_filename, 
                                 const std::string &snd_address, 
                                 int32_t snd_port) {
    grpc::ClientContext context;
    rainstorm::NewSrcTaskRequest request;
    rainstorm::OperationStatus response;

    request.set_port(port);
    request.set_job_id(job_id);
    request.set_task_index(task_index);
    request.set_task_count(task_count);
    request.set_src_filename(src_filename);
    request.set_snd_address(snd_address);
    request.set_snd_port(snd_port);

    grpc::Status status = stub_->NewSrcTask(&context, request, &response);

    if (status.ok() && response.status() == rainstorm::SUCCESS) {
        std::cout << "NewSrcTask succeeded." << std::endl;
        return true;
    } else {
        std::cerr << "NewSrcTask failed: " << response.message() << std::endl;
        return false;
    }
}

bool RainStormClient::NewStageTask(int port, 
                                   const std::string &job_id, 
                                   int32_t stage_index, 
                                   int32_t task_index, 
                                   int32_t task_count, 
                                   const std::string &executable, 
                                   bool stateful, 
                                   bool last, 
                                   const std::vector<std::string> &snd_addresses, 
                                   const std::vector<int32_t> &snd_ports) {
    grpc::ClientContext context;
    rainstorm::NewStageTaskRequest request;
    rainstorm::OperationStatus response;

    request.set_port(port);
    request.set_job_id(job_id);
    request.set_stage_index(stage_index);
    request.set_task_index(task_index);
    request.set_task_count(task_count);
    request.set_executable(executable);
    request.set_stateful(stateful);
    request.set_last(last);
      
    if (snd_addresses.size() != snd_ports.size()) {
        std::cerr << "NewStageTask failed: snd_addresses and snd_ports size mismatch." << std::endl;
        return false;
    }

    for (const auto &addr : snd_addresses) {
        request.add_snd_addresses(addr);
    }

    for (auto port : snd_ports) {
        request.add_snd_ports(port);
    }

    grpc::Status status = stub_->NewStageTask(&context, request, &response);

    if (status.ok() && response.status() == rainstorm::SUCCESS) {
        std::cout << "NewStageTask succeeded." << std::endl;
        return true;
    } else {
        std::cerr << "NewStageTask failed." << std::endl;
        return false;
    }
}

bool RainStormClient::UpdateSrcTaskSend(int32_t port, int32_t index, const string& snd_address, int32_t snd_port) {
    grpc::ClientContext context;
    rainstorm::UpdateTaskSndRequest request;
    rainstorm::OperationStatus response;

    request.set_port(port);
    request.set_index(index);
    request.set_snd_address(snd_address);
    request.set_snd_port(snd_port);

    grpc::Status status = stub_->UpdateTaskSnd(&context, request, &response);
    return status.ok() && response.status() == rainstorm::SUCCESS;
}

bool RainStormClient::SendDataChunks(int32_t port, shared_ptr<SafeQueue<vector<KVStruct>>> queue, unordered_set<int>& acked_ids, mutex& acked_ids_mutex, int task_index) {
    grpc::ClientContext context;
    auto stream = stub_->SendDataChunks(&context);

    // Writer thread
    thread writer_thread([&] {
        // Initial empty write if needed
        // not strictly required since we start reading anyway
        rainstorm::StreamDataChunk initial_msg;
        auto* chunk = initial_msg.add_chunks();
        chunk->set_port(port);
        chunk->set_task_index(task_index);
        stream->Write(initial_msg);
        while (true) {
            vector<KVStruct> kv_pairs;
            if (queue->dequeue(kv_pairs)) {
                rainstorm::StreamDataChunk data_chunk_msg;
                for (const auto& kv : kv_pairs) {
                    auto* chunk = data_chunk_msg.add_chunks();
                    auto* pair = chunk->mutable_pair();
                    pair->set_id(kv.id);
                    pair->set_key(kv.key);
                    pair->set_value(kv.value);
                    pair->set_task_index(kv.task_index);
                }

                if (!stream->Write(data_chunk_msg)) {
                    cerr << "Failed to write data chunk" << endl;
                    break;
                }
            } else {
                // finished
                rainstorm::StreamDataChunk final_msg;
                auto* finished_chunk = final_msg.add_chunks();
                finished_chunk->set_finished(true);
                stream->Write(final_msg);
                break;
            }
        }

        stream->WritesDone();
    });

    // Reader thread
    thread reader_thread([&] {
        rainstorm::AckDataChunk server_chunk;
        while (stream->Read(&server_chunk)) {
            lock_guard<mutex> lock(acked_ids_mutex);
            for (auto id : server_chunk.id()) {
                acked_ids.insert(id);
            }
        }
    });

    writer_thread.join();
    reader_thread.join();

    grpc::Status status = stream->Finish();
    return status.ok();
}

bool RainStormClient::SendDataChunksLeader(shared_ptr<SafeQueue<vector<KVStruct>>> queue, unordered_set<int>& acked_ids, mutex& acked_ids_mutex, string job_id) {
    grpc::ClientContext context;
    auto stream = stub_->SendDataChunksToLeader(&context);

    // Writer thread
    thread writer_thread([&] {
        {
            rainstorm::StreamDataChunkLeader init_chunk;
            auto* first_chunk = init_chunk.add_chunks();
            first_chunk->set_job_id(job_id);
            if (!stream->Write(init_chunk)) {
                cerr << "Initial Write failed." << endl;
                return;
            }
        }

        while (true) {
            vector<KVStruct> kv_pairs;
            if (queue->dequeue(kv_pairs)) {
                rainstorm::StreamDataChunkLeader data_chunk_msg;
                for (const auto& kv : kv_pairs) {
                    auto* chunk = data_chunk_msg.add_chunks();
                    auto* pair = chunk->mutable_pair();
                    pair->set_id(kv.id);
                    pair->set_key(kv.key);
                    pair->set_value(kv.value);
                    pair->set_task_index(kv.task_index);
                }

                if (!stream->Write(data_chunk_msg)) {
                    cerr << "Failed to write data chunk" << endl;
                    break;
                }
            } else {
                rainstorm::StreamDataChunkLeader final_chunk;
                auto* last_chunk = final_chunk.add_chunks();
                last_chunk->set_finished(true);
                stream->Write(final_chunk);
                break;
            }
        }

        stream->WritesDone();
    });

    // Reader thread
    thread reader_thread([&] {
        rainstorm::AckDataChunk server_chunk;
        while (stream->Read(&server_chunk)) {
            lock_guard<mutex> lock(acked_ids_mutex);
            for (auto id : server_chunk.id()) {
                acked_ids.insert(id);
            }
        }
    });

    writer_thread.join();
    reader_thread.join();

    grpc::Status status = stream->Finish();
    return status.ok();
}
