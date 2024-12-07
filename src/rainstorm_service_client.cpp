#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "rainstorm_common.h"
#include "rainstorm_service_client.h"
#include "safequeue.hpp"

using namespace std;

RainStormClient::RainStormClient(shared_ptr<grpc::Channel> channel)
    : stub_(rainstorm::RainstormService::NewStub(channel)) {}

bool RainStormClient::NewSrcTask(const string& id, const string& src_filename) {
    grpc::ClientContext context;
    rainstorm::NewSrcTaskRequest request;
    rainstorm::OperationStatus response;

    request.set_id(id);
    request.set_src_filename(src_filename);

    grpc::Status status = stub_->NewSrcTask(&context, request, &response);
    if (status.ok() && response.status() == rainstorm::SUCCESS) {
        return true;
    }
    return false;
}

bool RainStormClient::NewStageTask(const string& id, const string& next_server_address, 
                                  const string& prev_server_address) {
    grpc::ClientContext context;
    rainstorm::NewStageTaskRequest request;
    rainstorm::OperationStatus response;

    request.set_id(id);
    request.set_next_server_address(next_server_address);
    request.set_prev_server_address(prev_server_address);

    grpc::Status status = stub_->NewStageTask(&context, request, &response);
    return status.ok() && response.status() == rainstorm::SUCCESS;
}

bool RainStormClient::UpdateTaskSnd(int32_t index, const string& snd_address, int32_t snd_port) {
    grpc::ClientContext context;
    rainstorm::UpdateTaskSndRequest request;
    rainstorm::OperationStatus response;

    request.set_index(index);
    request.set_snd_address(snd_address);
    request.set_snd_port(snd_port);

    grpc::Status status = stub_->UpdateTaskSnd(&context, request, &response);
    return status.ok() && response.status() == rainstorm::SUCCESS;
}

bool RainStormClient::SendDataChunks(shared_ptr<SafeQueue<vector<KVStruct>>> queue, unordered_set<int>& acked_ids, mutex& acked_ids_mutex) {
    grpc::ClientContext context;
    unique_ptr<grpc::ClientReaderWriter<rainstorm::StreamDataChunk, rainstorm::AckDataChunk>> stream(
        stub_->SendDataChunks(&context));

    // Writer thread
    thread writer_thread([&] {
        rainstorm::StreamDataChunk init_chunk;
        if (!stream->Write(init_chunk)) {
            cerr << "Initial Write failed." << endl;
            return;
        }

        while (true) {
            vector<KVStruct> kv_pairs;
            if (queue->dequeue(kv_pairs)) {
                rainstorm::StreamDataChunk data_chunk_msg;
                for (const auto& kv : kv_pairs) {
                    auto* data_chunk = data_chunk_msg.mutable_chunks()->Add();
                    auto* pair = data_chunk->mutable_pair();
                    pair->set_id(kv.id);
                    pair->set_key(kv.key);
                    pair->set_value(kv.value);
                }

                if (!stream->Write(data_chunk_msg)) {
                    cerr << "Failed to write data chunk" << endl;
                    break;
                }
            } else {
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
            for (const auto& chunk_id : server_chunk.id()) {
                acked_ids.insert(chunk_id);
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
    unique_ptr<grpc::ClientReaderWriter<rainstorm::StreamDataChunkLeader, rainstorm::AckDataChunk>> stream(
        stub_->SendDataChunksLeader(&context));

    // Writer thread
    thread writer_thread([&] {
        rainstorm::StreamDataChunkLeader init_chunk;
        auto* first_chunk = init_chunk.mutable_chunks()->Add();
        first_chunk->set_job_id(job_id);
        
        if (!stream->Write(init_chunk)) {
            cerr << "Initial Write failed." << endl;
            return;
        }

        while (true) {
            vector<KVStruct> kv_pairs;
            if (queue->dequeue(kv_pairs)) {
                rainstorm::StreamDataChunkLeader data_chunk_msg;
                for (const auto& kv : kv_pairs) {
                    auto* data_chunk = data_chunk_msg.mutable_chunks()->Add();
                    auto* pair = data_chunk->mutable_pair();
                    pair->set_id(kv.id);
                    pair->set_key(kv.key);
                    pair->set_value(kv.value);
                }

                if (!stream->Write(data_chunk_msg)) {
                    cerr << "Failed to write data chunk" << endl;
                    break;
                }
            } else {
                rainstorm::StreamDataChunkLeader final_chunk;
                auto* last_chunk = final_chunk.mutable_chunks()->Add();
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
            for (const auto& chunk_id : server_chunk.id()) {
                acked_ids.insert(chunk_id);
            }
        }
    });

    writer_thread.join();
    reader_thread.join();

    grpc::Status status = stream->Finish();
    return status.ok();
}
