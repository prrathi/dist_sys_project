#include "rainstorm_service_client.h"
#include <iostream>
#include <utility>

using namespace std;

RainStormClient::RainStormClient(std::shared_ptr<grpc::Channel> channel)
    : stub_(rainstorm::RainstormService::NewStub(channel)) {}

RainStormClient::RainStormClient(std::shared_ptr<grpc::Channel> channel, RainStormNode rainstorm_node)
    : stub_(rainstorm::RainstormService::NewStub(channel)), rainstorm_node_(rainstorm_node) {}

bool RainStormClient::NewSrcTask(const std::string &id, const std::string &src_filename) {
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

bool RainStormClient::NewStageTask(const std::string &id, const std::string &next_server_address, const std::string &prev_server_address) {
    grpc::ClientContext context;
    rainstorm::NewStageTaskRequest request;
    rainstorm::OperationStatus response;

    request.set_id(id);
    request.set_next_server_address(next_server_address);
    request.set_prev_server_address(prev_server_address);

    grpc::Status status = stub_->NewStageTask(&context, request, &response);
    return status.ok() && response.status() == rainstorm::SUCCESS;
}

bool RainStormClient::UpdateSrcTaskSend(const std::string &id, const std::string &new_next_server_address) {
    grpc::ClientContext context;
    rainstorm::UpdateSrcTaskSendRequest request;
    rainstorm::OperationStatus response;

    request.set_id(id);
    request.set_new_next_server_address(new_next_server_address);

    grpc::Status status = stub_->UpdateSrcTaskSend(&context, request, &response);
    return status.ok() && response.status() == rainstorm::SUCCESS;
}

bool RainStormClient::UpdateDstTaskRecieve(const std::string &id, const std::string &new_next_server_address, const std::string &new_prev_server_address) {
    grpc::ClientContext context;
    rainstorm::UpdateDstTaskRecieveRequest request;
    rainstorm::OperationStatus response;

    request.set_id(id);
    request.set_new_next_server_address(new_next_server_address);
    request.set_new_prev_server_address(new_prev_server_address);

    grpc::Status status = stub_->UpdateDstTaskRecieve(&context, request, &response);
    return status.ok() && response.status() == rainstorm::SUCCESS;
}

bool RainStormClient::SendDataChunksStage(const std::string &task_id, SafeQueue<vector<pair<string, string>>>& queue) {
    grpc::ClientContext context;
    std::unique_ptr<grpc::ClientReaderWriter<rainstorm::StreamDataChunk, rainstorm::AckDataChunk>> stream(stub_->SendDataChunks(&context));

    std::mutex mtx;
    std::condition_variable cv;
    bool done_writing = false;

    // Writer thread
    std::thread writer_thread([&] {
        // 1. Send initial request with task ID
        rainstorm::StreamDataChunk init_chunk;
        rainstorm::DataChunk* data_chunk = init_chunk.add_chunks();
        data_chunk->set_id(task_id);
        if (!stream->Write(init_chunk)) {
            std::cout << "Initial Write failed (task ID)." << std::endl;
        }
        //std::cout << "Initial chunk with id: " << task_id << " sent successfully" << std::endl;

        // 2. Send subsequent requests with key-value pairs
        while (true) {
            std::vector<std::pair<std::string, std::string>> kv_pairs;
            if (queue.dequeue(kv_pairs)) {
                rainstorm::StreamDataChunk data_chunk_msg;
                for (const auto& kv : kv_pairs) {
                    rainstorm::DataChunk* data_chunk = data_chunk_msg.add_chunks();
                    rainstorm::KV* p = data_chunk->mutable_pairs();
                    p->set_key(kv.first);
                    p->set_value(kv.second);
                }

                if (!stream->Write(data_chunk_msg)) {
                    std::cout << "Write failed (key-value pairs)." << std::endl;
                    break;
                }
            }
        }
        // 3. Signal that writing is done
        stream->WritesDone();
        {
            std::lock_guard<std::mutex> lock(mtx);
            done_writing = true;
        }
        cv.notify_one();

    });

    // Reader thread
    std::thread reader_thread([&] {
        rainstorm::StreamDataChunk server_chunk;
        while (stream->Read(&server_chunk)) {
            // process server acks if any
        }
    });

    writer_thread.join();
    {
        std::unique_lock<std::mutex> lock(mtx);
        cv.wait(lock, [&] { return done_writing; });
    }
    reader_thread.join();

    grpc::Status status = stream->Finish();
    return status.ok();
}

bool RainStormClient::SendDataChunksSrc(const std::string &task_id, const string& srcfile) {
    grpc::ClientContext context;
    std::shared_ptr<grpc::ClientReaderWriter<rainstorm::StreamDataChunk, rainstorm::StreamDataChunk>> stream(
        stub_->SendDataChunks(&context));

    std::mutex mtx;
    std::condition_variable cv;
    bool done_writing = false;

    // Writer thread
    std::thread writer_thread([&] {
        rainstorm::StreamDataChunk init_chunk;
        init_chunk.set_id(task_id);
        if (!stream->Write(init_chunk)) {
            return;
        }

        for (auto &kv : kv_pairs) {
            rainstorm::StreamDataChunk chunk;
            auto p = chunk.add_pairs();
            p->set_key(kv.first);
            p->set_value(kv.second);
            if (!stream->Write(chunk)) {
                break;
            }
        }

        stream->WritesDone();
        {
            std::lock_guard<std::mutex> lock(mtx);
            done_writing = true;
        }
        cv.notify_one();
    });

    // Reader thread
    std::thread reader_thread([&] {
        rainstorm::StreamDataChunk server_chunk;
        while (stream->Read(&server_chunk)) {
            // process server acks if any
        }
    });

    writer_thread.join();
    {
        std::unique_lock<std::mutex> lock(mtx);
        cv.wait(lock, [&] { return done_writing; });
    }
    reader_thread.join();

    grpc::Status status = stream->Finish();
    return status.ok();
}

