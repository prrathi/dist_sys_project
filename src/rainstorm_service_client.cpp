#include "rainstorm_service_client.h"
#include <iostream>
#include <utility>

using namespace std;

RainStormClient::RainStormClient(std::shared_ptr<grpc::Channel> channel)
    : stub_(rainstorm::RainstormService::NewStub(channel)) {}

bool RainStormClient::NewSrcTask(const std::string &job_id, 
                                  int32_t task_id, 
                                  int32_t task_count, 
                                  const std::string &src_filename, 
                                  const std::string &snd_address, 
                                  int32_t snd_port) {
    grpc::ClientContext context;
    rainstorm::NewSrcTaskRequest request;
    rainstorm::OperationStatus response;

    request.set_job_id(job_id);
    request.set_task_id(task_id);
    request.set_task_count(task_count);
    request.set_src_filename(src_filename);
    request.set_snd_address(snd_address);
    request.set_snd_port(snd_port);

    grpc::Status status = stub_->NewSrcTask(&context, request, &response);

    if (status.ok() && response.status() == rainstorm::SUCCESS) {
        std::cout << "NewSrcTask succeeded: " << std::endl;
        return true;
    } else {
        std::cerr << "NewSrcTask failed: " << std::endl;
        return false;
    }
}

bool RainStormClient::NewStageTask(const std::string &job_id, 
                                    int32_t stage_id, 
                                    int32_t task_id, 
                                    int32_t task_count, 
                                    const std::string &executable, 
                                    bool stateful, 
                                    bool last, 
                                    const std::vector<std::string> &snd_addresses, 
                                    const std::vector<int32_t> &snd_ports) {
    grpc::ClientContext context;
    rainstorm::NewStageTaskRequest request;
    rainstorm::OperationStatus response;

    request.set_job_id(job_id);
    request.set_stage_id(stage_id);
    request.set_task_id(task_id);
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

    for (const auto &port : snd_ports) {
        request.add_snd_ports(port);
    }

    grpc::Status status = stub_->NewStageTask(&context, request, &response);

    if (status.ok() && response.status() == rainstorm::SUCCESS) {
        std::cout << "NewSrcTask succeeded: " << std::endl;
        return true;
    } else {
        std::cerr << "NewSrcTask failed: " << std::endl;
        return false;
    }
}

bool RainStormClient::UpdateSrcTaskSend(int32_t index, const std::string &snd_address, int32_t snd_port) {
    grpc::ClientContext context;
    rainstorm::UpdateTaskSndRequest request;
    rainstorm::OperationStatus response;

    request.set_index(index);
    request.set_snd_address(snd_address);
    request.set_snd_port(snd_port);

    grpc::Status status = stub_->UpdateTaskSnd(&context, request, &response);

    if (status.ok() && response.status() == rainstorm::SUCCESS) {
        std::cout << "UpdateSrcTaskSend succeeded: " << std::endl;
        return true;
    } else {
        std::cout << "UpdateSrcTaskSend failed: " << std::endl;
        return false;
    }
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

bool RainStormClient::SendDataChunksLeader(const std::string &task_id, SafeQueue<vector<pair<string, string>>>& queue) {
    // On the client side it is same as SendDataChunksStage except for we send a job_id before sending data.
    return true;
}