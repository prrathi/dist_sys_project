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
        cout << "END TIME IS " << chrono::duration_cast<chrono::milliseconds>(chrono::system_clock::now().time_since_epoch()).count() << endl;
        std::cout << "NewStageTask succeeded for stage " << stage_index << " task " << task_index << std::endl;
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

bool RainStormClient::SendDataChunks(int32_t port, 
                                     std::shared_ptr<SafeQueue<std::vector<KVStruct>>> queue, 
                                     std::unordered_set<int>& acked_ids, 
                                     std::mutex& acked_ids_mutex, 
                                     int task_index) {
    grpc::ClientContext context;
    auto stream = stub_->SendDataChunks(&context);
    
    if (!stream) {
        std::cerr << "Failed to create stream." << std::endl;
        return false;
    }
    
    // Create writer and reader threads
    std::thread writer_thread(&RainStormClient::writeToStream, this, stream.get(), port, task_index, queue);
    std::thread reader_thread(&RainStormClient::readFromStream, this, stream.get(), std::ref(acked_ids), std::ref(acked_ids_mutex));
    
    // Wait for both threads to finish
    writer_thread.join();
    reader_thread.join();
    
    // Finish the stream and check for errors
    grpc::Status status = stream->Finish();
    if (status.ok()) {
        std::cout << "SendDataChunks RPC completed successfully." << std::endl;
        return true;
    } else {
        std::cerr << "SendDataChunks RPC failed: " << status.error_message() << std::endl;
        return false;
    }
}

// Writer function
// Writer function
void RainStormClient::writeToStream(
    grpc::ClientReaderWriter<rainstorm::StreamDataChunk, rainstorm::AckDataChunk>* stream,
    int32_t port, 
    int task_index, 
    std::shared_ptr<SafeQueue<std::vector<KVStruct>>> queue) {
    
    try {
        // Create a single StreamDataChunk message containing both port and task_index
        rainstorm::StreamDataChunk initial_msg;
        
        // Add port to the message
        rainstorm::DataChunk port_chunk;
        port_chunk.set_port(port);
        initial_msg.add_chunks()->CopyFrom(port_chunk);
        
        // Add task_index to the message
        rainstorm::DataChunk task_index_chunk;
        task_index_chunk.set_task_index(task_index);
        initial_msg.add_chunks()->CopyFrom(task_index_chunk);
        
        // Log and send the initial combined message
        std::cout << "Writing initial message with port: " << port 
                  << " and task_index: " << task_index << std::endl;
        if (!stream->Write(initial_msg)) {
            std::cerr << "Failed to write initial message with port and task_index." << std::endl;
            return;
        }
        std::cout << "Initial message written successfully." << std::endl;
        
        // Clear initial_msg for reuse
        initial_msg.Clear();
        
        // Continue with sending KV pairs as before
        while (true) {
            std::vector<KVStruct> kv_pairs;
            if (queue->dequeue(kv_pairs)) { // Assume dequeue blocks until data is available
                if (kv_pairs.empty()) {
                    // No data to send; skip
                    continue;
                }
                
                rainstorm::StreamDataChunk data_chunk_msg;
                for (const auto& kv : kv_pairs) {
                    // Validate KVStruct fields
                    if (kv.key.empty()) {
                        std::cerr << "Warning: KVStruct with empty key detected. ID: " 
                                  << kv.id << std::endl;
                        continue; // Skip or handle accordingly
                    }
                    if (kv.value.empty()) {
                        std::cerr << "Warning: KVStruct with empty value detected. ID: " 
                                  << kv.id << std::endl;
                        continue; // Skip or handle accordingly
                    }
                    
                    rainstorm::DataChunk pair_chunk;
                    rainstorm::KV* kv_ptr = pair_chunk.mutable_pair();
                    kv_ptr->set_id(kv.id);
                    kv_ptr->set_key(kv.key);
                    kv_ptr->set_value(kv.value);
                    kv_ptr->set_task_index(kv.task_index);
                    
                    data_chunk_msg.add_chunks()->CopyFrom(pair_chunk);
                }
                
                if (data_chunk_msg.chunks_size() == 0) {
                    // No valid data to send
                    continue;
                }
                
                std::cout << "Writing " << data_chunk_msg.chunks_size() 
                          << " KV pair(s)." << std::endl;
                if (!stream->Write(data_chunk_msg)) {
                    std::cerr << "Failed to write KV pair message(s)." << std::endl;
                    return;
                }
                std::cout << "KV pair message(s) written successfully." << std::endl;
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
            } else {
                // No more data to send; send final message
                rainstorm::StreamDataChunk finished_msg;
                rainstorm::DataChunk finished_chunk;
                finished_chunk.set_finished(true);
                finished_msg.add_chunks()->CopyFrom(finished_chunk);
                
                std::cout << "Writing finished message." << std::endl;
                if (!stream->Write(finished_msg)) {
                    std::cerr << "Failed to write finished message." << std::endl;
                } else {
                    std::cout << "Finished message written successfully." << std::endl;
                }
                break;
            }
        }
        
        // Indicate that no more writes will be sent
        std::cout << "Calling WritesDone() on stream." << std::endl;
        stream->WritesDone();
    } catch (const std::exception& e) {
        std::cerr << "Exception in writeToStream: " << e.what() << std::endl;
    }
}


// Reader function
void RainStormClient::readFromStream(grpc::ClientReaderWriter<rainstorm::StreamDataChunk, rainstorm::AckDataChunk>* stream,
                                  std::unordered_set<int>& acked_ids, 
                                  std::mutex& acked_ids_mutex) {
    try {
        rainstorm::AckDataChunk ack_chunk;
        while (stream->Read(&ack_chunk)) {
            std::lock_guard<std::mutex> lock(acked_ids_mutex);
            for (auto id : ack_chunk.id()) {
                acked_ids.insert(id);
                std::cout << "Received acknowledgment for ID: " << id << std::endl;
            }
        }
        std::cout << "No more acknowledgments to read." << std::endl;
    } catch (const std::exception& e) {
        std::cerr << "Exception in readFromStream: " << e.what() << std::endl;
    }
}

bool RainStormClient::SendDataChunksLeader(shared_ptr<SafeQueue<vector<KVStruct>>> queue, unordered_set<int>& acked_ids, mutex& acked_ids_mutex, string job_id) {
    grpc::ClientContext context;
    auto stream = stub_->SendDataChunksToLeader(&context);

    // Writer thread
    thread writer_thread([&] {
        try {
        {
            rainstorm::StreamDataChunkLeader init_chunk;
            auto* first_chunk = init_chunk.add_chunks();
            first_chunk->set_job_id(job_id);
            cout << "here 1" << job_id << endl;
            if (!stream->Write(init_chunk)) {
                cerr << "Initial Write failed." << endl;
                return;
            }
        }

        cout << "here 2" << endl;
        while (true) {
            vector<KVStruct> kv_pairs;
            if (queue->dequeue(kv_pairs)) {
                rainstorm::StreamDataChunkLeader data_chunk_msg;
                for (const auto& kv : kv_pairs) {
                    cout << kv.id << ":" << kv.key << ":" << kv.value  << ":" << kv.task_index << endl;
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
        cout << "stream writes done" << endl;
        stream->WritesDone();
        } catch (const std::exception& e) {
            std::cout << "writer thread exception: " << e.what() << std::endl;
        }
    });

    // Reader thread
    thread reader_thread([&] {
        rainstorm::AckDataChunk server_chunk;
        cout << "here 3" << endl;
        while (stream->Read(&server_chunk)) {
            cout << "here 4" << endl;
            lock_guard<mutex> lock(acked_ids_mutex);
            for (auto id : server_chunk.id()) {
                acked_ids.insert(id);
            }
        }
    });

    cout << "called early" << endl;

    writer_thread.join();
    reader_thread.join();

    grpc::Status status = stream->Finish();
    return status.ok();
}
