#pragma once

#include <string>
#include <utility>
#include <vector>
#include <unordered_map>
#include <memory>
#include <mutex>
#include <array>
#include <grpcpp/grpcpp.h>
#include "hydfs.grpc.pb.h"

static const size_t SHARD_COUNT = 32;  // Number of mutex shards, must be power of 2

class HydfsServer : public filetransfer::FileTransferService::Service {
public:
    HydfsServer();
    ~HydfsServer();

    void wait();

    // "Create" file on leader server if doesn't exist
    grpc::Status CreateFile(grpc::ServerContext* context,
                            const filetransfer::FileOrderRequest* request,
                            filetransfer::OperationStatus* response) override;

    // Append content to server's file, creating new if not exists
    grpc::Status AppendFile(grpc::ServerContext* context,
                           grpc::ServerReader<filetransfer::AppendRequest>* reader,
                           filetransfer::OperationStatus* response) override;

    // Get combined content from server's file
    grpc::Status GetFile(grpc::ServerContext* context,
                        const filetransfer::FileRequest* request,
                        grpc::ServerWriter<filetransfer::GetResponse>* writer) override;

    // Merge file, server is guaranteed to be the leader
    grpc::Status MergeFile(grpc::ServerContext* context,
                          const filetransfer::MergeRequest* request,
                          filetransfer::OperationStatus* response) override;

    // Write (if not exists) or replace (if exists) server's file with received content 
    grpc::Status OverwriteFile(grpc::ServerContext* context,
                              grpc::ServerReader<filetransfer::OverwriteRequest>* reader,
                              filetransfer::OperationStatus* response) override;

    // Update replication of all files that server is leader of
    grpc::Status UpdateFilesReplication(grpc::ServerContext* context, 
                                      const filetransfer::ReplicationRequest* request, 
                                      filetransfer::OperationStatus* response) override;

    // Update order for all matching files 
    grpc::Status UpdateOrder(grpc::ServerContext* context, 
                           const filetransfer::UpdateOrderRequest* request, 
                           filetransfer::OperationStatus* response) override;

private:
    // Helper to get the correct mutex for a filename
    size_t get_shard_index(const std::string& filename) {
        return std::hash<std::string>{}(filename) & (SHARD_COUNT - 1);
    }

    std::string server_address_;
    std::unique_ptr<grpc::Server> server_;
    // Each HYDFS filename stores node's order and chunked filenames ordered by time. Note that merged content is stored at original filename
    std::unordered_map<std::string, std::pair<int32_t, std::vector<std::string>>> file_map_;
    // File map and its sharded mutexes
    std::array<std::mutex, SHARD_COUNT> shard_mutexes_;
};
