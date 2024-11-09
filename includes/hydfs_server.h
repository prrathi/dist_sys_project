#ifndef HYDFS_SERVER_H
#define HYDFS_SERVER_H

#include <string>
#include <chrono>
#include <utility>
#include <vector>
#include <unordered_map>
#include <memory>
#include <grpcpp/grpcpp.h>
#include "hydfs.grpc.pb.h"
#include <filesystem>

class HydfsServer : public filetransfer::FileTransferService::Service {
public:
    HydfsServer();
    ~HydfsServer();

    void wait();

    // Check if server has file
    grpc::Status FileExists(grpc::ServerContext* context,
                           const filetransfer::FileRequest* request,
                           filetransfer::FileExistsStatus* response) override;

    // Notify server that it is leader for new file 
    grpc::Status CreateFileLeader(grpc::ServerContext* context,
                                 const filetransfer::FileRequest* request,
                                 filetransfer::OperationStatus* response) override;

    // Append content to server's file, creating new if not exists
    grpc::Status AppendFile(grpc::ServerContext* context,
                           grpc::ServerReader<filetransfer::FileChunk>* reader,
                           filetransfer::OperationStatus* response) override;

    // Get combined content from server's file
    grpc::Status GetFile(grpc::ServerContext* context,
                        const filetransfer::GetRequest* request,
                        grpc::ServerWriter<filetransfer::FileChunk>* writer) override;

    // Merge file, server is guaranteed to be the leader
    grpc::Status MergeFile(grpc::ServerContext* context,
                          const filetransfer::FileRequest* request,
                          filetransfer::OperationStatus* response) override;

    // Write (if not exists) or replace (if exists) server's file with received content 
    grpc::Status OverwriteFile(grpc::ServerContext* context,
                              grpc::ServerReader<filetransfer::FileChunk>* reader,
                              filetransfer::OperationStatus* response) override;

    // Forward all files that server is leader of to new successor
    grpc::Status ForwardLeaderFiles(grpc::ServerContext* context,
                                   const filetransfer::ForwardRequest* request,
                                   filetransfer::OperationStatus* response) override;

private:
    // Merge chunks into original filename and remove chunks
    void merge_file(const std::string& filename);

    std::unique_ptr<grpc::Server> server_;
    // Files that server is leader of
    std::unordered_set<std::string> leader_files;
    // Chunked filenames for original filename, note that merged content is stored at original filename
    std::unordered_map<std::string, std::vector<std::string>> file_map;
};

#endif // HYDFS_SERVER_H
