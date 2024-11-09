#pragma once

#include <iostream>
#include <fstream>
#include <memory>
#include <grpcpp/grpcpp.h>
#include "hydfs.grpc.pb.h"

class FileTransferClient {
public:
    FileTransferClient(std::shared_ptr<grpc::Channel> channel);
    bool CreateFile(const std::string& hydfs_filename, int order);
    bool AppendFile(const std::string& local_filepath, const std::string& hydfs_filename);
    bool GetFile(const std::string& hydfs_filename, const std::string& local_filepath);
    bool MergeFile(const std::string& hydfs_filename);
    bool OverwriteFile(const std::string& local_hydfs_filepath, const std::string& hydfs_filename, int order);
    bool UpdateReplication(int failure_case, const std::string& existing_successor, const std::vector<std::string>& new_successors);
    bool UpdateOrder(int order, int new_order);

private:
    std::unique_ptr<filetransfer::FileTransferService::Stub> stub_;
}; 