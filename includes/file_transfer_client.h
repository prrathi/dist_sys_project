#pragma once
#include <iostream>
#include <fstream>
#include <memory>
#include <grpcpp/grpcpp.h>
#include "hydfs.grpc.pb.h"

class FileTransferClient {
public:
    FileTransferClient(std::shared_ptr<grpc::Channel> channel);
    bool CreateFile(const std::string& file_path, const std::string& hydfs_filename);
    bool GetFile(const std::string& hydfs_filename, const std::string& local_filepath);
    bool AppendFile(const std::string& file_path, const std::string& hydfs_filename);

private:
    std::unique_ptr<filetransfer::FileTransferService::Stub> stub_;
}; 