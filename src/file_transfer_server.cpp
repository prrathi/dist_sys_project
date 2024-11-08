#pragma once
#include <iostream>
#include <fstream>
#include <grpcpp/grpcpp.h>
#include "hydfs.grpc.pb.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;
using grpc::ServerReader;
using filetransfer::FileTransferService;
using filetransfer::FileChunk;
using filetransfer::UploadStatus;

class FileTransferServiceImpl final : public FileTransferService::Service {
public:
  Status CreateFile(ServerContext* context, ServerReader<FileChunk>* reader, UploadStatus* response) override {
    FileChunk chunk;
    std::ofstream outfile;
    bool firstChunk = true;

    // read in file and write to disk
    while (reader->Read(&chunk)) {
      if (firstChunk) {
        std::string filename = chunk.filename();
        if (filename.empty()) {
          response->set_success(false);
          response->set_message("Filename is missing in the first chunk.");
          return Status::OK;
        }
        outfile.open(filename, std::ios::binary);
        if (!outfile) {
          response->set_success(false);
          response->set_message("Failed to open file for writing: " + filename);
          return Status::OK;
        }
        firstChunk = false;
      }
      outfile.write(chunk.content().data(), chunk.content().size());
    }
    // handle logic here?


    if (outfile.is_open()) {
      outfile.close();
    }
    response->set_success(true);
    response->set_message("File received successfully.");
    return Status::OK;
  }
};

void RunServer() {
  std::string server_address("0.0.0.0:12345");
  FileTransferServiceImpl service;

  ServerBuilder builder;
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  builder.RegisterService(&service);

  std::unique_ptr<Server> server(builder.BuildAndStart());
  std::cout << "Server listening on " << server_address << std::endl;

  server->Wait();
}