#pragma once
#include <iostream>
#include <fstream>
#include <grpcpp/grpcpp.h>
#include <filesystem>

#include "hydfs.grpc.pb.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;
using grpc::ServerReader;
using grpc::ServerWriter;
using filetransfer::FileTransferService;
using filetransfer::FileChunk;
using filetransfer::UploadStatus;
using filetransfer::DownloadRequest;

#define GRPC_PORT 8081

// Server stores file in directory called hydfs/


class FileTransferServiceImpl final : public FileTransferService::Service {
public:
  Status CreateFile(ServerContext* context, ServerReader<FileChunk>* reader, UploadStatus* response) override {
    FileChunk chunk;
    std::ofstream outfile;
    bool firstChunk = true;

    // also check if the file already exists, if it does then ignore 

    // read in file and write to disk
    while (reader->Read(&chunk)) {
      if (firstChunk) {
        std::string filename = chunk.filename();
        if (filename.empty()) {
          response->set_success(false);
          response->set_message("Filename is missing in the first chunk.");
          return Status::OK;
        }
        // create dir if not exists called hydfs
        std::filesystem::create_directory("hydfs");
        outfile.open("hydfs/" + filename, std::ios::binary);
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

  Status GetFile(ServerContext* context, const DownloadRequest* request, ServerWriter<FileChunk>* writer) override {
    std::string filename = request->filename();
    std::ifstream infile("hydfs/" + filename, std::ios::binary);

    if (!infile) {
      return Status::CANCELLED;  // file not found
    }

    FileChunk chunk;
    const size_t buffer_size = 1024 * 1024; // 1MB
    char buffer[buffer_size];

    while (infile.read(buffer, buffer_size) || infile.gcount() > 0) {
      chunk.set_content(buffer, infile.gcount());
      if (!writer->Write(chunk)) {    
        std::cout << "Failed writing" << std::endl;
        return Status::CANCELLED;  // failed write 
      }
    }
    infile.close();
    return Status::OK;
  }

};

void RunServer() {

    char hostname[256]; 
    if (gethostname(hostname, sizeof(hostname)) != 0) {
        perror("gethostname"); // Print error message if gethostname fails
        exit(1);
    } 
    
    std::string hostname_str = hostname;

  // Using port 8081 for grpc servers
  std::string server_address(hostname_str + ":" + std::to_string(GRPC_PORT));
  FileTransferServiceImpl service;

  ServerBuilder builder;
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  builder.RegisterService(&service);

  std::unique_ptr<Server> server(builder.BuildAndStart());
  std::cout << "gRPC Server listening on " << server_address << std::endl;

  server->Wait();
}