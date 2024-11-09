#include <grpcpp/grpcpp.h>
#include <iostream>
#include <fstream>
#include <filesystem>
#include "hydfs_server.h"
#include "file_transfer_client.h"

#define GRPC_PORT 8081

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;
using grpc::ServerReader;
using grpc::ServerWriter;
using filetransfer::FileChunk;
using filetransfer::OperationStatus;
using filetransfer::GetRequest;
using filetransfer::FileRequest;
using filetransfer::FileExistsStatus;
using filetransfer::ForwardRequest;

HydfsServer::HydfsServer() {
    char hostname[256];
    if (gethostname(hostname, sizeof(hostname)) != 0) {
        perror("gethostname");
        exit(1);
    }
    std::string hostname_str = hostname;
    std::string server_address(hostname_str + ":" + std::to_string(GRPC_PORT));

    ServerBuilder builder;
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(this);

    server_ = builder.BuildAndStart();
    std::cout << "gRPC Server listening on " << server_address << std::endl;

    std::filesystem::create_directory("hydfs");
}

HydfsServer::~HydfsServer() {
    if (server_) {
        server_->Shutdown();
    }
}

void HydfsServer::wait() {
    if (server_) {
        server_->Wait();
    }
}

Status HydfsServer::FileExists(ServerContext* context, const FileRequest* request, FileExistsStatus* response) {
    std::string filename = request->filename();
    bool exists = std::filesystem::exists("hydfs/" + filename);
    response->set_exists(exists);
    return Status::OK;
}

Status HydfsServer::CreateFileLeader(ServerContext* context, const FileRequest* request, OperationStatus* response) {
    // Boilerplate
    response->set_success(false);
    response->set_message("Not implemented");
    return Status::OK;
}

Status HydfsServer::AppendFile(ServerContext* context, ServerReader<FileChunk>* reader, OperationStatus* response) {
    FileChunk chunk;
    std::ofstream outfile;
    bool firstChunk = true;

    while (reader->Read(&chunk)) {
        if (firstChunk) {
            std::string filename = chunk.filename();
            if (filename.empty()) {
                response->set_success(false);
                response->set_message("Append: filename is missing in the first chunk.");
                return Status::OK;
            }
            outfile.open("hydfs/" + filename, std::ios::binary | std::ios::app);
            if (!outfile) {
                response->set_success(false);
                response->set_message("Append: Failed to open file for writing: " + filename);
                return Status::OK;
            }
            firstChunk = false;
        }
        outfile.write(chunk.content().data(), chunk.content().size());
    }

    if (outfile.is_open()) {
        outfile.close();
    }
    response->set_success(true);
    response->set_message("File appended successfully.");
    return Status::OK;
}

Status HydfsServer::GetFile(ServerContext* context, const GetRequest* request, ServerWriter<FileChunk>* writer) {
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
            return Status::CANCELLED;
        }
    }
    infile.close();
    return Status::OK;
}

Status HydfsServer::MergeFile(ServerContext* context, const FileRequest* request, OperationStatus* response) {
    // Boilerplate
    response->set_success(false);
    response->set_message("Not implemented");
    return Status::OK;
}

Status HydfsServer::OverwriteFile(ServerContext* context, ServerReader<FileChunk>* reader, OperationStatus* response) {
    // Boilerplate
    response->set_success(false);
    response->set_message("Not implemented");
    return Status::OK;
}

Status HydfsServer::ForwardLeaderFiles(ServerContext* context, const ForwardRequest* request, OperationStatus* response) {
    // Boilerplate
    response->set_success(false);
    response->set_message("Not implemented");
    return Status::OK;
}