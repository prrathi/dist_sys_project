#include <iostream>
#include <fstream>
#include <grpcpp/grpcpp.h>
#include <filesystem>

#include "hydfs.grpc.pb.h"
#include "file_transfer_server.h"

using namespace std;
using grpc::ServerContext;
using grpc::Status;
using grpc::ServerReader;
using grpc::ServerWriter;
using filetransfer::FileChunk;
using filetransfer::UploadStatus;
using filetransfer::DownloadRequest;

FileTransferServiceImpl::FileTransferServiceImpl() {}

Status FileTransferServiceImpl::CreateFile(ServerContext* context, ServerReader<FileChunk>* reader, UploadStatus* response) {
    FileChunk chunk;
    ofstream outfile;
    bool firstChunk = true;
    
    while (reader->Read(&chunk)) {
        if (firstChunk) {
            string filename = chunk.filename();
            if (filename.empty()) {
                response->set_success(false);
                response->set_message("Filename is missing in the first chunk.");
                return Status::OK;
            }
            filesystem::create_directory("hydfs");
            outfile.open("hydfs/" + filename, ios::binary);
            if (!outfile) {
                response->set_success(false);
                response->set_message("Failed to open file for writing: " + filename);
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
    response->set_message("File received successfully.");
    return Status::OK;
}

Status FileTransferServiceImpl::GetFile(ServerContext* context, const DownloadRequest* request, ServerWriter<FileChunk>* writer) {
    string filename = request->filename();
    ifstream infile("hydfs/" + filename, ios::binary);

    if (!infile) {
        return Status::CANCELLED;  // file not found
    }

    FileChunk chunk;
    const size_t buffer_size = 1024 * 1024; // 1MB
    char buffer[buffer_size];

    while (infile.read(buffer, buffer_size) || infile.gcount() > 0) {
        chunk.set_content(buffer, infile.gcount());
        if (!writer->Write(chunk)) {    
            cout << "Failed writing" << endl;
            return Status::CANCELLED;  // failed write 
        }
    }
    infile.close();
    return Status::OK;
}