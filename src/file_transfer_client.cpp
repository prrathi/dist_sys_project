#include "file_transfer_client.h"
#include <chrono>

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;
using grpc::ClientWriter;
using grpc::ClientReader;
using filetransfer::FileTransferService;
using filetransfer::FileChunk;
using filetransfer::OperationStatus;
using filetransfer::GetRequest;
using filetransfer::FileRequest;

using namespace std;

FileTransferClient::FileTransferClient(shared_ptr<Channel> channel)
    : stub_(FileTransferService::NewStub(channel)) {}

bool FileTransferClient::CreateFile(const string& file_path, const string& hydfs_filename) {
    ClientContext context;
    OperationStatus status;
    int timeout = 2000;
    std::chrono::system_clock::time_point deadline = std::chrono::system_clock::now() + std::chrono::milliseconds(timeout);
    context.set_deadline(deadline);
    
    unique_ptr<ClientWriter<FileChunk>> writer(stub_->OverwriteFile(&context, &status));

    ifstream infile(file_path, ios::binary);
    if (!infile) {
        cout << "Failed to open file: " << file_path << "\n";
        return false;
    }

    FileChunk chunk;
    chunk.set_filename(hydfs_filename);
    writer->Write(chunk);
    chunk.clear_filename();

    const size_t buffer_size = 1024 * 1024;
    char buffer[buffer_size];
    while (infile.read(buffer, buffer_size) || infile.gcount() > 0) {
        chunk.set_content(buffer, infile.gcount());
        std::chrono::system_clock::time_point deadline = std::chrono::system_clock::now() + std::chrono::milliseconds(timeout);
        context.set_deadline(deadline);
        if (!writer->Write(chunk)) {
            cout << "Failed to write chunk to stream." << "\n";
            break;
        }
    }
    
    infile.close();
    writer->WritesDone();

    Status rpc_status = writer->Finish();
    if (rpc_status.ok() && status.success()) {
        std::cout << "File uploaded successfully: " << status.message() << std::endl;
        return true;
    } else if (rpc_status.error_code() == grpc::StatusCode::DEADLINE_EXCEEDED) {
        std::cout << "File upload failed due to timeout: " << rpc_status.error_message() << std::endl;
        return false;
    } else {
        std::cout << "File upload failed: " << status.message() << std::endl;
        return false;
    }
}

bool FileTransferClient::GetFile(const std::string& hydfs_filename, const std::string& local_filepath) {
    ClientContext context;
    GetRequest request;
    request.set_filename(hydfs_filename);
    request.set_from_leader(false);

    std::ofstream outfile(local_filepath, std::ios::binary);
    if (!outfile) {
        std::cerr << "Failed to open local file for writing: " << local_filepath << std::endl;
        return false;
    }

    int timeout = 30000;
    std::chrono::system_clock::time_point deadline = std::chrono::system_clock::now() + std::chrono::milliseconds(timeout);
    context.set_deadline(deadline);

    FileChunk chunk;
    std::unique_ptr<ClientReader<FileChunk>> reader(stub_->GetFile(&context, request));

    while (reader->Read(&chunk)) {
        outfile.write(chunk.content().data(), chunk.content().size());
    }

    outfile.close(); 
    Status status = reader->Finish();
    if (status.ok()) {
        std::cout << "File downloaded successfully." << std::endl;
        return true;
    } else if(status.error_code() == grpc::StatusCode::CANCELLED) {
        std::cout << "Issue with downloading file: " << status.error_message() << std::endl;
    } else if (status.error_code() == grpc::StatusCode::DEADLINE_EXCEEDED) {
        std::cout << "File download failed due to timeout: " << status.error_message() << std::endl;
        return false;
    } else {
        std::cout << "File download failed: " << status.error_message() << std::endl;
        return false;
    }
    return true;
}

bool FileTransferClient::AppendFile(const string& file_path, const string& hydfs_filename) {
    ClientContext context;
    OperationStatus status;
    int timeout = 2000;
    std::chrono::system_clock::time_point deadline = std::chrono::system_clock::now() + std::chrono::milliseconds(timeout);
    context.set_deadline(deadline);
    
    unique_ptr<ClientWriter<FileChunk>> writer(stub_->AppendFile(&context, &status));

    ifstream infile(file_path, ios::binary);
    if (!infile) {
        cout << "Failed to open file: " << file_path << "\n";
        return false;
    }

    FileChunk chunk;
    chunk.set_filename(hydfs_filename);
    writer->Write(chunk);
    chunk.clear_filename();

    const size_t buffer_size = 1024 * 1024;
    char buffer[buffer_size];
    while (infile.read(buffer, buffer_size) || infile.gcount() > 0) {
        chunk.set_content(buffer, infile.gcount());
        std::chrono::system_clock::time_point deadline = std::chrono::system_clock::now() + std::chrono::milliseconds(timeout);
        context.set_deadline(deadline);
        if (!writer->Write(chunk)) {
            cout << "Append: Failed to write chunk to stream." << "\n";
            break;
        }
    }
    
    infile.close();
    writer->WritesDone();

    Status rpc_status = writer->Finish();
    if (rpc_status.ok() && status.success()) {
        std::cout << "Append uploaded successfully: " << status.message() << std::endl;
        return true;
    } else if (rpc_status.error_code() == grpc::StatusCode::DEADLINE_EXCEEDED) {
        std::cout << "Append upload failed due to timeout: " << rpc_status.error_message() << std::endl;
        return false;
    } else {
        std::cout << "Append upload failed: " << status.message() << std::endl;
        return false;
    }
}