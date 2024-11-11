#include <chrono>
#include <thread>
#include "file_transfer_client.h"
static const int TIMEOUT_MS = 2000;
static const size_t BUFFER_SIZE = 1024 * 1024;  // 1MB buffer

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;
using grpc::ClientWriter;
using grpc::ClientReader;
using filetransfer::FileTransferService;
using filetransfer::StatusCode;
using filetransfer::Chunk;
using filetransfer::OperationStatus;
using filetransfer::FileRequest;
using filetransfer::FileOrderRequest;
using filetransfer::AppendRequest;
using filetransfer::GetResponse;
using filetransfer::MergeRequest;
using filetransfer::OverwriteRequest;
using filetransfer::ReplicationRequest;
using filetransfer::UpdateOrderRequest;

using namespace std;

FileTransferClient::FileTransferClient(shared_ptr<Channel> channel)
    : stub_(FileTransferService::NewStub(channel)) {}

bool FileTransferClient::CreateFile(const string& hydfs_filename, int order) {
    ClientContext context;
    OperationStatus status;
    FileOrderRequest request;
    request.set_filename(hydfs_filename);
    request.set_order(order);
    
    chrono::system_clock::time_point deadline = 
        chrono::system_clock::now() + chrono::milliseconds(TIMEOUT_MS);
    context.set_deadline(deadline);
    
    int max_retries = 3;
    int retry_count = 0;
    Status rpc_status;
    
    while (retry_count < max_retries) {
        rpc_status = stub_->CreateFile(&context, request, &status);
        if (rpc_status.ok()) break;
        retry_count++;
        this_thread::sleep_for(chrono::milliseconds(100));
    }
    
    if (rpc_status.ok() && status.status() == StatusCode::SUCCESS) {
        cout << "File created successfully: " << status.message() << endl;
        return true;
    } else if (status.status() == StatusCode::ALREADY_EXISTS) {
        cout << "File already exists: " << hydfs_filename << endl;
        return false;
    } else {
        cout << "Failed to create file: " << status.message() << endl;
        return false;
    }
}

bool FileTransferClient::AppendFile(const string& file_path, const string& hydfs_filename) {
    ClientContext context;
    OperationStatus status;
    chrono::system_clock::time_point deadline = 
        chrono::system_clock::now() + chrono::milliseconds(TIMEOUT_MS * 5);
    context.set_deadline(deadline);
    
    int max_retries = 3;
    int retry_count = 0;
    unique_ptr<ClientWriter<AppendRequest>> writer;
    
    while (retry_count < max_retries) {
        writer = stub_->AppendFile(&context, &status);
        if (writer) break;
        retry_count++;
        this_thread::sleep_for(chrono::milliseconds(100));
    }

    if (!writer) {
        cout << "Failed to establish connection for append" << "\n";
        return false;
    }

    ifstream infile(file_path, ios::binary);
    if (!infile) {
        cout << "Failed to open file: " << file_path << "\n";
        return false;
    }

    // Send filename first
    AppendRequest metadata_msg;
    metadata_msg.mutable_file_request()->set_filename(hydfs_filename);
    if (!writer->Write(metadata_msg)) {
        cout << "Failed to write metadata append" << "\n";
        return false;
    }

    // Send chunks
    char buffer[BUFFER_SIZE];
    while (infile.read(buffer, BUFFER_SIZE) || infile.gcount() > 0) {
        AppendRequest chunk_msg;
        chunk_msg.mutable_chunk()->set_content(buffer, infile.gcount());
        if (!writer->Write(chunk_msg)) {
            cout << "Failed to write chunk to stream." << "\n";
            break;
        }
    }
    
    infile.close();
    writer->WritesDone();

    Status rpc_status = writer->Finish();
    if (rpc_status.ok() && status.status() == StatusCode::SUCCESS) {
        cout << "File uploaded successfully: " << status.message() << endl;
        return true;
    } else if (rpc_status.error_code() == grpc::StatusCode::DEADLINE_EXCEEDED) {
        cout << "File upload failed due to timeout: " << rpc_status.error_message() << endl;
        return false;
    } else {
        cout << "File upload failed: " << status.message() << endl;
        return false;
    }
}

bool FileTransferClient::GetFile(const string& hydfs_filename, const string& local_filepath) {
    FileRequest request;
    request.set_filename(hydfs_filename);
    
    ClientContext context;
    chrono::system_clock::time_point deadline = 
        chrono::system_clock::now() + chrono::milliseconds(TIMEOUT_MS * 5);
    context.set_deadline(deadline);
    
    GetResponse response;
    int max_retries = 3;
    int retry_count = 0;
    unique_ptr<ClientReader<GetResponse>> reader;
    
    while (retry_count < max_retries) {
        reader = stub_->GetFile(&context, request);
        if (reader) break;
        retry_count++;
        this_thread::sleep_for(chrono::milliseconds(100));
    }
    
    if (!reader) {
        cerr << "Failed to establish connection for get" << endl;
        return false;
    }
    
    // Read first response to check status
    if (!reader->Read(&response)) {
        cerr << "Failed to read response" << endl;
        return false;
    }
    
    if (response.has_status()) {
        if (response.status().status() == StatusCode::NOT_FOUND) {
            cerr << "File not found: " << response.status().message() << endl;
            return false;
        } else if (response.status().status() == StatusCode::SUCCESS) {
            return true;
        }
    }
    
    // Open output file
    ofstream outfile(local_filepath, ios::binary);
    if (!outfile) {
        cerr << "Failed to open output file: " << local_filepath << endl;
        return false;
    }

    // Write first chunk if it was data
    if (response.has_chunk()) {
        outfile.write(response.chunk().content().data(), response.chunk().content().size());
    }
    
    // Read remaining responses with timeout handling
    while (reader->Read(&response)) {
        if (response.has_status()) {
            if (response.status().status() == StatusCode::SUCCESS) {
                break;
            } else {
                cerr << "Error: " << response.status().message() << endl;
                outfile.close();
                return false;
            }
        }
        outfile.write(response.chunk().content().data(), response.chunk().content().size());
    }
    
    outfile.close();
    
    Status status = reader->Finish();
    if (!status.ok()) {
        cerr << "RPC failed: " << status.error_message() << endl;
        return false;
    }
    
    return true;
}

bool FileTransferClient::MergeFile(const string& hydfs_filename, const vector<string>& successors) {
    ClientContext context;
    OperationStatus status;
    MergeRequest request;
    request.set_filename(hydfs_filename);
    for (const string& successor : successors) {
        request.add_successors(successor);
    }
    
    chrono::system_clock::time_point deadline = 
        chrono::system_clock::now() + chrono::milliseconds(TIMEOUT_MS * 10);
    context.set_deadline(deadline);
    
    int max_retries = 3;
    int retry_count = 0;
    Status rpc_status;
    
    while (retry_count < max_retries) {
        rpc_status = stub_->MergeFile(&context, request, &status);
        if (rpc_status.ok()) break;
        retry_count++;
        this_thread::sleep_for(chrono::milliseconds(100));
    }

    if (rpc_status.ok() && status.status() == StatusCode::SUCCESS) {
        return true;
    } else {
        cout << "Failed to merge file: " << status.message() << "\n";
        cout << "RPC status: " << rpc_status.error_message() << "\n";
        return false;
    }
}

bool FileTransferClient::OverwriteFile(const string& local_hydfs_filepath, const string& hydfs_filename, int order) {
    ClientContext context;
    OperationStatus status;
    
    chrono::system_clock::time_point deadline = 
        chrono::system_clock::now() + chrono::milliseconds(TIMEOUT_MS * 5);
    context.set_deadline(deadline);
    
    int max_retries = 3;
    int retry_count = 0;
    unique_ptr<ClientWriter<OverwriteRequest>> writer;
    
    while (retry_count < max_retries) {
        writer = stub_->OverwriteFile(&context, &status);
        if (writer) break;
        retry_count++;
        this_thread::sleep_for(chrono::milliseconds(100));
    }

    if (!writer) {
        cout << "Failed to establish connection for overwrite" << "\n";
        return false;
    }

    ifstream infile(local_hydfs_filepath, ios::binary);
    if (!infile) {
        cout << "Failed to open file: " << local_hydfs_filepath << "\n";
        return false;
    }

    // Send file metadata first
    OverwriteRequest metadata_msg;
    auto* file_request = metadata_msg.mutable_file_request();
    file_request->set_filename(hydfs_filename);
    file_request->set_order(order);
    if (!writer->Write(metadata_msg)) {
        cout << "Failed to write metadata overwrite" << "\n";
        return false;
    }

    // Send file contents in chunks
    char buffer[BUFFER_SIZE];
    while (infile.read(buffer, BUFFER_SIZE) || infile.gcount() > 0) {
        OverwriteRequest chunk_msg;
        auto* chunk = chunk_msg.mutable_chunk();
        chunk->set_content(buffer, infile.gcount());
        if (!writer->Write(chunk_msg)) {
            cout << "Failed to write chunk to stream." << "\n";
            break;
        }
    }
    
    infile.close();
    writer->WritesDone();

    Status rpc_status = writer->Finish();
    if (rpc_status.ok() && status.status() == StatusCode::SUCCESS) {
        return true;
    } else {
        cout << "Failed to overwrite file: " << status.message() << "\n";
        return false;
    }
}

bool FileTransferClient::UpdateReplication(int failure_case, const string& existing_successor, const vector<string>& new_successors) {
    ClientContext context;
    OperationStatus status;
    ReplicationRequest request;
    
    chrono::system_clock::time_point deadline = 
        chrono::system_clock::now() + chrono::milliseconds(TIMEOUT_MS * 2);
    context.set_deadline(deadline);
    
    request.set_failure_case(failure_case);
    
    if (!existing_successor.empty()) {
        request.set_existing_successor(existing_successor);
    }
    
    for (const auto& successor : new_successors) {
        request.add_new_successors(successor);
    }
    
    int max_retries = 3;
    int retry_count = 0;
    Status rpc_status;
    
    while (retry_count < max_retries) {
        rpc_status = stub_->UpdateFilesReplication(&context, request, &status);
        if (rpc_status.ok()) break;
        retry_count++;
        this_thread::sleep_for(chrono::milliseconds(100));
    }
    
    if (rpc_status.ok() && status.status() == StatusCode::SUCCESS) {
        return true;
    } else {
        cout << "Failed to update replication: " << status.message() << endl;
        return false;
    }
}

bool FileTransferClient::UpdateOrder(int old_order, int new_order) {
    ClientContext context;
    OperationStatus status;
    UpdateOrderRequest request;
    
    chrono::system_clock::time_point deadline = 
        chrono::system_clock::now() + chrono::milliseconds(TIMEOUT_MS);
    context.set_deadline(deadline);
    
    request.set_old_order(old_order);
    request.set_new_order(new_order);
    
    int max_retries = 3;
    int retry_count = 0;
    Status rpc_status;
    
    while (retry_count < max_retries) {
        rpc_status = stub_->UpdateOrder(&context, request, &status);
        if (rpc_status.ok()) break;
        retry_count++;
        this_thread::sleep_for(chrono::milliseconds(100));
    }
    
    if (rpc_status.ok() && status.status() == StatusCode::SUCCESS) {
        return true;
    } else {
        cout << "Failed to update order: " << status.message() << endl;
        return false;
    }
}