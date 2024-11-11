#include <iostream>
#include <fstream>
#include <filesystem>
#include <chrono>
#include <iomanip>
#include <sstream>
#include <random>
#include <thread>
#include <grpcpp/grpcpp.h>
#include "hydfs_server.h"
#include "file_transfer_client.h"

static const int GRPC_PORT_SERVER = 8081;
static const int GRPC_PORT_SERVER_2 = 8082;
static const size_t BUFFER_SIZE = 1024 * 1024;  // 1MB buffer
static const int SERVER_TIMEOUT_MS = 5000;  // 5 second server timeout
static const int MAX_RETRIES = 3;
static const int RETRY_DELAY_MS = 100;

using namespace std;

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;
using grpc::ServerReader;
using grpc::ServerWriter;
using filetransfer::StatusCode;
using filetransfer::OperationStatus;
using filetransfer::Chunk;
using filetransfer::FileRequest;
using filetransfer::FileOrderRequest;
using filetransfer::AppendRequest;
using filetransfer::GetResponse;
using filetransfer::MergeRequest;
using filetransfer::OverwriteRequest;
using filetransfer::ReplicationRequest;
using filetransfer::UpdateOrderRequest;

HydfsServer::HydfsServer() {
    char hostname[256];
    if (gethostname(hostname, sizeof(hostname)) != 0) {
        perror("gethostname");
        exit(1);
    }
    string hostname_str = hostname;
    server_address_ = hostname_str + ":" + to_string(GRPC_PORT_SERVER);
    server_address_2_ = hostname_str + ":" + to_string(GRPC_PORT_SERVER_2);

    ServerBuilder builder;
    builder.AddListeningPort(server_address_, grpc::InsecureServerCredentials());
    builder.RegisterService(this);
    server_ = builder.BuildAndStart();

    ServerBuilder builder_2;
    builder_2.AddListeningPort(server_address_2_, grpc::InsecureServerCredentials());
    builder_2.RegisterService(this);
    server_2_ = builder_2.BuildAndStart();

    cout << "gRPC Server listening on " << server_address_ << endl;
    cout << "gRPC Server 2 listening on " << server_address_2_ << endl;
    // if (filesystem::exists("hydfs/")) {
    //    filesystem::remove_all("hydfs/");
    // }
    // filesystem::create_directory("hydfs");
    for (const auto&entry : filesystem::directory_iterator("hydfs")) {
	string filename = entry.path().filename().string();
	file_map_[filename] = make_pair(0, vector<string>());
    }
}

HydfsServer::~HydfsServer() {
    if (server_) {
        server_->Shutdown();
        server_2_->Shutdown();
    }
}

void HydfsServer::wait() {
    if (server_) {
        server_->Wait();
        server_2_->Wait();
    }
}

Status HydfsServer::CreateFile(ServerContext* context, const FileOrderRequest* request, OperationStatus* response) {
    string filename = request->filename();
    size_t shard = get_shard_index(filename);
    
    lock_guard<mutex> lock(shard_mutexes_[shard]);
    if (file_map_.find(filename) != file_map_.end()) {
        response->set_status(StatusCode::ALREADY_EXISTS);
        return Status::OK;
    }
    file_map_[filename] = make_pair(request->order(), vector<string>());

    response->set_status(StatusCode::SUCCESS);
    return Status::OK;
}

Status HydfsServer::AppendFile(ServerContext* context, ServerReader<AppendRequest>* reader, OperationStatus* response) {
    AppendRequest msg;
    ofstream outfile;
    string filename;
    string chunk_filename;
    
    // get first message to get filename with retry
    int retry_count = 0;
    bool read_success = false;
    while (retry_count < MAX_RETRIES && !read_success) {
        if (reader->Read(&msg)) {
            read_success = true;
            break;
        }
        retry_count++;
        this_thread::sleep_for(chrono::milliseconds(RETRY_DELAY_MS));
    }

    if (!read_success) {
        response->set_status(StatusCode::INVALID);
        response->set_message("Failed to read initial message after retries");
        return Status::OK;
    }

    cout << "Received write request for " << msg.file_request().filename() << " at " << server_address_ << endl;

    // first message must contain file request
    if (!msg.has_file_request()) {
        response->set_status(StatusCode::INVALID);
        response->set_message("First message must contain file request");
        return Status::OK;
    }
    filename = msg.file_request().filename();
    if (filename.empty()) {
        response->set_status(StatusCode::INVALID);
        response->set_message("Filename is missing in file request");
        return Status::OK;
    }

    // create lock once we have filename
    size_t shard = get_shard_index(filename);
    lock_guard<mutex> lock(shard_mutexes_[shard]);
    
    // file must have been "create"d
    if (file_map_.find(filename) == file_map_.end()) {
        response->set_status(StatusCode::NOT_FOUND);
        return Status::OK;
    }

    // create directory structure if needed
    string full_path = "hydfs/" + filename;
    filesystem::path dir_path = filesystem::path(full_path).parent_path();
    filesystem::create_directories(dir_path);

    // generate random id for chunk
    static random_device rd;
    static mt19937 gen(rd());
    static uniform_int_distribution<> dis(1000, 999999);
    do {
        int random_id = dis(gen);
        chunk_filename = filename + "_" + to_string(random_id);
    } while (filesystem::exists("hydfs/" + chunk_filename));
    outfile.open("hydfs/" + chunk_filename, ios::binary | ios::app);
    if (!outfile) {
        response->set_status(StatusCode::INVALID);
        response->set_message("Could not open file for writing");
        return Status::OK;
    }

    // process remaining messages with timeout handling
    while (true) {
        retry_count = 0;
        read_success = false;
        
        while (retry_count < MAX_RETRIES && !read_success) {
            if (reader->Read(&msg)) {
                read_success = true;
                break;
            }
            if (context->IsCancelled()) {
                response->set_status(StatusCode::INVALID);
                response->set_message("Operation cancelled due to timeout");
                return Status::OK;
            }
            retry_count++;
            this_thread::sleep_for(chrono::milliseconds(RETRY_DELAY_MS));
        }

        if (!read_success) break;  // End of stream or persistent failure

        if (msg.has_chunk()) {
            outfile.write(msg.chunk().content().data(), msg.chunk().content().size());
            if (outfile.fail()) {
                response->set_status(StatusCode::INVALID);
                response->set_message("Failed to write chunk to file");
                return Status::OK;
            }
        }
    }

    outfile.close();

    file_map_[filename].second.push_back(chunk_filename);

    cout << "Completed write request for " << filename << " at " << server_address_ << endl;
    
    response->set_status(StatusCode::SUCCESS);
    return Status::OK;
}

Status HydfsServer::GetFile(ServerContext* context, const FileRequest* request, ServerWriter<GetResponse>* writer) {
    string filename = request->filename();
    cout << "Received read request for " << filename << " at " << server_address_ << endl;
    vector<string> chunk_files;
    
    auto it = file_map_.find(filename);
    if (it == file_map_.end()) {
        GetResponse response;
        OperationStatus* status = response.mutable_status();
        status->set_status(StatusCode::NOT_FOUND);
        writer->Write(response);
        return Status::OK;
    }

    size_t shard = get_shard_index(filename);
    lock_guard<mutex> lock(shard_mutexes_[shard]);
    chunk_files = it->second.second;

    // set up chunks
    char buffer[BUFFER_SIZE];
    GetResponse chunk_response;
    Chunk* chunk = chunk_response.mutable_chunk();

    // Read and send file contents with retry logic
    auto send_chunk = [&](const char* data, size_t size) -> bool {
        chunk->set_content(data, size);
        int retry_count = 0;
        while (retry_count < MAX_RETRIES) {
            if (writer->Write(chunk_response)) return true;
            if (context->IsCancelled()) return false;
            retry_count++;
            this_thread::sleep_for(chrono::milliseconds(RETRY_DELAY_MS));
        }
        return false;
    };

    // First the merged content if it exists
    ifstream infile("hydfs/" + filename, ios::binary);
    if (infile) {
        while (infile.read(buffer, BUFFER_SIZE) || infile.gcount() > 0) {
            if (!send_chunk(buffer, infile.gcount())) {
                return Status::CANCELLED;
            }
        }
        infile.close();
    }

    // Then the new chunks
    for (const string& chunk_filename : chunk_files) {
        ifstream chunk_file("hydfs/" + chunk_filename, ios::binary);
        if (!chunk_file) {
            cout << "Server failed to open chunk: " << chunk_filename << endl;
            continue;
        }

        while (chunk_file.read(buffer, BUFFER_SIZE) || chunk_file.gcount() > 0) {
            if (!send_chunk(buffer, chunk_file.gcount())) {
                return Status::CANCELLED;
            }
        }
        chunk_file.close();
    }

    // Send final success status
    GetResponse final_response;
    OperationStatus* status = final_response.mutable_status();
    status->set_status(StatusCode::SUCCESS);
    
    int retry_count = 0;
    while (retry_count < MAX_RETRIES) {
        if (writer->Write(final_response)) break;
        if (context->IsCancelled()) return Status::CANCELLED;
        retry_count++;
        this_thread::sleep_for(chrono::milliseconds(RETRY_DELAY_MS));
    }

    cout << "Completed read request for " << filename << " at " << server_address_ << endl;

    return Status::OK;
}

Status HydfsServer::MergeFile(ServerContext* context, const MergeRequest* request, OperationStatus* response) {
    string filename = request->filename();
    size_t shard = get_shard_index(filename);
    
    lock_guard<mutex> lock(shard_mutexes_[shard]);
    cout << "MERGINGGGG " << filename << " at " << server_address_ << "\n";

    if (file_map_.find(filename) == file_map_.end()) {
        response->set_status(StatusCode::NOT_FOUND);
        return Status::OK;
    }

    // append to file if exists, otherwise create it
    string full_path = "hydfs/" + filename;
    ofstream outfile(full_path, ios::binary | ios::app);
    if (!outfile) {
        response->set_status(StatusCode::INVALID);
        response->set_message("Failed to merge file");
        return Status::OK;
    }

    // go through each chunk file, append its contents, and remove it
    for (const auto& chunk_filename : file_map_[filename].second) {
        string chunk_path = "hydfs/" + chunk_filename;
        ifstream infile(chunk_path, ios::binary);
        if (!infile) {
            continue;
        }
        outfile << infile.rdbuf();
        infile.close();
        remove(chunk_path.c_str());
    }
    outfile.close();

    // Verify file is readable before forwarding
    ifstream test_read(full_path, ios::binary);
    if (!test_read) {
        response->set_status(StatusCode::INVALID);
        response->set_message("Cannot read merged file: " + full_path);
        return Status::OK;
    }
    test_read.close();

    // clear the chunks vector since we've merged them
    file_map_[filename].second.clear();

    cout << "sending merged " << filename << " to " << request->successors().size() << " successors\n";
    
    // forward the merged file to all successors from the request. hacky but ¯\_(ツ)_/¯
    int successor_order = 3 - request->successors().size();
    for (const string& successor_address : request->successors()) {
        cout << "sending merged " << filename << " to " << successor_address << "\n";
        if (successor_address.empty()) {
            response->set_status(StatusCode::INVALID);
            response->set_message("Successor address is empty");
            return Status::OK;
        }

        int retry_count = 0;
        bool success = false;
        while (retry_count < MAX_RETRIES && !success) {
            auto channel = grpc::CreateChannel(successor_address, grpc::InsecureChannelCredentials());
            FileTransferClient client(channel);
            if (client.OverwriteFile(full_path, filename, successor_order)) {
                success = true;
                break;
            }
            retry_count++;
            this_thread::sleep_for(chrono::milliseconds(RETRY_DELAY_MS));
        }

        if (!success) {
            response->set_status(StatusCode::INVALID);
            response->set_message("Failed to forward to successor after retries");
            return Status::OK;
        }
        successor_order++;
    }

    response->set_status(StatusCode::SUCCESS);
    return Status::OK;
}

Status HydfsServer::OverwriteFile(ServerContext* context, ServerReader<OverwriteRequest>* reader, OperationStatus* response) {
    OverwriteRequest msg;
    string filename;
    int order;

    // Get first message with retries
    int retry_count = 0;
    bool read_success = false;
    while (retry_count < MAX_RETRIES && !read_success) {
        if (reader->Read(&msg)) {
            read_success = true;
            break;
        }
        retry_count++;
        std::this_thread::sleep_for(std::chrono::milliseconds(RETRY_DELAY_MS));
    }

    if (!read_success) {
        response->set_status(StatusCode::INVALID);
        response->set_message("Failed to read initial message after retries");
        return Status::OK;
    }

    // Validate first message
    if (!msg.has_file_request()) {
        response->set_status(StatusCode::INVALID);
        response->set_message("First message must contain file request");
        return Status::OK;
    }

    filename = msg.file_request().filename();
    order = msg.file_request().order();

    if (filename.empty()) {
        response->set_status(StatusCode::INVALID);
        response->set_message("Filename is missing in file request");
        return Status::OK;
    }

    cout << "Received overwrite request for " << filename << " with order " << order << " at " << server_address_ << "\n";

    // Create lock once we have filename
    size_t shard = get_shard_index(filename);
    lock_guard<mutex> lock(shard_mutexes_[shard]);

    // Create directory structure if needed
    string full_path = "hydfs/" + filename;
    filesystem::path dir_path = filesystem::path(full_path).parent_path();
    filesystem::create_directories(dir_path);

    // Open file for writing (truncating any existing content)
    ofstream outfile(full_path, ios::binary | ios::trunc);
    if (!outfile) {
        response->set_status(StatusCode::INVALID);
        response->set_message("Could not open file for writing");
        return Status::OK;
    }

    // Process remaining messages (chunks)
    while (reader->Read(&msg)) {
        if (msg.has_chunk()) {
            outfile.write(msg.chunk().content().data(), msg.chunk().content().size());
            if (outfile.fail()) {
                response->set_status(StatusCode::INVALID);
                response->set_message("Failed to write chunk to file");
                return Status::OK;
            }
        }
    }
    
    outfile.close();

    // Update or create file entry in map
    file_map_[filename] = make_pair(order, vector<string>());

    cout << "Completed overwrite request for " << filename << " at " << server_address_ << endl;
    
    response->set_status(StatusCode::SUCCESS);
    return Status::OK;
}

Status HydfsServer::UpdateFilesReplication(ServerContext* context, const ReplicationRequest* request, OperationStatus* response) {
    vector<string> files_to_forward;
    vector<string> new_successors;
    string existing_successor;
    pair<int, int> existing_order = make_pair(-1, -1);
    int num_preceding_failures = 0;
    
    // First phase: Collect information under lock
    {
        // Lock scope
        vector<unique_lock<mutex>> locks;
        for (size_t i = 0; i < SHARD_COUNT; i++) {
            locks.emplace_back(shard_mutexes_[i]);
        }

        int32_t failure_case = request->failure_case();
        cout << "Handling failure case: " << failure_case << " on " << server_address_ << "\n";
        switch (failure_case) {
            case 1: // 001
                num_preceding_failures = 0;
                break;
            case 2: // 010
                num_preceding_failures = 0;
                existing_order = make_pair(2, 1);
                break;
            case 4: // 100
                num_preceding_failures = 1;
                break;
            default:
                response->set_status(StatusCode::INVALID);
                response->set_message("Invalid failure case");
                return Status::OK;
        }

        // Update orders and collect files that need forwarding
        for (auto& [filename, file_info] : file_map_) {
            int32_t current_order = file_info.first;
            int32_t new_order = max(0, current_order - num_preceding_failures);
            file_info.first = new_order;
            if (new_order == 0 && !request->new_successors().empty()) {
                files_to_forward.push_back(filename);
            }
        }

        // Store information we'll need after releasing locks
        new_successors.assign(request->new_successors().begin(), request->new_successors().end());
        existing_successor = request->existing_successor();
    } // Locks are released here

    // Second phase: Forward files and update orders without holding locks
    if (existing_order.first != -1) {
        auto channel = grpc::CreateChannel(existing_successor, grpc::InsecureChannelCredentials());
        FileTransferClient client(channel);
        client.UpdateOrder(existing_order.first, existing_order.second);
    }

    for (const string& filename : files_to_forward) {
        auto channel = grpc::CreateChannel(server_address_2_, grpc::InsecureChannelCredentials());
        FileTransferClient client(channel);
        client.MergeFile(filename, new_successors);
    }

    response->set_status(StatusCode::SUCCESS);
    return Status::OK;
}

Status HydfsServer::UpdateOrder(ServerContext* context, const UpdateOrderRequest* request, OperationStatus* response) {
    // lock all shards when updating order
    vector<unique_lock<mutex>> locks;
    for (size_t i = 0; i < SHARD_COUNT; i++) {
        locks.emplace_back(shard_mutexes_[i]);
    }

    // Update order for all files that match the old order
    int32_t old_order = request->old_order();
    int32_t new_order = request->new_order();
    for (auto& [filename, file_info] : file_map_) {
        if (file_info.first == old_order) {
            file_info.first = new_order;
        }
    }

    response->set_status(StatusCode::SUCCESS);
    return Status::OK;
}


vector<string> HydfsServer::getAllFileNames() {
    // not gonna lock
    vector<string> file_names;
    for (auto& [filename, file_info] : file_map_) {
        file_names.push_back(filename);
    }
    return file_names;
}
