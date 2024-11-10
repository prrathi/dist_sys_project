#include <iostream>
#include <fstream>
#include <filesystem>
#include <chrono>
#include <iomanip>
#include <sstream>
#include <random>
#include <grpcpp/grpcpp.h>
#include "hydfs_server.h"
#include "file_transfer_client.h"

static const int GRPC_PORT = 8081;
static const size_t BUFFER_SIZE = 1024 * 1024;  // 1MB buffer

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
    server_address_ = hostname_str + ":" + to_string(GRPC_PORT);

    ServerBuilder builder;
    builder.AddListeningPort(server_address_, grpc::InsecureServerCredentials());
    builder.RegisterService(this);

    server_ = builder.BuildAndStart();
    cout << "gRPC Server listening on " << server_address_ << endl;
    if (filesystem::exists("hydfs/")) {
        filesystem::remove_all("hydfs/");
    }
    filesystem::create_directory("hydfs");
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

    // get first message to get filename
    if (!reader->Read(&msg)) {
        response->set_status(StatusCode::INVALID);
        response->set_message("Failed to read initial message");
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

    // process remaining messages
    while (reader->Read(&msg)) {
        if (msg.has_chunk()) {
            outfile.write(msg.chunk().content().data(), msg.chunk().content().size());
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

    // first the merged content if it exists
    ifstream infile("hydfs/" + filename, ios::binary);
    if (infile) {
        while (infile.read(buffer, BUFFER_SIZE) || infile.gcount() > 0) {
            chunk->set_content(buffer, infile.gcount());
            if (!writer->Write(chunk_response)) {
                OperationStatus* status = chunk_response.mutable_status();
                status->set_status(StatusCode::INVALID);
                status->set_message("Failed to write chunk");
                return Status::OK;
            }
        }
        infile.close();
    }

    // then the new chunks
    for (const string& chunk_filename : chunk_files) {
        ifstream infile("hydfs/" + chunk_filename, ios::binary);
        if (!infile) {
            cout << "Server failed to open chunk: " << chunk_filename << endl;
            continue;
        }

        while (infile.read(buffer, BUFFER_SIZE) || infile.gcount() > 0) {
            chunk->set_content(buffer, infile.gcount());
            if (!writer->Write(chunk_response)) {
                OperationStatus* status = chunk_response.mutable_status();
                status->set_status(StatusCode::INVALID);
                status->set_message("Failed to write chunk");
                return Status::OK;
            }
        }
        infile.close();
    }

    cout << "Completed read request for " << filename << " at " << server_address_ << endl;

    GetResponse final_response;
    OperationStatus* status = final_response.mutable_status();
    status->set_status(StatusCode::SUCCESS);
    writer->Write(final_response);
    return Status::OK;
}

Status HydfsServer::MergeFile(ServerContext* context, const MergeRequest* request, OperationStatus* response) {
    string filename = request->filename();
    size_t shard = get_shard_index(filename);
    lock_guard<mutex> lock(shard_mutexes_[shard]);

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

    // clear the chunks vector since we've merged them
    file_map_[filename].second.clear();
    
    // forward the merged file to all successors from the request. hacky but ¯\_(ツ)_/¯
    int successor_order = 3 - request->successors().size();
    for (const string& successor_address : request->successors()) {
        if (successor_address.empty()) {
            response->set_status(StatusCode::INVALID);
            response->set_message("Successor address is empty");
            return Status::OK;
        }
        auto channel = grpc::CreateChannel(successor_address, grpc::InsecureChannelCredentials());
        FileTransferClient client(channel);
        client.OverwriteFile(full_path, filename, successor_order);
        successor_order++;
    }

    response->set_status(StatusCode::SUCCESS);
    return Status::OK;
}

Status HydfsServer::OverwriteFile(ServerContext* context, ServerReader<OverwriteRequest>* reader, OperationStatus* response) {
    OverwriteRequest msg;
    ofstream outfile;
    string filename;

    // Get first message to get filename
    if (!reader->Read(&msg)) {
        response->set_status(StatusCode::INVALID);
        response->set_message("Failed to read initial message");
        return Status::OK;
    }

    // First message must contain file request
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
    int order = msg.file_request().order();

    // Create lock once we have filename
    size_t shard = get_shard_index(filename);
    lock_guard<mutex> lock(shard_mutexes_[shard]);

    // Create directory structure if needed
    string full_path = "hydfs/" + filename;
    filesystem::path dir_path = filesystem::path(full_path).parent_path();
    filesystem::create_directories(dir_path);

    // Open file for writing (truncating any existing content)
    outfile.open(full_path, ios::binary | ios::trunc);
    if (!outfile) {
        response->set_status(StatusCode::INVALID);
        response->set_message("Could not open file for writing");
        return Status::OK;
    }

    // process remaining messages
    while (reader->Read(&msg)) {
        if (msg.has_chunk()) {
            outfile.write(msg.chunk().content().data(), msg.chunk().content().size());
        }
    }
    outfile.close();

    // reset chunks vector if exists
    file_map_[filename] = make_pair(order, vector<string>()); 

    response->set_status(StatusCode::SUCCESS);
    return Status::OK;
}

Status HydfsServer::UpdateFilesReplication(ServerContext* context, const ReplicationRequest* request, OperationStatus* response) {
    // lock all shards when handling failure detection -> new replication
    vector<unique_lock<mutex>> locks;
    for (size_t i = 0; i < SHARD_COUNT; i++) {
        locks.emplace_back(shard_mutexes_[i]);
    }

    // encoding of failures - 0 and 1 are alive and dead resp for one pred and two successors. only one failure at a time needed due to sequential client detection
    int32_t failure_case = request->failure_case();
    int num_preceding_failures = 0;
    pair<int, int> existing_order = make_pair(-1, -1);
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

    vector<string> files_to_forward;
    // iterate through all files and update their order
    for (auto& [filename, file_info] : file_map_) {
        int32_t current_order = file_info.first;
        int32_t new_order = max(0, current_order - num_preceding_failures);
        file_info.first = new_order;
        // If order becomes 0 and we have target servers, this file needs to be forwarded
        if (new_order == 0 && !request->new_successors().empty()) {
            files_to_forward.push_back(filename);
        }
    }

    // with current setup should have zero to one existing successor with updated order
    if (existing_order.first != -1) {
        auto channel = grpc::CreateChannel(request->existing_successor(), grpc::InsecureChannelCredentials());
        FileTransferClient client(channel);
        client.UpdateOrder(existing_order.first, existing_order.second);
    }

    // with current setup should have zero to one new successor with updated order, send request to self as leader
    for (const string& filename : files_to_forward) {
        auto channel = grpc::CreateChannel(server_address_, grpc::InsecureChannelCredentials());
        FileTransferClient client(channel);
        client.MergeFile(filename, vector<string>(request->new_successors().begin(), request->new_successors().end()));
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


std::vector<std::string> HydfsServer::getAllFileNames() {
    // not gonna lock
    std::vector<std::string> file_names;
    for (auto& [filename, file_info] : file_map_) {
        file_names.push_back(filename);
    }
    return file_names;
}