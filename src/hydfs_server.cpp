#include "hydfs_server.h"
#include <grpcpp/grpcpp.h>

#define GRPC_PORT 8081

using grpc::Server;
using grpc::ServerBuilder;

HydfsServer::HydfsServer()
    : file_transfer_server_(new FileTransferServiceImpl())
{
    char hostname[256];
    if (gethostname(hostname, sizeof(hostname)) != 0) {
        perror("gethostname");
        exit(1);
    }
    std::string hostname_str = hostname;
    std::string server_address(hostname_str + ":" + std::to_string(GRPC_PORT));

    ServerBuilder builder;
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(file_transfer_server_);

    server_ = builder.BuildAndStart();
    std::cout << "gRPC Server listening on " << server_address << std::endl;
}

HydfsServer::~HydfsServer() {
    if (server_) {
        server_->Shutdown();
    }
    delete file_transfer_server_;
}

void HydfsServer::wait() {
    if (server_) {
        server_->Wait();
    }
}