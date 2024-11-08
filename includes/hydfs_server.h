#ifndef HYDFS_SERVER_H
#define HYDFS_SERVER_H

#include <string>
#include <chrono>
#include <utility>
#include <vector>
#include <unordered_map>
#include <memory>
#include <grpcpp/grpcpp.h>
#include "file_transfer_server.h"

class HydfsServer {
public:
    HydfsServer();
    ~HydfsServer();

    void wait();

private:
    using TimePoint = std::chrono::system_clock::time_point;
    
    FileTransferServiceImpl* file_transfer_server_;
    std::unique_ptr<grpc::Server> server_;
    std::unordered_map<std::string, std::vector<std::pair<TimePoint, std::string>>> file_map;
};

#endif // HYDFS_SERVER_H
