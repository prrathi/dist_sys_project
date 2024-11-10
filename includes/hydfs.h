#pragma once

#include <condition_variable>
#include <mutex>
#include <string>
#include <vector>
#include <utility>
#include <fstream>

#include "common.h"
#include "listener.h"
#include "worker.h"
#include "talker.h"
#include "hydfs_server.h"
#include "LRUCache.h"

using namespace std;

class Hydfs {
public:
    Hydfs();
    ~Hydfs();
    void swim();
    void pipeListener();
    void runServer();
private:
    void handleCommand(const std::string& command);
    void handleClientRequests(const std::string& command);
    void handleCreate(const std::string& filename, const std::string& hydfs_filename);
    void handleGet(const std::string& filename, const std::string& hydfs_filename, const std::string& target);
    void handleAppend(const std::string& filename, const std::string& hydfs_filename);
    void handleMerge(const std::string& hydfs_filename);
    void handleNodeFailureDetected(const std::string& failed_node_id, const unordered_set<std::string>& nodeIds);
    std::string getTarget(const std::string& filename);
    std::vector<std::string> getAllSuccessors(const std::string& filename);
    FullNode initNode();
private:
    LRUCache lru_cache;
    const std::string introducerHostname = "fa24-cs425-5801.cs.illinois.edu";
    std::string FIFO_PATH = "/tmp/mp3";
    FullNode currNode;
    std::condition_variable condVar;
    std::mutex globalMtx;   
    std::mutex cacheMtx;
    bool join = false;
    bool leave = false;
    HydfsServer server;
};