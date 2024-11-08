#ifndef HYDFS_H
#define HYDFS_H

#include "common.h"
#include "listener.h"
#include "worker.h"
#include "talker.h"
#include <condition_variable>

class Hydfs {
public:
    class FileTransferServiceImpl;
    Hydfs();
    ~Hydfs();
    void swim();
    void pipeListener();
    void runServer();
private:
    void handleCommand(const std::string& command);
    void handleClientRequests(const std::string& command);
    void handleCreate(const std::string& filename, const std::string& hydfs_filename, const std::string& target);
    void handleGet(const std::string& filename, const std::string& hydfs_filename, const std::string& target);
    std::string getTarget(const std::string& filename);
    FullNode initNode();
private:
    const std::string introducerHostname = "fa24-cs425-5801.cs.illinois.edu";
    string FIFO_PATH = "/tmp/mp3";
    FullNode currNode;
    std::condition_variable condVar;
    std::mutex globalMtx;   
    bool join = false;
    bool leave = false;

};

#endif // HYDFS_H