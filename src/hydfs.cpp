#include <iostream>
#include <cstdlib> 
#include <string>
#include <thread>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <ifaddrs.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <algorithm>
#include <random>
#include <chrono>
#include <future>
#include <ctime>

#include "hydfs.h"
#include "file_transfer_client.cpp"
#include "utils.h"

#define LRUCache_CAPACITY 1024 * 1024 * 50
#define PERIOD 650
#define SUSPERIOD 7
#define PINGPERIOD 400
#define NORMALPERIOD 2000
#define NORMALPINGPERIOD 1500

#define GRPC_PORT 8081
#define LOGFILE "Logs/log.txt"

using grpc::Server;
using grpc::ServerBuilder;

Hydfs::Hydfs() 
    : cache(LRUCache_CAPACITY)
    , server()
{
}

Hydfs::~Hydfs() {}

void Hydfs::handleCreate(const std::string& filename, const std::string& hydfs_filename, const std::string& target) {
    if (cache.exist(hydfs_filename)) {
        std::cout << "File already exists on hydfs: cache" << std::endl;
        return;
    }
    std::cout << "create called" << " target: " << target << "\n";
    FileTransferClient client(grpc::CreateChannel(target, grpc::InsecureChannelCredentials()));
    bool res = client.CreateFile(filename, hydfs_filename);
    if (res) {
        std::cout << "Create Successful" << std::endl;
        std::vector<char> contents = readFileIntoVector(filename);
        if (contents.size() > cache.capacity()) {
            return;
        }   
        std::lock_guard<std::mutex> lock(cacheMtx);
        cache.put(hydfs_filename, make_pair(contents.size(), contents));
    } else {
        std::cout << "Create Failed" << std::endl;
    }
}

void Hydfs::handleGet(const std::string& filename, const std::string& hydfs_filename, const std::string& target) {

    // check here whether in cache, only need local consistency which is guaranteed
    if (cache.exist(hydfs_filename)) {
        std::lock_guard<std::mutex> lock(cacheMtx);
        std::vector<char> contents = cache.get(hydfs_filename).second;
        std::ofstream file(filename, std::ios::binary);
        if (!file) {
            std::cout << "Failed to open file: " << filename << "\n";
        } else {
            std::cout << "writing cached file: " << filename << "\n";
            file.write(contents.data(), contents.size());
        }
        return;
    }
    std::cout << "get called" << " target: " << target << "\n";
    FileTransferClient client(grpc::CreateChannel(target, grpc::InsecureChannelCredentials()));
    bool res = client.GetFile(hydfs_filename, filename);
    if (res) {
        std::cout << "Get Successful" << std::endl;
    } else {
        std::cout << "Get Failed" << std::endl;
    }
}

void Hydfs::handleAppend(const std::string& filename, const std::string& hydfs_filename, const std::string& target) {
    std::cout << "Append called" << " target: " << target << "\n";
    FileTransferClient client(grpc::CreateChannel(target, grpc::InsecureChannelCredentials()));
    bool res = client.AppendFile(filename, hydfs_filename);
    if (res) {
        std::cout << "Append Successful" << std::endl;
        std::lock_guard<std::mutex> lock(cacheMtx);
        cache.remove(hydfs_filename);
    } else {
        std::cout << "Append Failed" << std::endl;
    }
}

// deterministic for node x filename
std::string Hydfs::getTarget(const std::string& filename) {
    size_t modulus = 8192;
    std::vector<string> successors = find3Successors(filename, currNode.getAllIds(), modulus);
    size_t currHash = hashString(currNode.getId() + filename, modulus);
    std::mt19937 gen(currHash); 
    std::uniform_int_distribution<> distrib(0, successors.size() - 1);
    int randomIndex = distrib(gen);  
    return successors[randomIndex];
}

void Hydfs::handleClientRequests(const std::string& command) {

    // parsing a lil scuffed 
    if (command.substr(0, 6) == "create") {

        size_t loc_delim = command.find(" ");
        std::string filename = command.substr(loc_delim + 1, command.find(" ", loc_delim + 1) - loc_delim - 1);
        loc_delim = command.find(" ", loc_delim + 1);
        std::string hydfs_filename = command.substr(loc_delim + 1, command.find("\n") - loc_delim - 1);

        std::string targetHost = getTarget(hydfs_filename) + ":" + std::to_string(GRPC_PORT); // use the hydfs filename right?

        cout << "create" << filename << " hydfs: " << hydfs_filename << " targetHost: " << targetHost << "\n";
        handleCreate(filename, hydfs_filename, targetHost);

    } else if (command.substr(0, 3) == "get") {
        
        size_t loc_delim = command.find(" ");
        std::string hydfs_filename = command.substr(loc_delim + 1, command.find(" ", loc_delim + 1) - loc_delim - 1);
        loc_delim = command.find(" ", loc_delim + 1);
        std::string filename = command.substr(loc_delim + 1, command.find("\n") - loc_delim - 1);

        std::string targetHost = getTarget(hydfs_filename) + ":" + std::to_string(GRPC_PORT); // use the hydfs filename right?

        cout << "Get" << filename << " hydfs: " << hydfs_filename << " targetHost: " << targetHost << "\n";
        handleGet(filename, hydfs_filename, targetHost);

    } else if (command.substr(0, 6) == "append") {

        size_t loc_delim = command.find(" ");
        std::string filename = command.substr(loc_delim + 1, command.find(" ", loc_delim + 1) - loc_delim - 1);
        loc_delim = command.find(" ", loc_delim + 1);
        std::string hydfs_filename = command.substr(loc_delim + 1, command.find("\n") - loc_delim - 1);

        std::string targetHost = getTarget(hydfs_filename) + ":" + std::to_string(GRPC_PORT); // use the hydfs filename right?

        cout << "Append" << filename << " hydfs: " << hydfs_filename << " targetHost: " << targetHost << "\n";
        handleAppend(filename, hydfs_filename, targetHost);

    } else if (command.substr(0, 5) == "merge") {

    } else if (command.substr(0, 2) == "ls") {

    } else if (command.substr(0, 5) == "store") {

    } else if (command.substr(0, 14) == "getfromreplica") {

    } else if (command.substr(0, 12) == "list_mem_ids") {

    } else if (command.substr(0, 11) == "multiappend") { 
        // i dont think need to implent this here, maybe just do at python calling append multiple times i think better
    } else {
        cout << "Bad Command" << "\n";
    }
}

void Hydfs::handleCommand(const std::string& command) {
    cout << "COMMAND: " << command << endl;
    if (command == "join\n") {
        std::lock_guard<std::mutex> lck(globalMtx);
        join = true;
        // what if this gets dropped, wont be that unlucky right lol
        writeToLog(currNode.getLogFile(), "Attempting to join group: " + currNode.getId());
        condVar.notify_one();
    } else if (command == "leave\n") {
        leave = true;
    } else if (command == "list_mem\n") {
        for (const auto& id : currNode.getAllIds()) {
            std::cout << id << std::endl;
        }
        cout << "send list size: " << currNode.getStateIdsToSend().size() << endl;
    } else if (command == "list_self\n") {
        std::cout << currNode.getId() << std::endl;
    } else if (command == "enable_sus\n") {
        for (const auto& id : currNode.getAllIds()) {
            PassNodeState currState = currNode.getState(id);
            currState.nodeIncarnation = 0;
            currNode.updateState(currState);
        }
        currNode.setSusStatus(true);
        currNode.setPingTime(PINGPERIOD);
        currNode.setPeriodTime(PERIOD);
    } else if (command == "disable_sus\n") {
        for (const auto& id : currNode.getAllIds()) {
            PassNodeState currState = currNode.getState(id);
            currState.nodeIncarnation = 0;
            currNode.updateState(currState);
        }
        currNode.setSusStatus(false);
        currNode.setPingTime(NORMALPINGPERIOD);
        currNode.setPeriodTime(NORMALPERIOD);
    } else if (command == "status_sus\n") {
        if (currNode.getSusStatus()) {
            std::cout << "Sus status: enabled" << std::endl;
        } else {
            std::cout << "Sus status: disabled" << std::endl;
        }
    } else if (command == "list_suspected\n") {
        for (const auto& id : currNode.getAllIds()) {
            const auto& state = currNode.getState(id);
            if (state.status == Sus) {
                std::cout << id << " is suspected" << std::endl;
            }
        }
    } else {
        // assume client request 
        handleClientRequests(command);
    }
}

void Hydfs::pipeListener() {
    // make the named pipe if it doesn't exist
    std::cout << "ASDASD\n";
    if (mkfifo(FIFO_PATH.c_str(), 0666) == -1 && errno != EEXIST) {
        perror("mkfifo");
        exit(EXIT_FAILURE);
    }

    while (true) {
        int fd = open(FIFO_PATH.c_str(), O_RDONLY);
        if (fd == -1) {
            perror("open");
            exit(EXIT_FAILURE);
        }
        cout << "Opened FIFO" << endl;
        char buffer[256];
        ssize_t res;
        ssize_t offset = 0;
        ssize_t total_read = 0;
        while ((res = read(fd, buffer + offset, sizeof(buffer) - 1)) > 0) {
            offset += res;
            total_read += res;
        }
        buffer[total_read] = '\0';
        handleCommand(buffer);
        close(fd);
    }
}

void Hydfs::runServer() {
    server.wait();  // Just wait on the already-created server
}

void Hydfs::swim() {
    // Check if the user is prathi3 and change the hostname if so
    const char* user = getenv("USER");
    if (user != nullptr && strcmp(user, "prathi3") == 0) {
      // introducerHostname = "fa24-cs425-5806.cs.illinois.edu";
      FIFO_PATH = "/tmp/mp3-prathi3";
      // cout << "Introducer hostname changed to: " << introducerHostname << endl;
    }

    // std::thread listener_thread(pipe_listener);
    // listener_thread.detach(); 

    currNode = initNode();

    auto rng = std::default_random_engine {};

    std::thread udp_server_thread(runUdpServer, std::ref(currNode));
    udp_server_thread.detach(); 

    while (true) {
        if (!currNode.getIsIntroducer() && !join) {
            std::unique_lock<std::mutex> lck(globalMtx);
            condVar.wait(lck, [&]{ 
                return join; 
            });
            SwimMessage joinMessage(currNode.getId(), currNode.getState(currNode.getId()).nodeIncarnation, "", DingDong, currNode.getCurrentPeriod(), {currNode.getState(currNode.getId())});
            sendUdpRequest(introducerHostname, serializeMessage(joinMessage));
        }

        auto ids = currNode.getAllIds();
        vector<string> machinesToCheck; 
        machinesToCheck.insert(machinesToCheck.end(), ids.begin(), ids.end());

        std::shuffle(machinesToCheck.begin(), machinesToCheck.end(), rng);

        for (const auto& machineId : machinesToCheck) {
            if (machineId == currNode.getId()) {
                continue;
            }
            auto nodeIds = currNode.getAllIds();
            for (auto& id : nodeIds) {
                auto state = currNode.getState(id);
                if (currNode.getSusStatus() && state.status == Sus && (currNode.getCurrentPeriod() - state.susBeginPeriod >= currNode.getSusPeriod())){
                    state.status = Dead;
                    state.deadBeginPeriod = currNode.getCurrentPeriod(); 
                    cout << "Node " << id << " is dead after being susses" << endl;
                    writeToLog(currNode.getLogFile(), "On node " + currNode.getId() + ": Node " + id + " is detected to have failed after Sussing at period " + to_string(currNode.getCurrentPeriod()) + ".");
                    currNode.updateState(state);
                    currNode.addSendId(state.nodeId);
                }
                if (state.status == Dead && (currNode.getCurrentPeriod() - state.deadBeginPeriod > (uint16_t)(2 * (currNode.getAllIds().size()) - 1))) {
                    auto now = std::chrono::system_clock::now();
                    auto now_time = std::chrono::system_clock::to_time_t(now);
                    std::tm* now_tm = std::localtime(&now_time);
                    char buffer[10];
                    std::strftime(buffer, sizeof(buffer), "%M:%S", now_tm);
                    std::string timestamp(buffer);
                    writeToLog(currNode.getLogFile(), "On node " + currNode.getId() + ": Node " + state.nodeId + " is removed after being dead for too long " + to_string(currNode.getCurrentPeriod()) + " at " + timestamp + ".");
                    currNode.removeNode(state.nodeId);
                    currNode.removeNewId(state.nodeId);
                    currNode.removeSendId(state.nodeId);
                }
                else if (state.status == Dead) {
                  writeToLog(currNode.getLogFile(), "On node " + currNode.getId() + ": Node " + state.nodeId + " is communicated as dead " + to_string(currNode.getCurrentPeriod()) + ".");
                }
            }
            currNode.updateStateIdsToSend();

            // no point threading here ****            
            std::thread t(handlePing, std::ref(currNode), machineId);
            t.detach();
            // when deciding what message we should be piggybacking onto pings if the period an update was made is within 2N-1 periods then we choose to send it
            std::this_thread::sleep_for(std::chrono::milliseconds(currNode.getPeriodTime()));

            currNode.setCurrentPeriod(currNode.getCurrentPeriod() + 1);
        }
        //std::this_thread::sleep_for(std::chrono::milliseconds(PERIOD)); // temp
    }
}

string generateId(string hostname) {
    auto now = std::chrono::system_clock::now();
    std::time_t now_c = std::chrono::system_clock::to_time_t(now);
    std::string id = hostname + "-" + std::to_string(now_c);

    cout << "ID generated: " << id << endl;
    return id;
}

FullNode Hydfs::initNode() {
    char hostname[256]; 
    if (gethostname(hostname, sizeof(hostname)) != 0) {
        perror("gethostname"); // Print error message if gethostname fails
        exit(1);
    } 
    
    std::string hostname_str = hostname;

    string nodeId = generateId(hostname_str);

    cout << "HOSTNAME: " << hostname_str << endl;

    PassNodeState currNodeState = {
        nodeId, // id (change to str)
        0, // incarnation num
        hostname_str,
        Alive, // status
        0, 
        0,
    };

    std::vector<PassNodeState> nodeStates = {currNodeState};
    if (hostname_str == introducerHostname) {
        return FullNode(true, nodeId, nodeStates, 0, false, false, 0, NORMALPINGPERIOD, NORMALPERIOD, SUSPERIOD, false, LOGFILE);
    } else {
        return FullNode(false, nodeId, nodeStates, 0, false, false, 0, NORMALPINGPERIOD, NORMALPERIOD, SUSPERIOD, false, LOGFILE);
    }
}
