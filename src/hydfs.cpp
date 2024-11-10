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
#include <filesystem>

#include "hydfs.h"
#include "file_transfer_client.h"
#include "utils.h"

// Implementation-specific constants
static const int PERIOD = 1000;
static const int SUS_PERIOD = 18;
static const int PING_PERIOD = 1000;
static const int NORMAL_PERIOD = 3000;
static const int NORMAL_PING_PERIOD = 2500;
static const int MODULUS = 8192;
static const int GRPC_PORT = 8081;
static const size_t LRU_CACHE_CAPACITY = 1024 * 1024 * 50;
static const size_t NUM_NODES_TO_CALL = 3;

// Class static member definitions
const char* DEFAULT_LOG_FILE = "Logs/log.txt";
const char* DEFAULT_FIFO_PATH = "/tmp/mp3";

Hydfs::Hydfs() 
    : lru_cache(LRU_CACHE_CAPACITY)
    , server()
{
}

Hydfs::~Hydfs() {}

void Hydfs::handleCreate(const std::string& filename, const std::string& hydfs_filename) {
    if (lru_cache.exist(hydfs_filename)) {
        std::cout << "File already exists on hydfs: cache" << std::endl;
        return;
    }
    //std::cout << "create called" << "\n";
    std::vector<std::string> successors = getAllSuccessors(hydfs_filename);
    for (size_t i = 0; i < 3; i++) {
        std::string targetHost = successors[i] + ":" + std::to_string(GRPC_PORT); 
        FileTransferClient client(grpc::CreateChannel(targetHost, grpc::InsecureChannelCredentials()));
        std::cout << "Create called, Target: " << targetHost << "\n";
        bool res = client.CreateFile(hydfs_filename, i);
        if (res) {
            std::cout << "Create Successful on " << targetHost << "" << "\n";
        } else  {
            std::cout << "Failed to create file on " << targetHost << "\n";
            //assume all succeed tbh
            return;
        }
    }
    handleAppend(filename, hydfs_filename);
}

void Hydfs::handleGet(const std::string& filename, const std::string& hydfs_filename, const std::string& target) {

    // check here whether in cache, only need local consistency which is guaranteed
    if (lru_cache.exist(hydfs_filename)) {
        std::lock_guard<std::mutex> lock(cacheMtx);
        std::vector<char> contents = lru_cache.get(hydfs_filename).second;
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
        // assuming stuff .. can fix change later if issues come
        std::cout << "Get Successful" << std::endl;
        std::cout << "Caching" << "\n";
        std::vector<char> contents = readFileIntoVector(filename);
        if (contents.size() > lru_cache.capacity()) {
            return;
        }   
        std::lock_guard<std::mutex> lock(cacheMtx);
        lru_cache.put(hydfs_filename, make_pair(contents.size(), contents));
    } else {
        std::cout << "Get Failed" << std::endl;
    }
}

void Hydfs::handleAppend(const std::string& filename, const std::string& hydfs_filename) {
    std::vector<std::string> successors = getAllSuccessors(hydfs_filename);
    for (size_t i = 0; i < 3; i++) {
        std::string targetHost = successors[i] + ":" + std::to_string(GRPC_PORT); 
        FileTransferClient client(grpc::CreateChannel(targetHost, grpc::InsecureChannelCredentials()));
        std::cout << "Append called, Target: " << targetHost << "\n";
        bool res = client.AppendFile(filename, hydfs_filename);
        if (res) {
            std::cout << "Append Successful: " << targetHost << "\n";
        } else {
            std::cout << "Append Failed on target: " << targetHost << std::endl;
            return;
        }
    }
    std::lock_guard<std::mutex> lock(cacheMtx);
    lru_cache.remove(hydfs_filename);
}

void Hydfs::handleMerge(const std::string& hydfs_filename) {
    std::string file_leader = getAllSuccessors(hydfs_filename)[0];
    std::string targetHost = file_leader + ":" + std::to_string(GRPC_PORT);
    FileTransferClient client(grpc::CreateChannel(targetHost, grpc::InsecureChannelCredentials()));
    std::cout << "Merge called, Target: " << targetHost << "\n";
    bool res = client.MergeFile(hydfs_filename);
    if (res) {
        std::cout << "Merge Successful" << std::endl;
    } else {
        std::cout << "Merge Failed on target: " << targetHost << std::endl;
        return;
    }

}

void Hydfs::handleNodeFailureDetected(const std::string& failed_node_id, const unordered_set<std::string>& nodeIds) {
    auto successors = findSuccessors(failed_node_id, nodeIds, MODULUS);
    std::string successor = successors[0].first +  ":" + std::to_string(GRPC_PORT); // 1 2 3
    std::string pred1 = find2Predecessor(failed_node_id, nodeIds, MODULUS).first + ":" + std::to_string(GRPC_PORT);
    std::string pred2 = find2Predecessor(failed_node_id, nodeIds, MODULUS).first + ":" + std::to_string(GRPC_PORT);


    // right now only handling non consec failures case 1, 2, 4? 
    // for the new leader of whatever went down
    // another case here
    FileTransferClient client(grpc::CreateChannel(successor, grpc::InsecureChannelCredentials()));
    std::string new_sucessor = successors[2].first + ":" + std::to_string(GRPC_PORT);
    bool res = client.UpdateReplication(4, successor, {new_sucessor}); // assuming the 1 means dead 
    if (res) {
        std::cout << "UpdateReplication Successful" <<  "\n";
    } else {
        std::cout << "UpdateReplication Failed" << "\n";
    }

    // first predecessor, also need case of multiple?
    FileTransferClient client2(grpc::CreateChannel(pred1, grpc::InsecureChannelCredentials()));
    std::string successor2 = successors[1].first +  ":" + std::to_string(GRPC_PORT);
    res = client2.UpdateReplication(2, successor, {successor2}); // assuming the 1 means dead 
    if (res) {
        std::cout << "UpdateReplication Successful" <<  "\n";
    } else {
        std::cout << "UpdateReplication Failed" << "\n";
    }

    // 2nd alive + 1st alive
    FileTransferClient client3(grpc::CreateChannel(pred2, grpc::InsecureChannelCredentials()));
    res = client3.UpdateReplication(1, pred1, {successor}); // assuming the 1 means dead 
    if (res) {
        std::cout << "UpdateReplication Successful" <<  "\n";
    } else {
        std::cout << "UpdateReplication Failed" << "\n";
    }
}

std::vector<std::string> Hydfs::getAllSuccessors(const std::string& filename) {
    std::vector<std::pair<std::string, std::pair<size_t, size_t>>> successors =  find3SuccessorsFile(filename, currNode.getAllIds(), MODULUS);
    std::vector<std::string> res;
    for (size_t i = 0; i < successors.size(); i++) {
        res.push_back(successors[i].first);
    }
    return res;
}


// deterministic for node x filename
std::string Hydfs::getTarget(const std::string& filename) {
    std::vector<std::pair<std::string, std::pair<size_t, size_t>>> successors = find3SuccessorsFile(filename, currNode.getAllIds(), MODULUS);
    size_t currHash = hashString(currNode.getId() + filename, MODULUS);
    std::mt19937 gen(currHash); 
    std::uniform_int_distribution<> distrib(0, successors.size() - 1);
    int randomIndex = distrib(gen);  
    return successors[randomIndex].first;
}

void Hydfs::handleClientRequests(const std::string& command) {

    // parsing a lil scuffed 
    if (command.substr(0, 6) == "create") {

        size_t loc_delim = command.find(" ");
        std::string filename = command.substr(loc_delim + 1, command.find(" ", loc_delim + 1) - loc_delim - 1);
        loc_delim = command.find(" ", loc_delim + 1);
        std::string hydfs_filename = command.substr(loc_delim + 1, command.find("\n") - loc_delim - 1);

        //cout << "Create" << filename << " hydfs: " << hydfs_filename << " targetHost: " << targetHost << "\n";
        handleCreate(filename, hydfs_filename);

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

        //cout << "Append" << filename << " hydfs: " << hydfs_filename << " targetHost: " << targetHost << "\n";
        handleAppend(filename, hydfs_filename);

    } else if (command.substr(0, 5) == "merge") {
        size_t loc_delim = command.find(" ");
        std::string hydfs_filename = command.substr(loc_delim + 1, command.find("\n") - loc_delim - 1);
        handleMerge(hydfs_filename);

    } else if (command.substr(0, 2) == "ls") {
        size_t loc_delim = command.find(" ");
        std::string hydfs_filename = command.substr(loc_delim + 1, command.find("\n", loc_delim + 1) - loc_delim - 1);
        cout << "ls: " << hydfs_filename << "\n";
        std::vector<std::pair<std::string, std::pair<size_t, size_t>>> successors = find3SuccessorsFile(hydfs_filename, currNode.getAllIds(), MODULUS);
        for (const auto& successor : successors) {
            std::cout << HostToIp(successor.first) << " Node ID: " << successor.second.first <<   "\n";
        }
        std::cout << "File ID: " << successors[0].second.second << "\n";

    } else if (command.substr(0, 5) == "store") {
        std::string path("hydfs/");
        if (std::filesystem::is_directory(path)) {
            std::cout << "Listing directory: " << path << "\n";
            for (const auto &entry : std::filesystem::directory_iterator(path)) {
                std::string path(entry.path());
                std::string last_element(path.substr(path.rfind("/") + 1));
                std::cout << last_element << " ID: " << hashString(last_element, MODULUS) << "\n";
            }
            string hostname = currNode.getId().substr(0, currNode.getId().find("-"));
            std::cout << "VM: " << hostname << " VM ID: " << hashString(hostname, MODULUS) << "\n";
        }
    } else if (command.substr(0, 14) == "getfromreplica") {
        size_t loc_delim = command.find(" ");
        std::string VMaddress = command.substr(loc_delim + 1, command.find(" ", loc_delim + 1) - loc_delim - 1);
        loc_delim = command.find(" ", loc_delim + 1);
        std::string hydfs_filename = command.substr(loc_delim + 1, command.find(" ", loc_delim + 1) - loc_delim - 1);
        loc_delim = command.find(" ", loc_delim + 1);
        std::string filename = command.substr(loc_delim + 1, command.find("\n") - loc_delim - 1);

        std::string targetHost = VMaddress + ":" + std::to_string(GRPC_PORT); 
        handleGet(filename, hydfs_filename, targetHost);  // should just be like get right

    } else if (command.substr(0, 12) == "list_mem_ids") {
        cout << "list_mem_ids" << "\n";
        vector<pair<size_t, string>> nodes_on_ring; 
        for (const auto& id : currNode.getAllIds()) {
            string hostname = id.substr(0, id.find("-"));
            nodes_on_ring.push_back({hashString(hostname, MODULUS), hostname});
        }
        sort(nodes_on_ring.begin(), nodes_on_ring.end());
        for (const auto& node : nodes_on_ring) {
            cout << "VM: " << node.second << " ID: " << node.first << "\n";
        }
        cout << "finish list_mem_ids" << endl;

    } else if (command.substr(0, 11) == "multiappend") { 
        cout << "Should not have reached here: multiappend" << "\n";
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
        currNode.setPingTime(PING_PERIOD);
        currNode.setPeriodTime(PERIOD);
    } else if (command == "disable_sus\n") {
        for (const auto& id : currNode.getAllIds()) {
            PassNodeState currState = currNode.getState(id);
            currState.nodeIncarnation = 0;
            currNode.updateState(currState);
        }
        currNode.setSusStatus(false);
        currNode.setPingTime(NORMAL_PING_PERIOD);
        currNode.setPeriodTime(NORMAL_PERIOD);
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
    //std::cout << "ASDASD\n";
    if (mkfifo(DEFAULT_FIFO_PATH, 0666) == -1 && errno != EEXIST) {
        perror("mkfifo");
        exit(EXIT_FAILURE);
    }

    while (true) {
        int fd = open(DEFAULT_FIFO_PATH, O_RDONLY);
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
        FIFO_PATH = "/tmp/mp3-prathi3";
    }

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
                //if (state.status == Dead && (currNode.getCurrentPeriod() - state.deadBeginPeriod == (uint16_t)(2 * (currNode.getAllIds().size()) - 1))) {
                if (state.status == Dead && (currNode.getCurrentPeriod() - state.deadBeginPeriod == NUM_NODES_TO_CALL)) {
                    auto now = std::chrono::system_clock::now();
                    auto now_time = std::chrono::system_clock::to_time_t(now);
                    std::tm* now_tm = std::localtime(&now_time);
                    char buffer[10];
                    std::strftime(buffer, sizeof(buffer), "%M:%S", now_tm);
                    std::string timestamp(buffer);
                    writeToLog(currNode.getLogFile(), "On node " + currNode.getId() + ": Node " + state.nodeId + " is removed after being dead for too long " + to_string(currNode.getCurrentPeriod()) + " at " + timestamp + ".");
                    cout << "Node " << state.nodeId << " is removed after being dead for too long " << currNode.getCurrentPeriod() << " at " << timestamp << "\n";
                    currNode.removeNode(state.nodeId);
                    currNode.removeNewId(state.nodeId);
                    currNode.removeSendId(state.nodeId);
                    if (checkIfMinVM(currNode.getId(), currNode.getAllIds(), MODULUS)) {
                        handleNodeFailureDetected(state.nodeId, currNode.getAllIds());   
                    }
                }
                else if (state.status == Dead) {
                  writeToLog(currNode.getLogFile(), "On node " + currNode.getId() + ": Node " + state.nodeId + " is communicated as dead " + to_string(currNode.getCurrentPeriod()) + ".");
                }
            }
            currNode.updateStateIdsToSend();

            // no point threading here ****            
            // std::thread t(handlePing, std::ref(currNode), machineId);
            // t.detach();
            handlePing(currNode, machineId);
            // when deciding what message we should be piggybacking onto pings if the period an update was made is within 2N-1 periods then we choose to send it
            // std::this_thread::sleep_for(std::chrono::milliseconds(currNode.getPeriodTime())); // do need this? hm

            currNode.setCurrentPeriod(currNode.getCurrentPeriod() + 1);
        }
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
    // start in sus mode
    std::vector<PassNodeState> nodeStates = {currNodeState};
    if (hostname_str == introducerHostname) {
        return FullNode(true, nodeId, nodeStates, 0, false, false, 0, NORMAL_PING_PERIOD, NORMAL_PERIOD, SUS_PERIOD, true, DEFAULT_LOG_FILE);
    } else {
        return FullNode(false, nodeId, nodeStates, 0, false, false, 0, NORMAL_PING_PERIOD, NORMAL_PERIOD, SUS_PERIOD, true, DEFAULT_LOG_FILE);
    }
}
