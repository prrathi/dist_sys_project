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
#include "hydfs_server.h"
#include "helper.h"

// Implementation-specific constants
static const int PERIOD = 800;
static const int SUS_PERIOD = 12;
static const int PING_PERIOD = 800;
static const int NORMAL_PERIOD = 3000;
static const int NORMAL_PING_PERIOD = 2500;
static const int MODULUS = 8192 * 2;
static const int GRPC_PORT_SERVER = 8081;
static const size_t LRU_CACHE_CAPACITY = 1024 * 1024 * 50;
static const size_t NUM_NODES_TO_CALL = 3;

// Class static member definitions
const char* DEFAULT_LOG_FILE = "Logs/log.txt";
char* DEFAULT_FIFO_PATH = "/tmp/mp4";

using namespace std;

Hydfs::Hydfs() 
    : lru_cache(LRU_CACHE_CAPACITY)
    , server()
{
    if (string(getenv("USER")) == "prathi3" || string(getenv("USER")) == "praneet") {
        DEFAULT_FIFO_PATH = "/tmp/mp4-prathi3";
    }
    cout << "FIFO PATH: " << DEFAULT_FIFO_PATH << "\n";
}

Hydfs::~Hydfs() {}

void Hydfs::handleCreate(const string& filename, const string& hydfs_filename) {
    if (lru_cache.exist(hydfs_filename)) {
        cout << "File already exists on hydfs: cache" << endl;
        return;
    }
    //cout << "create called" << "\n";
    vector<string> successors = getAllSuccessors(hydfs_filename);
    for (size_t i = 0; i < 3; i++) {
        string targetHost = successors[i] + ":" + to_string(GRPC_PORT_SERVER); 
        FileTransferClient client(grpc::CreateChannel(targetHost, grpc::InsecureChannelCredentials()));
        cout << "Create called, Target: " << targetHost << "\n";
        bool res = client.CreateFile(hydfs_filename, i);
        if (res) {
            cout << "Create Successful on " << targetHost << "" << "\n";
        } else  {
            cout << "Failed to create file on " << targetHost << "\n";
            //assume all succeed tbh
            return;
        }
    }
    handleAppend(filename, hydfs_filename);
}

void Hydfs::handleGet(const string& filename, const string& hydfs_filename, const string& target, bool avoid_cache) {

    // check here whether in cache, only need local consistency which is guaranteed
    if (lru_cache.exist(hydfs_filename) && !avoid_cache) {
        lock_guard<mutex> lock(cacheMtx);
        vector<char> contents = lru_cache.get(hydfs_filename).second;
        ofstream file(filename, ios::binary);
        if (!file) {
            cout << "Failed to open file: " << filename << "\n";
        } else {
            cout << "writing cached file: " << filename << "\n";
            file.write(contents.data(), contents.size());
        }
        return;
    }
    cout << "get called" << " target: " << target << "\n";
    FileTransferClient client(grpc::CreateChannel(target, grpc::InsecureChannelCredentials()));
    bool res = client.GetFile(hydfs_filename, filename);
    if (res) {
        // assuming stuff .. can fix change later if issues come
        cout << "Get Successful" << endl;
        if (avoid_cache) {
            return;
        }
        cout << "Caching" << "\n";
        vector<char> contents = readFileIntoVector(filename);
        if (contents.size() > lru_cache.capacity()) {
            return;
        }   
        lock_guard<mutex> lock(cacheMtx);
        lru_cache.put(hydfs_filename, make_pair(contents.size(), contents));
    } else {
        cout << "Get Failed" << endl;
    }
}

void Hydfs::handleAppend(const string& filename, const string& hydfs_filename) {
    vector<string> successors = getAllSuccessors(hydfs_filename);
    for (size_t i = 0; i < 3; i++) {
        string targetHost = successors[i] + ":" + to_string(GRPC_PORT_SERVER); 
        FileTransferClient client(grpc::CreateChannel(targetHost, grpc::InsecureChannelCredentials()));
        cout << "Append called, Target: " << targetHost << "\n";
        bool res = client.AppendFile(filename, hydfs_filename);
        if (res) {
            cout << "Append Successful: " << targetHost << "\n";
        } else {
            cout << "Append Failed on target: " << targetHost << endl;
            return;
        }
    }
    lock_guard<mutex> lock(cacheMtx);
    lru_cache.remove(hydfs_filename);
}

void Hydfs::handleMerge(const string& hydfs_filename) {
    vector<string> successors = getAllSuccessors(hydfs_filename);
    for (size_t i = 0; i < successors.size(); i++) {
        successors[i] = successors[i] + ":" + to_string(GRPC_PORT_SERVER);
    }
    string target_host = successors[0];
    vector<string> non_leader_successors(successors.begin() + 1, successors.end());
    FileTransferClient client(grpc::CreateChannel(target_host, grpc::InsecureChannelCredentials()));
    cout << "Merge called, Target: " << target_host << "\n";
    bool res = client.MergeFile(hydfs_filename, non_leader_successors);
    if (res) {
        cout << "Merge Successful" << endl;
    } else {
        cout << "Merge Failed on target: " << target_host << endl;
        return;
    }
    lock_guard<mutex> lock(cacheMtx);
    lru_cache.remove(hydfs_filename);
}

void Hydfs::handleNodeFailureDetected(const string& failed_node_id, const unordered_set<string>& nodeIds) {
    cout << "Handling node failure of " << failed_node_id << " on " << currNode.getId() << "\n";
    auto successors = findSuccessors(failed_node_id, nodeIds, MODULUS);
    string successor1 = successors[0].first +  ":" + to_string(GRPC_PORT_SERVER);
    string successor2 = successors[1].first + ":" + to_string(GRPC_PORT_SERVER);
    string successor3 = successors[2].first + ":" + to_string(GRPC_PORT_SERVER);
    pair<string, string> preds = find2Predecessor(failed_node_id, nodeIds, MODULUS);
    string predecessor1 = preds.first + ":" + to_string(GRPC_PORT_SERVER); // immediately preceding leader
    string predecessor2 = preds.second + ":" + to_string(GRPC_PORT_SERVER); // second preceding leader

    cout << "Successors: " << successor1 << " " << successor2 << " " << successor3 << "\n";
    cout << "Predecessors: " << predecessor1 << " " << predecessor2 << "\n";

    // replication for files with new leader
    auto start_time = chrono::system_clock::now();
    time_t seconds = chrono::system_clock::to_time_t(start_time);
    auto microseconds = chrono::duration_cast<chrono::microseconds>(start_time.time_since_epoch()) % 1000000;
    cout << seconds << '.' << setw(6) << setfill('0') << microseconds.count() << endl;

    FileTransferClient client(grpc::CreateChannel(successor1, grpc::InsecureChannelCredentials()));
    bool res = client.UpdateReplication(4, successor2, {successor3}); 
    if (res) {
        cout << "UpdateReplication Successful" <<  "\n";
    } else {
        cout << "UpdateReplication Failed" << "\n";
    }

    // replication for files with immediately preceding leader
    FileTransferClient client2(grpc::CreateChannel(predecessor1, grpc::InsecureChannelCredentials()));
    res = client2.UpdateReplication(2, successor1, {successor2}); 
    if (res) {
        cout << "UpdateReplication Successful" <<  "\n";
    } else {
        cout << "UpdateReplication Failed" << "\n";
    }

    // replication for files with second preceding leader
    FileTransferClient client3(grpc::CreateChannel(predecessor2, grpc::InsecureChannelCredentials()));
    res = client3.UpdateReplication(1, predecessor1, {successor1}); 
    if (res) {
        cout << "UpdateReplication Successful" <<  "\n";
    } else {
        cout << "UpdateReplication Failed" << "\n";
    }

    auto end_time = chrono::system_clock::now();
    seconds = chrono::system_clock::to_time_t(end_time);
    microseconds = chrono::duration_cast<chrono::microseconds>(end_time.time_since_epoch()) % 1000000;
    cout << seconds << '.' << setw(6) << setfill('0') << microseconds.count() << endl;
}

vector<string> Hydfs::getAllSuccessors(const string& filename) {
    vector<pair<string, pair<size_t, size_t>>> successors =  find3SuccessorsFile(filename, currNode.getAllIds(), MODULUS);
    vector<string> res;
    for (size_t i = 0; i < successors.size(); i++) {
        res.push_back(successors[i].first);
    }
    return res;
}

// deterministic for node x filename
string Hydfs::getTarget(const string& filename) {
    vector<pair<string, pair<size_t, size_t>>> successors = find3SuccessorsFile(filename, currNode.getAllIds(), MODULUS);
    size_t currHash = hashString(currNode.getId() + filename, MODULUS);
    mt19937 gen(currHash); 
    uniform_int_distribution<> distrib(0, successors.size() - 1);
    int randomIndex = distrib(gen);  
    return successors[randomIndex].first;
}

void Hydfs::handleClientRequests(const string& command) {

    // parsing a lil scuffed 
    if (command.substr(0, 6) == "create") {

        size_t loc_delim = command.find(" ");
        string filename = command.substr(loc_delim + 1, command.find(" ", loc_delim + 1) - loc_delim - 1);
        loc_delim = command.find(" ", loc_delim + 1);
        string hydfs_filename = command.substr(loc_delim + 1, command.find("\n") - loc_delim - 1);

        //cout << "Create" << filename << " hydfs: " << hydfs_filename << " targetHost: " << targetHost << "\n";
        handleCreate(filename, hydfs_filename);

    } else if (command.substr(0, 6) == "append") {

        size_t loc_delim = command.find(" ");
        string filename = command.substr(loc_delim + 1, command.find(" ", loc_delim + 1) - loc_delim - 1);
        loc_delim = command.find(" ", loc_delim + 1);
        string hydfs_filename = command.substr(loc_delim + 1, command.find("\n") - loc_delim - 1);

        //cout << "Append" << filename << " hydfs: " << hydfs_filename << " targetHost: " << targetHost << "\n";
        handleAppend(filename, hydfs_filename);

    } else if (command.substr(0, 4) == "get ") {
        
        size_t loc_delim = command.find(" ");
        string hydfs_filename = command.substr(loc_delim + 1, command.find(" ", loc_delim + 1) - loc_delim - 1);
        loc_delim = command.find(" ", loc_delim + 1);
        string filename = command.substr(loc_delim + 1, command.find("\n") - loc_delim - 1);

        string targetHost = getTarget(hydfs_filename) + ":" + to_string(GRPC_PORT_SERVER); // use the hydfs filename right?

        cout << "Get" << filename << " hydfs: " << hydfs_filename << " targetHost: " << targetHost << "\n";
        handleGet(filename, hydfs_filename, targetHost, false);

    } else if (command.substr(0, 5) == "merge") {
        size_t loc_delim = command.find(" ");
        string hydfs_filename = command.substr(loc_delim + 1, command.find("\n") - loc_delim - 1);
        handleMerge(hydfs_filename);

    } else if (command.substr(0, 2) == "ls") {
        size_t loc_delim = command.find(" ");
        string hydfs_filename = command.substr(loc_delim + 1, command.find("\n", loc_delim + 1) - loc_delim - 1);
        cout << "ls: " << hydfs_filename << "\n";
        vector<pair<string, pair<size_t, size_t>>> successors = find3SuccessorsFile(hydfs_filename, currNode.getAllIds(), MODULUS);
        for (const auto& successor : successors) {
            cout << successor.first << " Node ID: " << successor.second.first <<   "\n";
        }
        cout << "File ID: " << successors[0].second.second << "\n";

    } else if (command.substr(0, 5) == "store") {
        vector<string> fileNames = server.getAllFileNames();
        for (const auto& fileName : fileNames) {
            cout << "Filename: " << fileName << " ID: " << hashString(fileName, MODULUS) << "\n";
        }
        string hostname = currNode.getId().substr(0, currNode.getId().rfind("-"));
        cout << "VM: " << hostname << " VM ID: " << hashString(hostname, MODULUS) << "\n";

    } else if (command.substr(0, 14) == "getfromreplica") {
        size_t loc_delim = command.find(" ");
        string VMaddress = command.substr(loc_delim + 1, command.find(" ", loc_delim + 1) - loc_delim - 1);
        loc_delim = command.find(" ", loc_delim + 1);
        string hydfs_filename = command.substr(loc_delim + 1, command.find(" ", loc_delim + 1) - loc_delim - 1);
        loc_delim = command.find(" ", loc_delim + 1);
        string filename = command.substr(loc_delim + 1, command.find("\n") - loc_delim - 1);

        string targetHost = VMaddress + ":" + to_string(GRPC_PORT_SERVER); 

        cout << "Getfromreplica" << filename << " hydfs: " << hydfs_filename << " targetHost: " << targetHost << "\n";
        handleGet(filename, hydfs_filename, targetHost, true);  // should just be like get right

    } else if (command.substr(0, 12) == "list_mem_ids") {
        cout << "list_mem_ids" << "\n";
        vector<pair<size_t, string>> nodes_on_ring; 
        for (const auto& id : currNode.getAllIds()) {
            string hostname = id.substr(0, id.rfind("-"));
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

void Hydfs::handleCommand(const string& command) {
    cout << "COMMAND: " << command << endl;
    if (command == "join\n") {
        lock_guard<mutex> lck(globalMtx);
        join = true;
        // what if this gets dropped, wont be that unlucky right lol
        writeToLog(currNode.getLogFile(), "Attempting to join group: " + currNode.getId());
        condVar.notify_one();
    } else if (command == "leave\n") {
        leave = true;
    } else if (command == "list_mem\n") {
        for (const auto& id : currNode.getAllIds()) {
            cout << id << endl;
        }
        cout << "send list size: " << currNode.getStateIdsToSend().size() << endl;
    } else if (command == "list_self\n") {
        cout << currNode.getId() << endl;
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
            cout << "Sus status: enabled" << endl;
        } else {
            cout << "Sus status: disabled" << endl;
        }
    } else if (command == "list_suspected\n") {
        for (const auto& id : currNode.getAllIds()) {
            const auto& state = currNode.getState(id);
            if (state.status == Sus) {
                cout << id << " is suspected" << endl;
            }
        }
    } else {
        // assume client request 
        handleClientRequests(command);
    }
}

void Hydfs::pipeListener() {
    // make the named pipe if it doesn't exist
    //cout << "ASDASD\n";
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
    const char* user = getenv("USER");
    if (user != nullptr && strcmp(user, "prathi3") == 0) {
        FIFO_PATH = "/tmp/mp4-prathi3";
    }

    currNode = initNode();

    auto rng = default_random_engine {};

    thread udp_server_thread(runUdpServer, ref(currNode));
    udp_server_thread.detach(); 
    bool indicator = false;

    while (true) {
        if (!currNode.getIsIntroducer() && !join) {
            unique_lock<mutex> lck(globalMtx);
            condVar.wait(lck, [&]{ 
                return join; 
            });
            SwimMessage joinMessage(currNode.getId(), currNode.getState(currNode.getId()).nodeIncarnation, "", DingDong, currNode.getCurrentPeriod(), {currNode.getState(currNode.getId())});
            sendUdpRequest(introducerHostname, serializeMessage(joinMessage));
        }
        if (join && !indicator) {
            cout << "Node " << currNode.getId() << " joined the group" << endl;
            indicator = true;
        }

        auto ids = currNode.getAllIds();
        vector<string> machinesToCheck; 
        machinesToCheck.insert(machinesToCheck.end(), ids.begin(), ids.end());

        shuffle(machinesToCheck.begin(), machinesToCheck.end(), rng);

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
                    auto now = chrono::system_clock::now();
                    auto now_time = chrono::system_clock::to_time_t(now);
                    tm* now_tm = localtime(&now_time);
                    char buffer[10];
                    strftime(buffer, sizeof(buffer), "%M:%S", now_tm);
                    string timestamp(buffer);
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
            // thread t(handlePing, ref(currNode), machineId);
            // t.detach();
            handlePing(currNode, machineId);
            // when deciding what message we should be piggybacking onto pings if the period an update was made is within 2N-1 periods then we choose to send it
            // this_thread::sleep_for(chrono::milliseconds(currNode.getPeriodTime())); // do need this? hm

            currNode.setCurrentPeriod(currNode.getCurrentPeriod() + 1);
        }
    }
}

string generateId(string hostname) {
    auto now = chrono::system_clock::now();
    time_t now_c = chrono::system_clock::to_time_t(now);
    string id = hostname + "-" + to_string(now_c);

    cout << "ID generated: " << id << endl;
    return id;
}

FullNode Hydfs::initNode() {
    char hostname[256]; 
    if (gethostname(hostname, sizeof(hostname)) != 0) {
        perror("gethostname"); // Print error message if gethostname fails
        exit(1);
    } 
    
    string hostname_str = hostname;

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
    vector<PassNodeState> nodeStates = {currNodeState};
    if (hostname_str == introducerHostname) {
        return FullNode(true, nodeId, nodeStates, 0, false, false, 0, NORMAL_PING_PERIOD, NORMAL_PERIOD, SUS_PERIOD, true, DEFAULT_LOG_FILE);
    } else {
        return FullNode(false, nodeId, nodeStates, 0, false, false, 0, NORMAL_PING_PERIOD, NORMAL_PERIOD, SUS_PERIOD, true, DEFAULT_LOG_FILE);
    }
}


void Hydfs::getFile(const string& filename, const string& hydfs_filename) {
    string targetHost = getTarget(hydfs_filename) + ":" + to_string(GRPC_PORT_SERVER);
    cout << "Get" << filename << " hydfs: " << hydfs_filename << " targetHost: " << targetHost << "\n";
    handleGet(filename, hydfs_filename, targetHost, false);
}

void Hydfs::createFile(const string& filename, const string& hydfs_filename) {
    handleCreate(filename, hydfs_filename);
}   

void Hydfs::appendFile(const string& filename, const string& hydfs_filename) {
    handleAppend(filename, hydfs_filename);
}

// just getting vms unordered, easier to check for task usage in leader
vector<string> Hydfs::getVMs() {
    auto nodeIds = currNode.getAllIds();
    vector<string> nodes;
    for (const auto& nodeId : nodeIds) {
        string hostname = nodeId.substr(0, nodeId.rfind("-"));
        nodes.push_back(hostname);
    }
    return nodes;
}