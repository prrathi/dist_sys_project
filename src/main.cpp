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
#include <condition_variable>
#include <mutex>
#include <algorithm>
#include <random>
#include <chrono>
#include <future>
#include <ctime>

#include "common.h"
#include "listener.h"
#include "worker.h"
#include "talker.h"

//Period now in milliseconds
#define PERIOD 650
#define SUSPERIOD 7
#define PINGPERIOD 400
#define NORMALPERIOD 2000
#define NORMALPINGPERIOD 1500

// #define PERIOD 2000
// #define SUSPERIOD 10
// #define PINGPERIOD 1500
// #define NORMALPERIOD 2000
// #define NORMALPINGPERIOD 1500

#define LOGFILE "Logs/log.txt"

using namespace std;


std::string introducerHostname = "fa24-cs425-5801.cs.illinois.edu";

FullNode currNode;
std::condition_variable condVar;
std::mutex globalMtx;   
const char* FIFO_PATH = "/tmp/mp2";

// for joining and leaving group if leaving just dont respond to anything in listener
bool join = false;
bool leave = false;




void handle_command(const std::string& command) {
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
    } else if (command == "list_suspected\n") {
        for (const auto& id : currNode.getAllIds()) {
            const auto& state = currNode.getState(id);
            if (state.status == Sus) {
                std::cout << id << " is suspected" << std::endl;
            }
        }
    } else {
        std::cout << "Invalid command detected" << std::endl;
    }
}

void pipe_listener() {
    // make the named pipe if it doesn't exist
    if (mkfifo(FIFO_PATH, 0666) == -1 && errno != EEXIST) {
        perror("mkfifo");
        exit(EXIT_FAILURE);
    }

    while (true) {
        int fd = open(FIFO_PATH, O_RDONLY);
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
        handle_command(buffer);
        close(fd);
    }
}



/*
  FullNode(bool isIntro
  , string nId, 
  const vector<PassNodeState>& nodeStates, 
  uint16_t cPeriod, curent period every time go through loop increment by 1 ... (its local)
  bool pResponded, false 
  bool pOpen, false
  float dRate, drop rate
  uint16_t tPing,  parameter -> how long until worker timeout
  uint16_t tPeriod,  param -> how long main thread will sleep for (assumed longer than tPing)
  uint16_t susPeriod, param  -> how long sus 
  bool hSusStatus, -> sus or not sus
  string logFile);



*/
// generate id from hostname + time
string generateId(string hostname) {
    auto now = std::chrono::system_clock::now();
    std::time_t now_c = std::chrono::system_clock::to_time_t(now);
    std::string id = hostname + "-" + std::to_string(now_c);

    cout << "ID generated: " << id << endl;
    return id;
}

FullNode initNode() {
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


int main() {

    // Check if the user is prathi3 and change the hostname if so
    const char* user = getenv("USER");
    if (user != nullptr && strcmp(user, "prathi3") == 0) {
      // introducerHostname = "fa24-cs425-5806.cs.illinois.edu";
      FIFO_PATH = "/tmp/mp2-prathi3";
      // cout << "Introducer hostname changed to: " << introducerHostname << endl;
    }

    std::thread listener_thread(pipe_listener);
    listener_thread.detach(); 

    currNode = initNode();

    auto rng = std::default_random_engine {};

    std::thread udp_server_thread(runUdpServer, std::ref(currNode));
    udp_server_thread.detach(); 

    while (true) {

        // in case we just leave but want to rejoin eventually busy wait if 1 actualyl nvm
        if (!currNode.getIsIntroducer() && !join) {
            std::unique_lock<std::mutex> lck(globalMtx);
            condVar.wait(lck, []{ 
                return join; 
            });
            SwimMessage joinMessage(currNode.getId(), currNode.getState(currNode.getId()).nodeIncarnation, "", DingDong, currNode.getCurrentPeriod(), {currNode.getState(currNode.getId())});
            sendUdpRequest(introducerHostname, serializeMessage(joinMessage));
        }
        //std::this_thread::sleep_for(std::chrono::milliseconds(PERIOD));
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

            //std::cout << "HERE Ids to send: " << std::endl;
            currNode.updateStateIdsToSend(); // 

            // std::cout << "State Ids to send: " << std::endl;
            // for (const auto& id : currNode.getStateIdsToSend()) {
            //     std::cout << id << " ";
            // }
            // std::cout << std::endl;
            
            std::thread t(handlePing, std::ref(currNode), machineId);
            t.detach();
            //handlePing(currNode, machineId);
            // when deciding what message we should be piggybacking onto pings if the period an update was made is within 2N-1 periods then we choose to send it
            std::this_thread::sleep_for(std::chrono::milliseconds(currNode.getPeriodTime()));

            //std::future<int> futureResult = std::async(std::launch::async, handlePing, currNode, machineId);
            // wait timeout
            //std::future_status status = futureResult.wait_for(std::chrono::seconds(timeoutSeconds));
            // if (status != std::future_status::ready) {
            //     std::cout << "Timeout! Function took too long." << std::endl;
            // }
           
            //cout << "MEMBERLIST STATE: " << endl;
            //for (const auto& id : currNode.getAllIds()) {
            //    auto state = currNode.getState(id);
            //    print_state(state);
            //}
            currNode.setCurrentPeriod(currNode.getCurrentPeriod() + 1);
        }
        //std::this_thread::sleep_for(std::chrono::milliseconds(PERIOD)); // temp
    }
    return 0;

}