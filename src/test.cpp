#include <iostream>
#include "common.h"
#include "talker.h"
#include "listener.h"
#include "worker.h"

using namespace std;

int main(int argc, char* argv[]) {
    if ((argc != 2) && (argc != 3)) {
        cerr << "Usage: " << argv[0] << " <mode> <hostname>" << endl;
        cerr << "Mode: 'send' or 'receive'" << endl;
        return 1;
    }

    string mode = argv[1];
    vector<PassNodeState> passVec;
    passVec.emplace_back();
    passVec.emplace_back();
    SwimMessage pingMessage("", 1, "", Ping, 0, passVec);
    string message = serializeMessage(pingMessage);
    cout << "INIT: " << message << endl;
    string hostname;
    if (argc == 3) {
    	hostname = argv[2];
    }

    if (mode == "send") {
        cout << "Sending message: " << message << endl;
        sendUdpRequest(hostname, message);
    } else if (mode == "receive") {
        cout << "Starting UDP server..." << endl;
        FullNode dummyNode;  // Create a dummy FullNode for testing
        runUdpServer(dummyNode);
    } else {
        cerr << "Invalid mode. Use 'send' or 'receive'." << endl;
        return 1;
    }

    return 0;
}
