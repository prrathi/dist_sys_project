#include "utils.h"
#include <iostream>
#include <string>
#include <vector>
#include <sstream>
#include <algorithm> 
#include <unordered_set>
#include <functional>
#include <utility>
#include <netdb.h>
#include <arpa/inet.h>


using namespace std;

size_t circularDistance(size_t hash1, size_t hash2, size_t modulus) {
    if (hash1 > hash2) {
      swap(hash1,hash2);
    }
    size_t diff = hash2 - hash1; 
    return min(diff, modulus - diff);
}

size_t hashString(const string& str, size_t modulus) {
    hash<string> hasher;
    return hasher(str) % modulus;
}

vector<pair<string, pair<size_t, size_t>>> find3Successors(const string& filename, const unordered_set<string>& nodeIds, size_t modulus) {
    size_t filenameHash = hashString(filename, modulus);
    //cout << "filenameHash: " << filenameHash << "\n";
    vector<pair<size_t, string>> nodeHashes;

    for (const auto& nodeId : nodeIds) {
        string hostname = nodeId.substr(0, nodeId.rfind("-"));
        size_t nodeHash = hashString(hostname, modulus);
        nodeHashes.push_back({circularDistance(nodeHash, filenameHash, modulus), hostname});
    }

    sort(nodeHashes.begin(), nodeHashes.end());
    vector<pair<string, pair<size_t, size_t>>> successors;
    size_t i = 0;

    for (const auto& node : nodeHashes) {
        if (i==3) break;
        successors.push_back({node.second, {hashString(node.second, modulus), filenameHash}});
        i++;        
    } 
    return successors;
}

std::string HostToIp(const std::string& host) {
    hostent* hostname = gethostbyname(host.c_str());
    if(hostname)
        return std::string(inet_ntoa(**(in_addr**)hostname->h_addr_list));
    return {};
}