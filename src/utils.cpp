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
#include "common.h"

using namespace std;

size_t hashString(const string& str, size_t modulus) {
    hash<string> hasher;
    return hasher(str) % modulus;
}

vector<pair<string, pair<size_t, size_t>>> find3SuccessorsFile(const string& filename, const unordered_set<string>& nodeIds, size_t modulus) {
    size_t filenameHash = hashString(filename, modulus);
    vector<pair<size_t, string>> nodeHashes;

    for (const auto& nodeId : nodeIds) {
        string hostname = nodeId.substr(0, nodeId.rfind("-"));
        size_t nodeHash = hashString(hostname, modulus);
        nodeHashes.push_back({(nodeHash - filenameHash) % modulus, hostname});
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


vector<pair<string, pair<size_t, size_t>>> findSuccessors(const string& failed_node_id, const unordered_set<string>& nodeIds, size_t modulus) {
    string curr_node_hostname = failed_node_id.substr(0, failed_node_id.rfind("-"));
    size_t failed_node_hash = hashString(curr_node_hostname, modulus);
    vector<pair<size_t, string>> nodeHashes;

    for (const auto& nodeId : nodeIds) {
        string hostname = nodeId.substr(0, nodeId.rfind("-"));
        size_t nodeHash = hashString(hostname, modulus);
        nodeHashes.push_back({(nodeHash - failed_node_hash) % modulus, hostname});
    }

    sort(nodeHashes.begin(), nodeHashes.end());
    vector<pair<string, pair<size_t, size_t>>> successors;
    size_t i = 0;

    for (const auto& node : nodeHashes) {
        if (i==3) break;
        successors.push_back({node.second, {hashString(node.second, modulus), failed_node_hash}});
        i++;        
    } 
    return successors;
}

std::pair<std::string, std::string> find2Predecessor(const string& failed_node_id, const unordered_set<string>& nodeIds, size_t modulus) {
    string curr_node_hostname = failed_node_id.substr(0, failed_node_id.rfind("-"));
    size_t failed_node_hash = hashString(curr_node_hostname, modulus);
    vector<pair<size_t, string>> nodeHashes;

    for (const auto& nodeId : nodeIds) {
        string hostname = nodeId.substr(0, nodeId.rfind("-"));
        size_t nodeHash = hashString(hostname, modulus);
        nodeHashes.push_back({(nodeHash - failed_node_hash) % modulus, hostname});
    }

    sort(nodeHashes.begin(), nodeHashes.end());
    return {nodeHashes[nodeHashes.size() - 1].second, nodeHashes[nodeHashes.size() - 2].second};
}

std::string HostToIp(const std::string& host) {
    hostent* hostname = gethostbyname(host.c_str());
    if(hostname)
        return std::string(inet_ntoa(**(in_addr**)hostname->h_addr_list));
    return {};
}

bool checkIfMinVM(const string& currNodeId, const unordered_set<string>& nodeIds, size_t modulus) {
    string curr_node_hostname = currNodeId.substr(0, currNodeId.rfind("-"));
    if (curr_node_hostname == "fa24-cs425-5801.cs.illinois.edu") return true;
    return false;
    // size_t min = hashString(curr_node_hostname, modulus);
    // for (const auto& nodeId : nodeIds) {
    //     string hostname = nodeId.substr(0, nodeId.rfind("-"));
    //     size_t nodeHash = hashString(hostname, modulus);
    //     if (nodeHash < min) {
    //         return false;
    //     }
    // }
}