#include "utils.h"
#include <iostream>
#include <string>
#include <vector>
#include <sstream>
#include <algorithm> 
#include <unordered_set>

size_t circularDistance(size_t hash1, size_t hash2, size_t modulus) {
    if (hash1 > hash2) {
      std::swap(hash1,hash2);
    }
    size_t diff = hash2 - hash1; 
    return std::min(diff, modulus - diff);
}

std::vector<std::string> find3Successors(const std::string& filename, const std::unordered_set<std::string>& nodeIds) {
    int modulus = 8192;
    std::hash<std::string> hasher;
    size_t filenameHash = hasher(filename) % modulus;
    //std::cout << "filenameHash: " << filenameHash << "\n";
    std::vector<std::pair<size_t, std::string>> nodeHashes;

    for (const auto& nodeId : nodeIds) {
        std::string hostname = nodeId.substr(0, nodeId.find("-"));
        size_t nodeHash = hasher(hostname) % modulus;
        //std::cout << "nodeId: " << nodeId << " nodeHash: " << nodeHash << "\n";
        nodeHashes.push_back({circularDistance(nodeHash, filenameHash, modulus), hostname});
    }

    std::sort(nodeHashes.begin(), nodeHashes.end());
    std::vector<std::string> successors;
    size_t i = 0;

    for (const auto& node : nodeHashes) {
        if (i==3) break;
        successors.push_back(node.second);
        i++;        
    } 
    return successors;
}