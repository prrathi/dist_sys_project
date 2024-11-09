#pragma once

#include <iostream>
#include <string>
#include <utility>
#include <vector>
#include <sstream>
#include <algorithm> 
#include <unordered_set>

size_t hashString(const std::string& str, size_t modulus);
std::vector<std::pair<std::string, std::pair<size_t, size_t>>> find3Successors(const std::string& filename, 
                                        const std::unordered_set<std::string>& nodeIds, 
                                        size_t modulus);
std::string HostToIp(const std::string& host);