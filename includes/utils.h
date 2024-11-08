#pragma once
#include <iostream>
#include <string>
#include <vector>
#include <sstream>
#include <algorithm> 
#include <unordered_set>

size_t circularDistance(size_t hash1, size_t hash2, size_t modulus);

std::vector<std::string> find3Successors(const std::string& filename, const std::unordered_set<std::string>& nodeIds);