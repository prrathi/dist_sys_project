#ifndef UTILS_H
#define UTILS_H

#include <iostream>
#include <string>
#include <vector>
#include <sstream>
#include <algorithm> 
#include <unordered_set>

using namespace std;

size_t circularDistance(size_t hash1, size_t hash2, size_t modulus);
size_t hashString(const string& str, size_t modulus);
vector<string> find3Successors(const string& filename, const unordered_set<string>& nodeIds, size_t modulus);

#endif // UTILS_H