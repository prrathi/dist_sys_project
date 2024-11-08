#include <list>
#include <unordered_map>
#include <assert.h>
#include <vector>
#include <string>
#include <iostream>
using namespace std;

std::vector<char> readFileIntoVector(const std::string& filename);
class LRUCache{
public:
        LRUCache(int cache_capacity);
        size_t size();
        size_t capacity();
        void put(const string &key, const pair<size_t,vector<char>> &val);
        bool exist(const string &key);
        pair<size_t,vector<char>> get(const string &key);
        void remove(const string &key);
private:
        void clean();
private:
        list< pair<string, pair<size_t,vector<char>> > > item_list;
        unordered_map<string, decltype(item_list.begin()) > item_map;
        size_t cache_size;
        size_t cache_capacity; // in bytes
};