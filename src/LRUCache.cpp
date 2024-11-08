#include <list>
#include <unordered_map>
#include <assert.h>
#include <vector>
#include <string>
#include <fstream>
#include <iostream>
#include "LRUCache.h"
using namespace std;

std::vector<char> readFileIntoVector(const std::string& filename) {
    std::ifstream file(filename, std::ios::binary); 
    if (!file) {
        std::cout << "Failed to open file: " << filename << "\n";
        return std::vector<char>(); 
    }
    return std::vector<char>(std::istreambuf_iterator<char>(file), std::istreambuf_iterator<char>());
}

LRUCache::LRUCache(int cache_capacity): cache_size(0), cache_capacity(cache_capacity) {}
void LRUCache::clean(){
    while(cache_size > cache_capacity) {
        auto last_it = item_list.end(); 
        last_it--;
        cache_size -= last_it->second.first;
        item_map.erase(last_it->first);
        item_list.pop_back();
    }
}
size_t LRUCache::size(){return cache_size;}
size_t LRUCache::capacity(){return cache_capacity;}
void LRUCache::remove(const string &key) {
    auto it = item_map.find(key);
    if (it != item_map.end()){
        cout << "Removing file: " << key << "\n";
        cache_size -= it->second->second.first; // hmm
        item_list.erase(it->second);
        item_map.erase(it);
    }
}
// filename, (filesize, content)
void LRUCache::put(const string &key, const pair<size_t,vector<char>> &val){
    if (val.first > cache_capacity) {
        cout << "file greater than cache capacity, not caching" << "\n";
        return;
    }
    auto it = item_map.find(key);
    if(it != item_map.end()){
        item_list.erase(it->second);
        item_map.erase(it);
        cache_size -= val.first;
    }
    cout << "Caching file: " << key << "\n";
    cache_size += val.first;
    item_list.push_front(make_pair(key,val));
    item_map.insert(make_pair(key, item_list.begin()));
    clean();
}

bool LRUCache::exist(const string &key){
    return (item_map.count(key)>0);
}

pair<size_t,vector<char>> LRUCache::get(const string &key){
    if (!exist(key)){
        cout << "file not in cache: " << key << "\n";
    }
    cout << "found in cache: " << key << "\n";
    auto it = item_map.find(key);
    item_list.splice(item_list.begin(), item_list, it->second);
    return it->second->second;
}

