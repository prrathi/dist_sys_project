#include <list>
#include <unordered_map>
#include <assert.h>
#include <vector>
#include <string>
#include <iostream>

class LRUCache{
public:
        LRUCache(int cache_capacity);
        size_t size();
        size_t capacity();
        void put(const std::string &key, const std::pair<size_t,std::vector<char>> &val);
        bool exist(const std::string &key);
        std::pair<size_t,std::vector<char>> get(const std::string &key);
        void remove(const std::string &key);
private:
        void clean();
private:
        std::list< std::pair<std::string, std::pair<size_t,std::vector<char>> > > item_list;
        std::unordered_map<std::string, decltype(item_list.begin()) > item_map;
        size_t cache_size;
        size_t cache_capacity; // in bytes
};