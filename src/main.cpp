#include <iostream>
#include <cstdlib> 
#include <string>
#include <thread>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <ifaddrs.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <condition_variable>
#include <mutex>
#include <algorithm>
#include <random>
#include <chrono>
#include <future>
#include <ctime>

#include "hydfs.h"
#include "file_transfer_server.cpp"




int main() {
    Hydfs hydfs;
    
    std::thread listener_thread([&hydfs](){ hydfs.pipeListener(); });
    std::thread swim_thread([&hydfs](){ hydfs.swim(); });
    std::thread server_thread(RunServer);

    listener_thread.join();
    swim_thread.join();
    server_thread.join();
    
    return 0;

}