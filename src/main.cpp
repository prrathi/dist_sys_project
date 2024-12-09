#include <iostream>
#include <string>
#include <unistd.h>
#include "rainstorm_factory_server.h"
#include "rainstorm_leader.h"

int main(int argc, char* argv[]) {
    if (argc != 2) {
        std::cerr << "Usage: " << argv[0] << " [leader|node]" << std::endl;
        return 1;
    }

    std::string mode = argv[1];
    if (mode != "leader" && mode != "factory") {
        std::cerr << "Invalid mode. Use 'leader' or 'node'" << std::endl;
        return 1;
    } else if (mode == "leader") {
        RainStormLeader leader;
        leader.runHydfs();
        leader.runServer();
    } else if (mode == "factory") {
        RainstormFactory factory;
        factory.runHydfs();
        factory.runServer();
    }
    return 0;
}