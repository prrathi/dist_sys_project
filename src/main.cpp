#include <iostream>
#include <string>
#include <unistd.h>
#include "rainstorm_node.h"

int main() {
    char hostname[256];
    if (gethostname(hostname, sizeof(hostname)) != 0) {
        perror("gethostname");
        return 1;
    }
    std::string server_address = std::string(hostname) + ":8086";
    
    RainStormNode node(server_address);
    node.runHydfs();
    node.runServer();

    return 0;
}