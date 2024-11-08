#ifndef WORKER_H
#define WORKER_H

#include <vector>
#include "common.h"

void writeToLog(string filename, string message);
void updateState(FullNode& node, SwimMessage& message);
vector<PassNodeState> constructSendStates(FullNode& node, string targetId);
void handlePing(FullNode& node, string targetId);
void handleRequest(FullNode& node, char buf[]);
void printMessageType(SwimMessage& message);

#endif // WORKER_H