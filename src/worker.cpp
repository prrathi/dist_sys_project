#include <thread>
#include <chrono>
#include <fstream>
#include <iostream>
#include "worker.h"
#include "talker.h"

#ifdef DEBUG
#define DBG(x) x
#else
#define DBG(x)
#endif

using namespace std;


void writeToLog(string filename, string message) {
  ofstream logFile(filename, ios::app); 
  logFile << message << endl;
  logFile.close();
}

void printMessageType(SwimMessage& message) {
  if (message.type == Ping) {
    cout << "Ping" << endl;
  } else if (message.type == Ack) {
    cout << "Ack" << endl;
  } else if (message.type == DingDong) {
    cout << "DingDong" << endl;
  } else if (message.type == Leave) {
    cout << "Leave" << endl;
  }
}

void updateState(FullNode& node, SwimMessage& message) {
  PassNodeState sourceState = node.getState(message.sourceId); 
  if (sourceState.nodeId != "") {
    message.states.push_back(sourceState); // consider alongside other states
  }
  for (PassNodeState msgState : message.states) {
    DBG(cout << "in update state" << msgState.nodeId << endl;)
    
    PassNodeState currState = node.getState(msgState.nodeId);
    if (currState.nodeId == "") { // does not exist in node
      writeToLog(node.getLogFile(), "On node " + node.getId() + ": Node " + msgState.nodeId + " is communicated to join at period " + to_string(node.getCurrentPeriod()) + ".");
      if (msgState.status == Sus && node.getSusStatus()) {
        msgState.susBeginPeriod = node.getCurrentPeriod();
        writeToLog(node.getLogFile(), "On node " + node.getId() + ": Node " + msgState.nodeId + " is communicated to be sus at period " + to_string(node.getCurrentPeriod()) + ".");
      }
      if (msgState.status != Dead) { // 
        node.updateState(msgState);
        node.addSendId(msgState.nodeId);
      }
    } 
    else if (msgState.status == Alive) { 
      if ((currState.status == Alive || (currState.status == Sus && node.getSusStatus())) && (currState.nodeIncarnation < msgState.nodeIncarnation)) {
        if (currState.status == Sus) {
          writeToLog(node.getLogFile(), "On node " + node.getId() + ": Node " + msgState.nodeId + " is communicated to not be sus at period " + to_string(node.getCurrentPeriod()) + ".");
        }
        currState.status = Alive;
        currState.nodeIncarnation = msgState.nodeIncarnation;
        node.updateState(currState);
        node.addSendId(msgState.nodeId);
      }
    } 
    else if (msgState.status == Dead) {
      if (currState.status != Dead) {
        currState.status = Dead;
        currState.deadBeginPeriod = node.getCurrentPeriod();
        node.updateState(currState);
        node.addSendId(msgState.nodeId);
        writeToLog(node.getLogFile(), "On node " + node.getId() + ": Node " + msgState.nodeId + " is communicated to have failed/left.");
        if (msgState.nodeId == node.getId()) {
          exit(0);
        }
      } 
    }
    else if (msgState.status == Sus && node.getSusStatus()) {
      if (msgState.nodeId == node.getId()) {
        if (currState.nodeIncarnation == msgState.nodeIncarnation) {
          currState.nodeIncarnation += 1;
        }
        writeToLog(node.getLogFile(), "On node " + node.getId() + ": Node " + msgState.nodeId + " found itself sus w incarnation " + to_string(currState.nodeIncarnation) + " and period " + to_string(node.getCurrentPeriod()));
        node.updateState(currState);
        node.addSendId(msgState.nodeId);
        auto stateIdsToSend = node.getStateIdsToSend();
        for (const auto& id : stateIdsToSend) {
          writeToLog(node.getLogFile(), "On node " + node.getId() + ": Node " + id + " is communicated to send at period " + to_string(node.getCurrentPeriod()) + ".");
        }
      } 
      else if (currState.status == Sus && currState.nodeIncarnation < msgState.nodeIncarnation) {
        cout << "On node " << node.getId() << ": Node " << msgState.nodeId << " is communicated to be sus with incarnation " << currState.nodeIncarnation << " and period " << node.getCurrentPeriod() << "." << endl;
        currState.nodeIncarnation = msgState.nodeIncarnation;
        currState.susBeginPeriod = node.getCurrentPeriod();
        node.updateState(currState);
        node.addSendId(msgState.nodeId);
        writeToLog(node.getLogFile(), "On node " + node.getId() + ": Node " + msgState.nodeId + " is communicated to be sus still w incarnation " + to_string(currState.nodeIncarnation) + " and period " + to_string(node.getCurrentPeriod()));
      } 
      else if (currState.status == Alive && currState.nodeIncarnation <= msgState.nodeIncarnation) {
        cout << "On node " << node.getId() << ": Node " << msgState.nodeId << " is communicated to be sus with incarnation " << currState.nodeIncarnation << " and period " << node.getCurrentPeriod() << "." << endl;
        currState.status = Sus;
        currState.nodeIncarnation = msgState.nodeIncarnation;
        currState.susBeginPeriod = node.getCurrentPeriod();
        node.updateState(currState);
        node.addSendId(msgState.nodeId);
        writeToLog(node.getLogFile(), "On node " + node.getId() + ": Node " + msgState.nodeId + " is communicated to be sus first time w incarnation " + to_string(currState.nodeIncarnation) + " and period " + to_string(node.getCurrentPeriod()));
      }
    }
  }
}


vector<PassNodeState> constructSendStates(FullNode& node, string targetId) {
  vector<PassNodeState> passNodes;
  unordered_set<string> newIds = node.getNewIds();
  if (newIds.find(targetId) != newIds.end()) {
    for (const auto& id : node.getAllIds()) {
      passNodes.push_back(node.getState(id));
    }
    node.removeNewId(targetId);
  }
  else {
    for (const auto& id : node.getStateIdsToSend()) {
      passNodes.push_back(node.getState(id));
    }
  }
  return passNodes;
}

void handlePing(FullNode& node, string targetId) {
  if (node.getState(targetId).status == Dead) {
    return;
  }
  SwimMessage pingMessage(node.getId(), node.getState(node.getId()).nodeIncarnation, targetId, Ping, node.getCurrentPeriod(), constructSendStates(node, targetId));
  node.setPingResponded(false);
  node.setPingOpen(true);
  sendUdpRequest(node.getState(targetId).nodeHostname, serializeMessage(pingMessage));
  if (!node.getSusStatus()) {
    std::this_thread::sleep_for(chrono::milliseconds(50));
      sendUdpRequest(node.getState(targetId).nodeHostname, serializeMessage(pingMessage));
    std::this_thread::sleep_for(chrono::milliseconds(50));
      sendUdpRequest(node.getState(targetId).nodeHostname, serializeMessage(pingMessage));
  }
  std::this_thread::sleep_for(chrono::milliseconds(node.getPingTime()));
  node.setPingOpen(false);
  if (!node.getPingResponded()) {
    PassNodeState targetState = node.getState(targetId);
    if (targetState.status == Alive && node.getSusStatus()) {
      targetState.status = Sus;
      targetState.susBeginPeriod = node.getCurrentPeriod();
      node.updateState(targetState);
      writeToLog(node.getLogFile(), "On node " + node.getId() + ": Node " + targetId + " is detected to be sus at period " + to_string(node.getCurrentPeriod()) + ".");
    }
    else if (targetState.status != Dead && !node.getSusStatus()) {
      targetState.status = Dead;
      targetState.deadBeginPeriod = node.getCurrentPeriod();
      node.updateState(targetState);
      writeToLog(node.getLogFile(), "On node " + node.getId() + ": Node " + targetId + " is detected to have failed.");
    }
    node.addSendId(targetId);
  }
}


void handleRequest(FullNode& node, char buf[]) {
  try {
  SwimMessage message = deserializeMessage(buf);
  DBG(printMessageType(message));
  if (message.targetId != node.getId() && message.type != DingDong) {
    return;
  }
  if (message.type == Ping) {
    updateState(node, message); // u donkey  might need to change this 
    // send multiple
    SwimMessage ackMessage(node.getId(), node.getState(node.getId()).nodeIncarnation, message.sourceId, Ack, message.period, constructSendStates(node, message.sourceId));
    sendUdpRequest(node.getState(message.sourceId).nodeHostname, serializeMessage(ackMessage));
    // if (!node.getSusStatus()) {
    //   std::this_thread::sleep_for(chrono::milliseconds(50));
    //   SwimMessage ackMessage2(node.getId(), node.getState(node.getId()).nodeIncarnation, message.sourceId, Ack, message.period, constructSendStates(node, message.sourceId));
    //     sendUdpRequest(node.getState(message.sourceId).nodeHostname, serializeMessage(ackMessage2));
    //   std::this_thread::sleep_for(chrono::milliseconds(50));
    //   SwimMessage ackMessage3(node.getId(), node.getState(node.getId()).nodeIncarnation, message.sourceId, Ack, message.period, constructSendStates(node, message.sourceId));
    //     sendUdpRequest(node.getState(message.sourceId).nodeHostname, serializeMessage(ackMessage3)); 
    // }
  } 
  else if (message.type == Ack && message.period == node.getCurrentPeriod() && node.getPingOpen()) {
    node.setPingResponded(true);
    node.setPingOpen(false);
    updateState(node, message);
  } 
  else if (message.type == DingDong && node.getIsIntroducer()) {
    node.addNewId(message.sourceId);
    updateState(node, message);
    writeToLog(node.getLogFile(), "On node " + node.getId() + ": Node " + message.sourceId + " is detected to have joined.");
    return; // no update from new node
  } 
  else if (message.type == Leave) {
    PassNodeState sourceState = node.getState(message.sourceId);
    sourceState.status = Dead;
    sourceState.deadBeginPeriod = node.getCurrentPeriod();
    node.updateState(sourceState);
    node.addSendId(message.sourceId);
    updateState(node, message);
    writeToLog(node.getLogFile(), "On node " + node.getId() + ": Node " + message.sourceId + " has left.");
  }
  } catch (...) {
    return;
  }
}
