#ifndef COMMON_H
#define COMMON_H

#include <string>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <mutex>
#include "json.hpp"
#include <iostream>

using namespace std;
using json = nlohmann::json;

enum NodeStatus {
  Alive,
  Dead,
  Sus
};

struct PassNodeState {
  string nodeId;
  uint16_t nodeIncarnation;
  string nodeHostname;
  NodeStatus status;
  uint16_t susBeginPeriod;
  uint16_t deadBeginPeriod;
  PassNodeState() : nodeId(""), nodeIncarnation(0), nodeHostname(""), status(Dead), susBeginPeriod(0), deadBeginPeriod(0){}
  PassNodeState(string nId, uint16_t nIncarnation, string nHostname, NodeStatus nStatus, uint16_t nSusBeginPeriod, uint16_t nDeadBeginPeriod)
    : nodeId(nId), nodeIncarnation(nIncarnation), nodeHostname(nHostname), status(nStatus), susBeginPeriod(nSusBeginPeriod), deadBeginPeriod(nDeadBeginPeriod){}
};

void print_state(const PassNodeState& pns);

class FullNode {
public:
  FullNode();
  FullNode(bool isIntro, string nId, const vector<PassNodeState>& nodeStates, uint16_t cPeriod, bool pResponded, bool pOpen, float dRate, uint16_t tPing, uint16_t tPeriod, uint16_t susPeriod, bool hSusStatus, string logFile);
  FullNode(const FullNode& other);
  FullNode& operator=(const FullNode& other);
  bool getIsIntroducer() const;
  string getId() const;
  unordered_set<string> getAllIds() const;
  const unordered_set<string>& getNewIds() const;
  const vector<string>& getStateIdsToSend() const;
  void updateStateIdsToSend();
  uint16_t getCurrentPeriod() const;
  bool getPingResponded() const;
  bool getPingOpen() const;
  float getDropRate() const;
  uint16_t getPingTime() const;
  uint16_t getPeriodTime() const;
  uint16_t getSusPeriod() const; 
  void setPingTime(uint16_t time);
  void setPeriodTime(uint16_t time);
  bool getSusStatus() const;
  const PassNodeState getState(string nodeId) const;
  string getLogFile() const;

  void setCurrentPeriod(uint16_t period);
  void setPingResponded(bool responded);
  void setPingOpen(bool open);
  void setSusStatus(bool status);

  void addNewId(string newId);
  void removeNewId(string id);
  void addSendId(string id);
  void removeSendId(string id);
  uint16_t getSendIdPeriod(string id);

  void removeNode(string nodeId);
  void updateState(const PassNodeState& nodeState);

private:
  bool isIntroducer;
  string id;
  unordered_set<string> newIds;
  unordered_map<string, PassNodeState> nodes;
  vector<string> stateIdsToSend;
  uint16_t currentPeriod;
  unordered_map<string, uint16_t> sendIdPeriods;

  bool pingResponded = false;
  bool pingOpen = false;

  float dropRate;
  uint16_t pingTimeMs;
  uint16_t periodTimeMs;
  uint16_t susPeriod;
  bool haveSusStatus;
  string logFile;
  

  mutable mutex mtx;
};

enum MessageType {
  Ping,
  Ack,
  DingDong,
  Leave
};

struct SwimMessage {
  string sourceId;
  uint16_t sourceIncarnation;
  string targetId;
  MessageType type;
  uint16_t period;
  vector<PassNodeState> states;
  SwimMessage(string sId, uint16_t sIncarnation, string tId, MessageType mType, uint16_t mPeriod, vector<PassNodeState> nStates)
    : sourceId(sId), sourceIncarnation(sIncarnation), targetId(tId), type(mType), period(mPeriod), states(nStates) {}
};

void to_json(json& j, const PassNodeState& pns);
void from_json(const json& j, PassNodeState& pns);

string serializeMessage(SwimMessage message);
SwimMessage deserializeMessage(string message);

#endif // COMMON_H
