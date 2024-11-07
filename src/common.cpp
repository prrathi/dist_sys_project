#include "common.h"

#ifdef DEBUG
#define DBG(x) x
#else
#define DBG(x)
#endif

using namespace std;
using json = nlohmann::json;

void print_state(const PassNodeState& pns) {
    std::cout << "PassNodeState: ";
    std::cout << "id: " << pns.nodeId << ", ";
    std::cout << "incarnation: " << pns.nodeIncarnation << ", ";
    std::cout << "hostname: " << pns.nodeHostname << ", ";
    std::cout << "status: " << (pns.status == Alive ? "Alive" : pns.status == Dead ? "Dead" : "Sus") << ", ";
    std::cout << "susBeginPeriod: " << pns.susBeginPeriod << ", ";
    std::cout << "deadBeginPeriod: " << pns.deadBeginPeriod << std::endl;
}

FullNode::FullNode() {}
FullNode::FullNode(bool isIntro, string nId, const vector<PassNodeState>& nodeStates, uint16_t cPeriod, bool pResponded, bool pOpen, float dRate, uint16_t tPing, uint16_t tPeriod, uint16_t susPeriod, bool hSusStatus, string logFile)
  : isIntroducer(isIntro), id(nId), currentPeriod(cPeriod), pingResponded(pResponded), pingOpen(pOpen), dropRate(dRate), pingTimeMs(tPing), periodTimeMs(tPeriod), susPeriod(susPeriod), haveSusStatus(hSusStatus), logFile(logFile) {
  for (PassNodeState state : nodeStates) {
    nodes[state.nodeId] = state;
  }
}
FullNode::FullNode(const FullNode& other) 
  : isIntroducer(other.isIntroducer), id(other.id), currentPeriod(other.currentPeriod), pingResponded(other.pingResponded), pingOpen(other.pingOpen), dropRate(other.dropRate), pingTimeMs(other.pingTimeMs), periodTimeMs(other.periodTimeMs), susPeriod(other.susPeriod), haveSusStatus(other.haveSusStatus), logFile(other.logFile) {
  for (const auto& pair : other.nodes) {
    nodes[pair.first] = pair.second;
  }
}

FullNode& FullNode::operator=(const FullNode& other) {
  if (this != &other) {
    isIntroducer = other.isIntroducer;
    id = other.id;
    currentPeriod = other.currentPeriod;
    pingResponded = other.pingResponded;
    pingOpen = other.pingOpen;
    dropRate = other.dropRate;
    pingTimeMs = other.pingTimeMs;
    periodTimeMs = other.periodTimeMs;
    susPeriod = other.susPeriod;
    haveSusStatus = other.haveSusStatus;
    logFile = other.logFile;
    for (const auto& pair : other.nodes) {
      nodes[pair.first] = pair.second;
    }
  }
  return *this;
}

bool FullNode::getIsIntroducer() const { return isIntroducer; }
string FullNode::getId() const { return id; }
unordered_set<string> FullNode::getAllIds() const { 
  std::lock_guard<std::mutex> lock(mtx);
  unordered_set<string> allIds;
  for (const auto& pair : nodes) {
    allIds.insert(pair.first);
  }
  return allIds; 
}
const unordered_set<string>& FullNode::getNewIds() const { 
  std::lock_guard<std::mutex> lock(mtx);
  return newIds; 
}
const vector<string>& FullNode::getStateIdsToSend() const { 
  std::lock_guard<std::mutex> lock(mtx);
  return stateIdsToSend; 
}

void FullNode::updateStateIdsToSend() {
  std::lock_guard<std::mutex> lock(mtx);
  std::vector<string> toSend;
  for (const auto& pair : nodes) {
    DBG(cout << pair.first << " " << sendIdPeriods[pair.first] << " " << currentPeriod << " " << (uint16_t)(2 * (nodes.size()) - 1) << endl;)
    if (currentPeriod - sendIdPeriods[pair.first] <= (uint16_t)(2 * (nodes.size()) - 1)) {
      toSend.push_back(pair.first);
    }
  }
  stateIdsToSend = toSend;
}


uint16_t FullNode::getCurrentPeriod() const { 
  std::lock_guard<std::mutex> lock(mtx);
  return currentPeriod; 
}
bool FullNode::getPingResponded() const { 
  std::lock_guard<std::mutex> lock(mtx);
  return pingResponded; 
}
bool FullNode::getPingOpen() const { 
  std::lock_guard<std::mutex> lock(mtx);
  return pingOpen; 
}
float FullNode::getDropRate() const { 
  std::lock_guard<std::mutex> lock(mtx);
  return dropRate; 
}
uint16_t FullNode::getPingTime() const { return pingTimeMs; }
uint16_t FullNode::getPeriodTime() const { return periodTimeMs; }
uint16_t FullNode::getSusPeriod() const { return susPeriod; }

void FullNode::setPingTime(uint16_t time) {
  std::lock_guard<std::mutex> lock(mtx);
  pingTimeMs = time;
}
void FullNode::setPeriodTime(uint16_t time) {
  std::lock_guard<std::mutex> lock(mtx);
  periodTimeMs = time;
}
bool FullNode::getSusStatus() const { 
  std::lock_guard<std::mutex> lock(mtx);
  return haveSusStatus; 
}
const PassNodeState FullNode::getState(string nodeId) const {
  std::lock_guard<std::mutex> lock(mtx);
  auto it = nodes.find(nodeId);
  if (it != nodes.end()) {
    return it->second;
  }
  // If the node doesn't exist, return a default PassNodeState
  return PassNodeState();
}
string FullNode::getLogFile() const { return logFile; }

void FullNode::setCurrentPeriod(uint16_t period) { 
  std::lock_guard<std::mutex> lock(mtx);
  currentPeriod = period; 
}
void FullNode::setPingResponded(bool responded) { 
  std::lock_guard<std::mutex> lock(mtx);
  pingResponded = responded; 
}
void FullNode::setPingOpen(bool open) { 
  std::lock_guard<std::mutex> lock(mtx);
  pingOpen = open; 
}
void FullNode::setSusStatus(bool status) { 
  std::lock_guard<std::mutex> lock(mtx);
  haveSusStatus = status; 
}

void FullNode::addNewId(string newId) { 
  std::lock_guard<std::mutex> lock(mtx);
  newIds.insert(newId); 
}
void FullNode::removeNewId(string id) { 
  std::lock_guard<std::mutex> lock(mtx);
  newIds.erase(id); 
}

uint16_t FullNode::getSendIdPeriod(string id) {
  std::lock_guard<std::mutex> lock(mtx);
  auto it = sendIdPeriods.find(id);
  return it->second;
}

void FullNode::addSendId(string id) { 
  std::lock_guard<std::mutex> lock(mtx);
  sendIdPeriods[id] = currentPeriod; 
}
void FullNode::removeSendId(string id) { 
  std::lock_guard<std::mutex> lock(mtx);
  sendIdPeriods.erase(id); 
}

void FullNode::updateState(const PassNodeState& nodeState) {
  std::lock_guard<std::mutex> lock(mtx);
  nodes[nodeState.nodeId] = nodeState;
}

 void FullNode::removeNode(string nodeId) {
   std::lock_guard<std::mutex> lock(mtx);
   nodes.erase(nodeId);
   std::cout << "Removed node from member list: " << nodeId << std::endl;
 }

void to_json(json& j, const PassNodeState& pns) {
  j = json{{"nodeId", pns.nodeId}, {"nodeIncarnation", pns.nodeIncarnation}, {"nodeHostname", pns.nodeHostname}, {"status", pns.status}, {"susBeginPeriod", pns.susBeginPeriod}, {"deadBeginPeriod", pns.deadBeginPeriod}};
}

void from_json(const json& j, PassNodeState& pns) {
  j.at("nodeId").get_to(pns.nodeId);
  j.at("nodeIncarnation").get_to(pns.nodeIncarnation);
  j.at("nodeHostname").get_to(pns.nodeHostname);
  j.at("status").get_to(pns.status);
  j.at("susBeginPeriod").get_to(pns.susBeginPeriod);
  j.at("deadBeginPeriod").get_to(pns.deadBeginPeriod);
}

string serializeMessage(SwimMessage message) {
  json j;
  j["sourceId"] = message.sourceId;
  j["sourceIncarnation"] = message.sourceIncarnation;
  j["targetId"] = message.targetId;
  j["type"] = message.type;
  j["period"] = message.period;
  j["states"] = message.states;
  return j.dump();
}

SwimMessage deserializeMessage(string message) {
  json j = json::parse(message);
  string sourceId = j["sourceId"];
  uint16_t sourceIncarnation = j["sourceIncarnation"];
  string targetId = j["targetId"];
  MessageType type = j["type"];
  uint16_t period = j["period"];
  vector<PassNodeState> states;
  for (const auto& state : j["states"]) {
    PassNodeState pns;
    from_json(state, pns);
    states.push_back(pns);
  }
  return SwimMessage(sourceId, sourceIncarnation, targetId, type, period, states);
}
