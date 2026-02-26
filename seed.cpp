#include <iostream>
#include <vector>
#include <map>
#include <set>
#include <string>
#include <sstream>
#include <fstream>
#include <thread>
#include <mutex>
#include <cstring>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <unistd.h>
#include <chrono>
#include <ctime>
#include <queue>

using namespace std;

struct PeerInfo {
    string ip;
    int port;
    chrono::system_clock::time_point registrationTime;
    bool isActive;
};

struct RegistrationProposal {
    string peerIp;
    int peerPort;
    int proposingSeeds;
};

class SeedNode {
private:
    int seedPort;
    map<string, PeerInfo> peerList;
    mutex peerListMutex;

    map<string, set<string>> deadNodeReporterVotes;
    map<string, set<string>> deadNodeSeedVotes;
    set<string> selfSeedVoted;
    mutex removalConsensusMutex;
    
    int serverSocket;
    vector<thread> threadPool;
    
    string logFileName;
    mutex logMutex;
    
    vector<pair<string, int>> otherSeeds;
    set<string> registrationVotes;
    set<string> removalVotes;
    mutex votingMutex;
    
    queue<string> pendingRegistrations;
    queue<string> pendingRemovals;

    int requiredSeedVotes;

public:
    SeedNode(int port, string outputFile, const string& seedConfigPath) : seedPort(port), logFileName(outputFile) {
        serverSocket = -1;
        loadSeedConfig(seedConfigPath);
        requiredSeedVotes = ((int)otherSeeds.size() + 1) / 2 + 1;
        initializeServer();
    }
    
    ~SeedNode() {
        if (serverSocket != -1) {
            close(serverSocket);
        }
    }
    
    void initializeServer() {
        serverSocket = socket(AF_INET, SOCK_STREAM, 0);
        if (serverSocket < 0) {
            cerr << "Error creating socket" << endl;
            return;
        }
        
        int opt = 1;
        if (setsockopt(serverSocket, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
            cerr << "Error setting socket options" << endl;
            return;
        }
        
        struct sockaddr_in address;
        address.sin_family = AF_INET;
        address.sin_addr.s_addr = INADDR_ANY;
        address.sin_port = htons(seedPort);
        
        if (bind(serverSocket, (struct sockaddr*)&address, sizeof(address)) < 0) {
            cerr << "Error binding socket on port " << seedPort << endl;
            return;
        }
        
        listen(serverSocket, 20);
        logMessage("Seed node initialized on port " + to_string(seedPort));
        logMessage("Seed removal quorum set to " + to_string(requiredSeedVotes) +
                   " out of total seeds=" + to_string((int)otherSeeds.size() + 1));
    }

    void loadSeedConfig(const string& configPath) {
        ifstream file(configPath);
        if (!file.is_open()) {
            logMessage("Could not read seed config at " + configPath + ", using standalone mode");
            return;
        }

        string line;
        while (getline(file, line)) {
            if (line.empty() || line[0] == '#') {
                continue;
            }

            size_t colon = line.find(':');
            if (colon == string::npos) {
                continue;
            }

            string ip = line.substr(0, colon);
            int port = stoi(line.substr(colon + 1));
            if (port != seedPort) {
                otherSeeds.push_back({ip, port});
            }
        }
    }

    int getRequiredRemovalReports(const string& deadNodeKey) {
        lock_guard<mutex> lock(peerListMutex);

        int activeWitnesses = 0;
        for (const auto& entry : peerList) {
            if (!entry.second.isActive) {
                continue;
            }
            if (entry.first == deadNodeKey) {
                continue;
            }
            activeWitnesses++;
        }

        if (activeWitnesses <= 0) {
            return 1;
        }

        return (activeWitnesses / 2) + 1;
    }

    void maybeApplyRemovalConsensus(const string& key, const string& deadNodeIp, int deadNodePort) {
        bool canRemove = false;
        {
            lock_guard<mutex> voteLock(removalConsensusMutex);
            canRemove = ((int)deadNodeSeedVotes[key].size() >= requiredSeedVotes);
        }

        if (!canRemove) {
            return;
        }

        lock_guard<mutex> lock(peerListMutex);
        if (peerList.find(key) != peerList.end() && peerList[key].isActive) {
            peerList[key].isActive = false;
            logMessage("REMOVAL CONSENSUS: Dead node " + deadNodeIp + ":" + to_string(deadNodePort) +
                       " removed after seed quorum votes=" + to_string(requiredSeedVotes));
        }
    }

    void broadcastSeedVote(const string& deadNodeIp, int deadNodePort) {
        string voteMessage = "SEEDVOTE_REMOVE " + deadNodeIp + ":" + to_string(deadNodePort) +
                             " " + to_string(seedPort);

        for (const auto& seed : otherSeeds) {
            int sock = socket(AF_INET, SOCK_STREAM, 0);
            if (sock < 0) {
                continue;
            }

            sockaddr_in addr{};
            addr.sin_family = AF_INET;
            addr.sin_addr.s_addr = inet_addr(seed.first.c_str());
            addr.sin_port = htons(seed.second);

            if (connect(sock, (sockaddr*)&addr, sizeof(addr)) < 0) {
                close(sock);
                continue;
            }

            send(sock, voteMessage.c_str(), voteMessage.size(), 0);
            close(sock);
        }
    }

    void castSelfSeedVoteIfNeeded(const string& key, const string& deadNodeIp, int deadNodePort) {
        bool newlyVoted = false;
        {
            lock_guard<mutex> voteLock(removalConsensusMutex);
            if (selfSeedVoted.find(key) == selfSeedVoted.end()) {
                selfSeedVoted.insert(key);
                deadNodeSeedVotes[key].insert(to_string(seedPort));
                newlyVoted = true;
                logMessage("SEED VOTE: cast local vote for " + key +
                           " (seedVotes=" + to_string((int)deadNodeSeedVotes[key].size()) +
                           "/" + to_string(requiredSeedVotes) + ")");
            }
        }

        if (!newlyVoted) {
            return;
        }

        broadcastSeedVote(deadNodeIp, deadNodePort);
        maybeApplyRemovalConsensus(key, deadNodeIp, deadNodePort);
    }

    void handleSeedVoteMessage(const string& deadNodeIp, int deadNodePort, const string& voterSeedPort) {
        string key = deadNodeIp + ":" + to_string(deadNodePort);
        {
            lock_guard<mutex> voteLock(removalConsensusMutex);
            deadNodeSeedVotes[key].insert(voterSeedPort);
            logMessage("SEED VOTE RECEIVED: " + key + " from seed " + voterSeedPort +
                       " (seedVotes=" + to_string((int)deadNodeSeedVotes[key].size()) +
                       "/" + to_string(requiredSeedVotes) + ")");
        }

        maybeApplyRemovalConsensus(key, deadNodeIp, deadNodePort);
    }
    
    void logMessage(const string& message) {
        lock_guard<mutex> lock(logMutex);
        
        auto now = chrono::system_clock::now();
        time_t time = chrono::system_clock::to_time_t(now);
        string timestamp = ctime(&time);
        timestamp.erase(timestamp.length() - 1);
        
        string logEntry = "[" + timestamp + "] " + message;
        cout << logEntry << endl;
        
        ofstream outFile(logFileName, ios::app);
        outFile << logEntry << endl;
        outFile.close();
    }
    
    void setPeerList(vector<pair<string, int>>& peers) {
        lock_guard<mutex> lock(peerListMutex);
        for (auto& peer : peers) {
            PeerInfo info;
            info.ip = peer.first;
            info.port = peer.second;
            info.registrationTime = chrono::system_clock::now();
            info.isActive = true;
            peerList[peer.first + ":" + to_string(peer.second)] = info;
        }
    }
    
    string getPeerListString() {
        lock_guard<mutex> lock(peerListMutex);
        stringstream ss;
        
        for (auto& entry : peerList) {
            if (entry.second.isActive) {
                ss << entry.second.ip << ":" << entry.second.port << ",";
            }
        }
        
        string result = ss.str();
        if (!result.empty() && result.back() == ',') {
            result.pop_back();
        }
        return result;
    }
    
    void handleRegistrationRequest(const string& peerIp, int peerPort, int clientSocket) {
        string key = peerIp + ":" + to_string(peerPort);
        
        lock_guard<mutex> lock(peerListMutex);
        
        if (peerList.find(key) == peerList.end()) {
            PeerInfo newPeer;
            newPeer.ip = peerIp;
            newPeer.port = peerPort;
            newPeer.registrationTime = chrono::system_clock::now();
            newPeer.isActive = true;
            
            peerList[key] = newPeer;
            
            logMessage("REGISTRATION: Peer " + peerIp + ":" + to_string(peerPort) + " registered");
            cout << "Sending: REGISTERED" << endl;
            send(clientSocket, "REGISTERED", 10, 0);
        } else {
            logMessage("DUPLICATE: Peer " + peerIp + ":" + to_string(peerPort) + " already registered");
            send(clientSocket, "DUPLICATE", 9, 0);
        }
    }
    
    void handlePeerListRequest(int clientSocket) {
        string peerListStr = getPeerListString();
        string response = "PEERLIST:" + peerListStr;
        
        send(clientSocket, response.c_str(), response.length(), 0);
        logMessage("Sent peer list to client with " + to_string(peerList.size()) + " peers");
    }
    
    void handleDeadNodeReport(const string& deadNodeIp, int deadNodePort,
                             const string& reporterNode, const string& timestamp) {
        string key = deadNodeIp + ":" + to_string(deadNodePort);
        int requiredRemovalReports = getRequiredRemovalReports(key);

        string reporterKey = reporterNode;
        {
            lock_guard<mutex> voteLock(removalConsensusMutex);
            deadNodeReporterVotes[key].insert(reporterKey);
            int currentVotes = (int)deadNodeReporterVotes[key].size();
            logMessage("REMOVAL VOTE: " + key + " reported by " + reporterNode +
                       " at " + timestamp + " (votes=" + to_string(currentVotes) +
                       "/" + to_string(requiredRemovalReports) + ")");
        }

        bool removeNow = false;
        {
            lock_guard<mutex> voteLock(removalConsensusMutex);
            removeNow = ((int)deadNodeReporterVotes[key].size() >= requiredRemovalReports);
        }

        if (!removeNow) {
            return;
        }

        castSelfSeedVoteIfNeeded(key, deadNodeIp, deadNodePort);
    }
    
    void handleClientConnection(int clientSocket) {
        char buffer[1024] = {0};
        
        int valread = recv(clientSocket, buffer, sizeof(buffer) - 1, 0);
        if (valread <= 0) {
            close(clientSocket);
            return;
        }
        
        string request(buffer);

        if (request.rfind("Dead Node:", 0) == 0) {
            string payload = request.substr(strlen("Dead Node:"));
            stringstream deadSs(payload);
            string deadIp, deadPortStr, timestamp, reporterIp, reporterPort;
            getline(deadSs, deadIp, ':');
            getline(deadSs, deadPortStr, ':');
            getline(deadSs, timestamp, ':');
            getline(deadSs, reporterIp, ':');
            getline(deadSs, reporterPort, ':');

            if (!deadIp.empty() && !deadPortStr.empty() && !timestamp.empty() && !reporterIp.empty()) {
                int deadPort = stoi(deadPortStr);
                string reporterKey = reporterIp;
                if (!reporterPort.empty()) {
                    reporterKey += ":" + reporterPort;
                } else {
                    reporterKey += "@" + timestamp;
                }
                handleDeadNodeReport(deadIp, deadPort, reporterKey, timestamp);
            }

            close(clientSocket);
            return;
        }
        stringstream ss(request);
        string command;
        ss >> command;
        
        if (command == "REGISTER") {
            string ipPort;
            ss >> ipPort;
            
            int colonPos = ipPort.find(':');
            string ip = ipPort.substr(0, colonPos);
            int port = stoi(ipPort.substr(colonPos + 1));
            
            handleRegistrationRequest(ip, port, clientSocket);
        } 
        else if (command == "GETPEERLIST") {
            handlePeerListRequest(clientSocket);
        }
        else if (command == "DEADNODE") {
            string deadNodeIpPort, timestamp, reporterNode;
            ss >> deadNodeIpPort >> timestamp >> reporterNode;
            
            int colonPos = deadNodeIpPort.find(':');
            string deadIp = deadNodeIpPort.substr(0, colonPos);
            int deadPort = stoi(deadNodeIpPort.substr(colonPos + 1));
            
            handleDeadNodeReport(deadIp, deadPort, reporterNode, timestamp);
        }
        else if (command == "SEEDVOTE_REMOVE") {
            string deadNodeIpPort, voterSeedPort;
            ss >> deadNodeIpPort >> voterSeedPort;

            int colonPos = deadNodeIpPort.find(':');
            if (colonPos != string::npos) {
                string deadIp = deadNodeIpPort.substr(0, colonPos);
                int deadPort = stoi(deadNodeIpPort.substr(colonPos + 1));
                handleSeedVoteMessage(deadIp, deadPort, voterSeedPort);
            }
        }
        
        close(clientSocket);
    }
    
    void startListening() {
        cout << "Seed node listening on port " << seedPort << endl;
        
        while (true) {
            struct sockaddr_in address;
            int addrlen = sizeof(address);
            
            int clientSocket = accept(serverSocket, (struct sockaddr*)&address, (socklen_t*)&addrlen);
            
            if (clientSocket < 0) {
                cerr << "Error accepting connection" << endl;
                continue;
            }
            
            thread t(&SeedNode::handleClientConnection, this, clientSocket);
            threadPool.push_back(move(t));
        }
    }
    
    void displayPeerList() {
        lock_guard<mutex> lock(peerListMutex);
        cout << "\n--- Current Peer List ---" << endl;
        for (auto& entry : peerList) {
            cout << entry.first << " (Active: " << (entry.second.isActive ? "Yes" : "No") << ")" << endl;
        }
        cout << "------------------------\n" << endl;
    }
};

int main(int argc, char* argv[]) {
    if (argc < 2) {
        cerr << "Usage: ./seed <port> [seed_config_file]" << endl;
        return 1;
    }
    
    int port = stoi(argv[1]);
    string outputFile = "seed_output_" + to_string(port) + ".txt";
    string configPath = "config.txt";
    if (argc >= 3) {
        configPath = argv[2];
    }
    
    SeedNode seed(port, outputFile, configPath);
    seed.startListening();
    
    return 0;
}
