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
#include <algorithm>
#include <functional>
#include <random>
#include <math.h>

using namespace std;

struct GossipMessage {
    string hash;
    string content;
    string senderIp;
    chrono::system_clock::time_point receivedTime;
};

struct PeerConnection {
    string ip;
    int port;
    int socket;
    bool isConnected;
    int failureCount;
};

struct SuspicionReport {
    string deadPeerKey;
    set<string> reportingPeers;
};

class PeerNode {
private:
    string selfIp;
    int selfPort;
    int serverSocket;
    
    vector<pair<string, int>> seedNodes;
    int totalSeeds;
    int requiredSeeds;
    
    vector<PeerConnection> neighbors;
    map<string, GossipMessage> messageList;
    
    int messageCounter;
    string logFileName;
    mutex logMutex;
    mutex neighborMutex;
    mutex messageMutex;
    
    bool networkFormed;
    map<string, SuspicionReport> deadNodeSuspicions;
    mutex suspicionMutex;
    
    vector<thread> threadPool;

public:
    PeerNode(string ip, int port, string seedConfigFile) 
        : selfIp(ip), selfPort(port), messageCounter(0), networkFormed(false) {
        
        logFileName = "peer_" + ip + "_" + to_string(port) + ".txt";
        serverSocket = -1;
        totalSeeds = 0;
        requiredSeeds = 0;
        
        readSeedConfig(seedConfigFile);
        initializeServer();
    }
    
    ~PeerNode() {
        if (serverSocket != -1) {
            close(serverSocket);
        }
        for (auto& peer : neighbors) {
            if (peer.socket != -1) {
                close(peer.socket);
            }
        }
    }
    
    void readSeedConfig(const string& configFile) {
        ifstream file(configFile);
        if (!file.is_open()) {
            cerr << "Error reading seed config file: " << configFile << endl;
            return;
        }
        
        string line;
        while (getline(file, line)) {
            if (line.empty() || line[0] == '#') continue;
            
            int colonPos = line.find(':');
            if (colonPos != string::npos) {
                string ip = line.substr(0, colonPos);
                int port = stoi(line.substr(colonPos + 1));
                seedNodes.push_back({ip, port});
                totalSeeds++;
            }
        }
        file.close();
        
        requiredSeeds = (totalSeeds / 2) + 1;
        logMessage("Read " + to_string(totalSeeds) + " seed nodes. Need " + to_string(requiredSeeds) + " for quorum");
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
    
    void initializeServer() {
        serverSocket = socket(AF_INET, SOCK_STREAM, 0);
        if (serverSocket < 0) {
            cerr << "Error creating socket" << endl;
            return;
        }
        
        int opt = 1;
        setsockopt(serverSocket, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
        
        struct sockaddr_in address;
        address.sin_family = AF_INET;
        address.sin_addr.s_addr = inet_addr(selfIp.c_str());
        address.sin_port = htons(selfPort);
        
        if (bind(serverSocket, (struct sockaddr*)&address, sizeof(address)) < 0) {
            cerr << "Error binding socket on port " << selfPort << endl;
            return;
        }
        
        listen(serverSocket, 10);
        logMessage("Peer node initialized on " + selfIp + ":" + to_string(selfPort));
    }
    
    bool registerWithSeeds() {
        int successfulRegistrations = 0;
        
        for (auto& seed : seedNodes) {
            if (registerWithSeed(seed.first, seed.second)) {
                successfulRegistrations++;
            }
        }
        
        logMessage("Registered with " + to_string(successfulRegistrations) + " seeds (required: " + 
                  to_string(requiredSeeds) + ")");
        
        return successfulRegistrations >= requiredSeeds;
    }
    
    bool registerWithSeed(const string& seedIp, int seedPort) {
        int sock = socket(AF_INET, SOCK_STREAM, 0);
        if (sock < 0) {
            return false;
        }
        
        struct sockaddr_in address;
        address.sin_family = AF_INET;
        address.sin_addr.s_addr = inet_addr(seedIp.c_str());
        address.sin_port = htons(seedPort);
        
        if (connect(sock, (struct sockaddr*)&address, sizeof(address)) < 0) {
            close(sock);
            return false;
        }
        
        string message = "REGISTER " + selfIp + ":" + to_string(selfPort);
        send(sock, message.c_str(), message.length(), 0);
        
        char buffer[1024] = {0};
        recv(sock, buffer, sizeof(buffer) - 1, 0);
        
        string response(buffer);
        close(sock);
        
        bool success = (response.find("REGISTERED") != string::npos);
        if (success) {
            logMessage("Successfully registered with seed " + seedIp + ":" + to_string(seedPort));
        }
        return success;
    }
    
    vector<pair<string, int>> getPeerListFromSeeds() {
        set<pair<string, int>> uniquePeers;
        
        for (auto& seed : seedNodes) {
            vector<pair<string, int>> peers = getPeerListFromSeed(seed.first, seed.second);
            for (auto& p : peers) {
                if (p.first != selfIp || p.second != selfPort) {
                    uniquePeers.insert(p);
                }
            }
        }
        
        vector<pair<string, int>> result(uniquePeers.begin(), uniquePeers.end());
        logMessage("Obtained " + to_string(result.size()) + " peers from seeds");
        
        return result;
    }
    
    vector<pair<string, int>> getPeerListFromSeed(const string& seedIp, int seedPort) {
        vector<pair<string, int>> peers;
        
        int sock = socket(AF_INET, SOCK_STREAM, 0);
        if (sock < 0) {
            return peers;
        }
        
        struct sockaddr_in address;
        address.sin_family = AF_INET;
        address.sin_addr.s_addr = inet_addr(seedIp.c_str());
        address.sin_port = htons(seedPort);
        
        if (connect(sock, (struct sockaddr*)&address, sizeof(address)) < 0) {
            close(sock);
            return peers;
        }
        
        string message = "GETPEERLIST";
        send(sock, message.c_str(), message.length(), 0);
        
        char buffer[4096] = {0};
        recv(sock, buffer, sizeof(buffer) - 1, 0);
        
        string response(buffer);
        if (response.find("PEERLIST:") == 0) {
            string peerString = response.substr(9);
            stringstream ss(peerString);
            string peerEntry;
            
            while (getline(ss, peerEntry, ',')) {
                int colonPos = peerEntry.find(':');
                if (colonPos != string::npos) {
                    string ip = peerEntry.substr(0, colonPos);
                    int port = stoi(peerEntry.substr(colonPos + 1));
                    peers.push_back({ip, port});
                }
            }
        }
        
        close(sock);
        return peers;
    }
    
    void connectToNeighbors(const vector<pair<string, int>>& peerList) {
        lock_guard<mutex> lock(neighborMutex);
        
        int targetNeighbors = max(3, (int)peerList.size() / 3);
        
        random_device rd;
        mt19937 gen(rd());
        uniform_int_distribution<> dis(0, peerList.size() - 1);
        
        set<pair<string, int>> selected;
        while (selected.size() < targetNeighbors && selected.size() < peerList.size()) {
            selected.insert(peerList[dis(gen)]);
        }
        
        for (auto& peer : selected) {
            PeerConnection conn;
            conn.ip = peer.first;
            conn.port = peer.second;
            conn.socket = -1;
            conn.isConnected = false;
            conn.failureCount = 0;
            
            neighbors.push_back(conn);
        }
        
        logMessage("Selected " + to_string(neighbors.size()) + " neighbors for connection");
        
        for (int i = 0; i < neighbors.size(); i++) {
            thread t(&PeerNode::establishConnectionToNeighbor, this, i);
            threadPool.push_back(move(t));
        }
    }
    
    void establishConnectionToNeighbor(int index) {
        if (index < 0 || index >= neighbors.size()) return;
        
        PeerConnection& peer = neighbors[index];
        
        int sock = socket(AF_INET, SOCK_STREAM, 0);
        if (sock < 0) {
            return;
        }
        
        struct sockaddr_in address;
        address.sin_family = AF_INET;
        address.sin_addr.s_addr = inet_addr(peer.ip.c_str());
        address.sin_port = htons(peer.port);
        
        if (connect(sock, (struct sockaddr*)&address, sizeof(address)) < 0) {
            close(sock);
            peer.isConnected = false;
            return;
        }
        
        peer.socket = sock;
        peer.isConnected = true;
        logMessage("Connected to neighbor " + peer.ip + ":" + to_string(peer.port));
    }
    
    string generateMessageHash(const string& content) {
        unsigned int hash = 0;
        for (char c : content) {
            hash = ((hash << 5) + hash) + c;
        }
        return to_string(hash);
    }
    
    void generateAndBroadcastGossip() {
        int maxMessages = 10;
        
        for (int i = 0; i < maxMessages; i++) {
            this_thread::sleep_for(chrono::seconds(5));
            
            auto now = chrono::system_clock::now();
            time_t time = chrono::system_clock::to_time_t(now);
            
            messageCounter++;
            string content = to_string(time) + ":" + selfIp + ":" + to_string(messageCounter);
            string hash = generateMessageHash(content);
            
            {
                lock_guard<mutex> lock(messageMutex);
                GossipMessage msg;
                msg.hash = hash;
                msg.content = content;
                msg.senderIp = selfIp;
                msg.receivedTime = now;
                messageList[hash] = msg;
            }
            
            logMessage("GOSSIP: Generated message #" + to_string(messageCounter) + " - " + content);
            broadcastMessage(content, selfIp);
        }
    }
    
    void broadcastMessage(const string& content, const string& senderIp) {
        lock_guard<mutex> lock(neighborMutex);
        
        for (auto& peer : neighbors) {
            if (peer.isConnected && peer.ip != senderIp) {
                string fullMessage = "GOSSIP:" + content + ":" + selfIp;
                int sent = send(peer.socket, fullMessage.c_str(), fullMessage.length(), 0);
                if (sent < 0) {
                    peer.isConnected = false;
                }
            }
        }
    }
    
    void listenForIncomingMessages() {
        while (true) {
            struct sockaddr_in address;
            int addrlen = sizeof(address);
            
            int clientSocket = accept(serverSocket, (struct sockaddr*)&address, (socklen_t*)&addrlen);
            
            if (clientSocket < 0) {
                continue;
            }
            
            thread t(&PeerNode::handleIncomingMessage, this, clientSocket);
            threadPool.push_back(move(t));
        }
    }
    
    void handleIncomingMessage(int clientSocket) {
        char buffer[1024] = {0};
        
        int valread = recv(clientSocket, buffer, sizeof(buffer) - 1, 0);
        if (valread <= 0) {
            close(clientSocket);
            return;
        }
        
        string message(buffer);
        
        if (message.find("GOSSIP:") == 0) {
            size_t firstColon = message.find(':', 7);
            size_t secondColon = message.rfind(':');
            
            string content = message.substr(7, firstColon - 7);
            string senderIp = message.substr(secondColon + 1);
            
            string hash = generateMessageHash(content);
            
            {
                lock_guard<mutex> lock(messageMutex);
                
                if (messageList.find(hash) == messageList.end()) {
                    GossipMessage msg;
                    msg.hash = hash;
                    msg.content = content;
                    msg.senderIp = senderIp;
                    msg.receivedTime = chrono::system_clock::now();
                    messageList[hash] = msg;
                    
                    logMessage("GOSSIP RECEIVED: " + content + " from " + senderIp);
                    broadcastMessage(content, senderIp);
                } else {
                    logMessage("Duplicate gossip message ignored: " + content);
                }
            }
        }
        
        close(clientSocket);
    }
    
    void periodicLivenessCheck() {
        while (true) {
            this_thread::sleep_for(chrono::seconds(3));
            
            lock_guard<mutex> lock(neighborMutex);
            
            for (auto& peer : neighbors) {
                bool isAlive = checkPeerLiveness(peer.ip, peer.port);
                
                if (!isAlive) {
                    peer.failureCount++;
                    
                    if (peer.failureCount >= 2) {
                        reportDeadNode(peer.ip, peer.port);
                        peer.isConnected = false;
                    }
                } else {
                    peer.failureCount = 0;
                }
            }
        }
    }
    
    bool checkPeerLiveness(const string& ip, int port) {
        string pingCommand = "ping -c 1 -W 1 " + ip + " > /dev/null 2>&1";
        int result = system(pingCommand.c_str());
        return (result == 0);
    }
    
    void reportDeadNode(const string& deadIp, int deadPort) {
        auto now = chrono::system_clock::now();
        time_t time = chrono::system_clock::to_time_t(now);
        
        logMessage("DEAD NODE DETECTED: " + deadIp + ":" + to_string(deadPort));
        
        for (auto& seed : seedNodes) {
            int sock = socket(AF_INET, SOCK_STREAM, 0);
            if (sock < 0) continue;
            
            struct sockaddr_in address;
            address.sin_family = AF_INET;
            address.sin_addr.s_addr = inet_addr(seed.first.c_str());
            address.sin_port = htons(seed.second);
            
            if (connect(sock, (struct sockaddr*)&address, sizeof(address)) < 0) {
                close(sock);
                continue;
            }
            
            string report = "DEADNODE " + deadIp + ":" + to_string(deadPort) + " " + 
                           to_string(time) + " " + selfIp;
            send(sock, report.c_str(), report.length(), 0);
            
            logMessage("Sent dead node report for " + deadIp + ":" + to_string(deadPort) + 
                      " to seed " + seed.first + ":" + to_string(seed.second));
            
            close(sock);
        }
    }
    
    void start() {
        logMessage("Starting peer node...");
        
        if (!registerWithSeeds()) {
            logMessage("ERROR: Could not register with enough seeds. Exiting.");
            return;
        }
        
        vector<pair<string, int>> peerList = getPeerListFromSeeds();
        
        connectToNeighbors(peerList);
        
        this_thread::sleep_for(chrono::seconds(2));
        networkFormed = true;
        
        logMessage("Network formed successfully with " + to_string(neighbors.size()) + " neighbors");
        
        thread listeningThread(&PeerNode::listenForIncomingMessages, this);
        threadPool.push_back(move(listeningThread));
        
        thread livenessThread(&PeerNode::periodicLivenessCheck, this);
        threadPool.push_back(move(livenessThread));
        
        thread gossipThread(&PeerNode::generateAndBroadcastGossip, this);
        threadPool.push_back(move(gossipThread));
        
        for (auto& t : threadPool) {
            if (t.joinable()) {
                t.join();
            }
        }
    }
};

int main(int argc, char* argv[]) {
    if (argc < 4) {
        cerr << "Usage: ./peer <self_ip> <self_port> <seed_config_file>" << endl;
        return 1;
    }
    
    string selfIp = argv[1];
    int selfPort = stoi(argv[2]);
    string seedConfigFile = argv[3];
    
    PeerNode peer(selfIp, selfPort, seedConfigFile);
    peer.start();
    
    return 0;
}
