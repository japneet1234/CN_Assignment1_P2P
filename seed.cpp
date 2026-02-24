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

public:
    SeedNode(int port, string outputFile) : seedPort(port), logFileName(outputFile) {
        serverSocket = -1;
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
                             const string& reporterIp, const string& timestamp) {
        string key = deadNodeIp + ":" + to_string(deadNodePort);
        
        lock_guard<mutex> lock(peerListMutex);
        
        if (peerList.find(key) != peerList.end()) {
            peerList[key].isActive = false;
            logMessage("REMOVAL: Dead node " + deadNodeIp + ":" + to_string(deadNodePort) + 
                      " reported by " + reporterIp + " at " + timestamp);
        }
    }
    
    void handleClientConnection(int clientSocket) {
        char buffer[1024] = {0};
        
        int valread = recv(clientSocket, buffer, sizeof(buffer) - 1, 0);
        if (valread <= 0) {
            close(clientSocket);
            return;
        }
        
        string request(buffer);
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
            string deadNodeIpPort, timestamp, reporterIp;
            ss >> deadNodeIpPort >> timestamp >> reporterIp;
            
            int colonPos = deadNodeIpPort.find(':');
            string deadIp = deadNodeIpPort.substr(0, colonPos);
            int deadPort = stoi(deadNodeIpPort.substr(colonPos + 1));
            
            handleDeadNodeReport(deadIp, deadPort, reporterIp, timestamp);
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
        cerr << "Usage: ./seed <port>" << endl;
        return 1;
    }
    
    int port = stoi(argv[1]);
    string outputFile = "seed_output_" + to_string(port) + ".txt";
    
    SeedNode seed(port, outputFile);
    seed.startListening();
    
    return 0;
}
