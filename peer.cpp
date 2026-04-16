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
#include <algorithm>
#include <random>
#include <chrono>
#include <ctime>

#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <sys/select.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>

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
    bool isConnected;
    int failureCount;
    bool deadReported;
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
    map<string, set<string>> suspicionVotes;
    set<string> selfSuspectedDeadNodes;
    set<string> reportedDeadNodes;

    int messageCounter;
    string logFileName;

    mutex logMutex;
    mutex neighborMutex;
    mutex messageMutex;
    mutex suspicionMutex;

    vector<thread> threadPool;

public:
    PeerNode(const string& ip, int port, const string& seedConfigFile)
        : selfIp(ip), selfPort(port), serverSocket(-1), totalSeeds(0), requiredSeeds(0), messageCounter(0) {
        logFileName = "peer_" + ip + "_" + to_string(port) + ".txt";
        readSeedConfig(seedConfigFile);
        initializeServer();
    }

    ~PeerNode() {
        if (serverSocket != -1) {
            close(serverSocket);
        }
    }

    void logMessage(const string& message) {
        lock_guard<mutex> lock(logMutex);

        auto now = chrono::system_clock::now();
        time_t t = chrono::system_clock::to_time_t(now);
        string ts = ctime(&t);
        if (!ts.empty() && ts.back() == '\n') {
            ts.pop_back();
        }

        string line = "[" + ts + "] " + message;
        cout << line << endl;

        ofstream out(logFileName, ios::app);
        out << line << endl;
    }

    void readSeedConfig(const string& configFile) {
        ifstream file(configFile);
        if (!file.is_open()) {
            cerr << "Error reading seed config file: " << configFile << endl;
            return;
        }

        string line;
        while (getline(file, line)) {
            if (line.empty() || line[0] == '#') {
                continue;
            }

            size_t colonPos = line.find(':');
            if (colonPos == string::npos) {
                continue;
            }

            string ip = line.substr(0, colonPos);
            int port = stoi(line.substr(colonPos + 1));
            seedNodes.push_back({ip, port});
            totalSeeds++;
        }

        requiredSeeds = (totalSeeds / 2) + 1;
        logMessage("Read " + to_string(totalSeeds) + " seed nodes. Need " + to_string(requiredSeeds) + " for quorum");
    }

    void initializeServer() {
        serverSocket = socket(AF_INET, SOCK_STREAM, 0);
        if (serverSocket < 0) {
            cerr << "Error creating peer server socket" << endl;
            return;
        }

        int opt = 1;
        setsockopt(serverSocket, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

        sockaddr_in address{};
        address.sin_family = AF_INET;
        address.sin_addr.s_addr = inet_addr(selfIp.c_str());
        address.sin_port = htons(selfPort);

        if (bind(serverSocket, (sockaddr*)&address, sizeof(address)) < 0) {
            cerr << "Error binding peer socket on " << selfIp << ":" << selfPort << endl;
            return;
        }

        listen(serverSocket, 64);
        logMessage("Peer node initialized on " + selfIp + ":" + to_string(selfPort));
    }

    int connectWithTimeout(const string& ip, int port, int timeoutMs) {
        int sock = socket(AF_INET, SOCK_STREAM, 0);
        if (sock < 0) {
            return -1;
        }

        int oldFlags = fcntl(sock, F_GETFL, 0);
        if (oldFlags < 0) {
            close(sock);
            return -1;
        }

        fcntl(sock, F_SETFL, oldFlags | O_NONBLOCK);

        sockaddr_in address{};
        address.sin_family = AF_INET;
        address.sin_port = htons(port);

        if (inet_pton(AF_INET, ip.c_str(), &address.sin_addr) <= 0) {
            close(sock);
            return -1;
        }

        int result = connect(sock, (sockaddr*)&address, sizeof(address));
        if (result == 0) {
            fcntl(sock, F_SETFL, oldFlags);
            return sock;
        }

        if (errno != EINPROGRESS) {
            close(sock);
            return -1;
        }

        fd_set wfds;
        FD_ZERO(&wfds);
        FD_SET(sock, &wfds);

        timeval tv{};
        tv.tv_sec = timeoutMs / 1000;
        tv.tv_usec = (timeoutMs % 1000) * 1000;

        result = select(sock + 1, nullptr, &wfds, nullptr, &tv);
        if (result <= 0) {
            close(sock);
            return -1;
        }

        int soError = 0;
        socklen_t len = sizeof(soError);
        if (getsockopt(sock, SOL_SOCKET, SO_ERROR, &soError, &len) < 0 || soError != 0) {
            close(sock);
            return -1;
        }

        fcntl(sock, F_SETFL, oldFlags);
        return sock;
    }

    bool registerWithSeed(const string& seedIp, int seedPort) {
        int sock = connectWithTimeout(seedIp, seedPort, 1200);
        if (sock < 0) {
            return false;
        }

        string request = "REGISTER " + selfIp + ":" + to_string(selfPort);
        send(sock, request.c_str(), request.size(), 0);

        char buffer[1024] = {0};
        int n = recv(sock, buffer, sizeof(buffer) - 1, 0);
        close(sock);

        if (n <= 0) {
            return false;
        }

        string response(buffer);
        bool ok = (response.find("REGISTERED") != string::npos) || (response.find("DUPLICATE") != string::npos);
        if (ok) {
            logMessage("Successfully registered with seed " + seedIp + ":" + to_string(seedPort));
        }
        return ok;
    }

    bool registerWithSeeds() {
        int success = 0;
        for (const auto& seed : seedNodes) {
            if (registerWithSeed(seed.first, seed.second)) {
                success++;
            }
        }

        logMessage("Registered with " + to_string(success) + " seeds (required: " + to_string(requiredSeeds) + ")");
        return success >= requiredSeeds;
    }

    vector<pair<string, int>> getPeerListFromSeed(const string& seedIp, int seedPort) {
        vector<pair<string, int>> peers;

        int sock = connectWithTimeout(seedIp, seedPort, 1200);
        if (sock < 0) {
            return peers;
        }

        string request = "GETPEERLIST";
        send(sock, request.c_str(), request.size(), 0);

        char buffer[4096] = {0};
        int n = recv(sock, buffer, sizeof(buffer) - 1, 0);
        close(sock);

        if (n <= 0) {
            return peers;
        }

        string response(buffer);
        if (response.rfind("PEERLIST:", 0) != 0) {
            return peers;
        }

        string payload = response.substr(9);
        stringstream ss(payload);
        string entry;
        while (getline(ss, entry, ',')) {
            size_t colon = entry.find(':');
            if (colon == string::npos) {
                continue;
            }
            string ip = entry.substr(0, colon);
            int port = stoi(entry.substr(colon + 1));
            if (!(ip == selfIp && port == selfPort)) {
                peers.push_back({ip, port});
            }
        }

        return peers;
    }

    vector<pair<string, int>> getPeerListFromSeeds() {
        set<pair<string, int>> combined;
        for (const auto& seed : seedNodes) {
            auto list = getPeerListFromSeed(seed.first, seed.second);
            for (const auto& p : list) {
                combined.insert(p);
            }
        }

        vector<pair<string, int>> result(combined.begin(), combined.end());
        logMessage("Obtained " + to_string(result.size()) + " peers from seeds");
        return result;
    }

    bool hasNeighborUnlocked(const string& ip, int port) {
        for (const auto& n : neighbors) {
            if (n.ip == ip && n.port == port) {
                return true;
            }
        }
        return false;
    }

    string buildNeighborSummaryUnlocked() {
        if (neighbors.empty()) {
            return "none";
        }

        string out;
        for (size_t i = 0; i < neighbors.size(); i++) {
            out += neighbors[i].ip + ":" + to_string(neighbors[i].port);
            if (i + 1 < neighbors.size()) {
                out += ", ";
            }
        }
        return out;
    }

     void reconcileNeighbors(const vector<pair<string, int>>& peerList) {
        if (peerList.empty()) {
            logMessage("Neighbor refresh: no peers available yet");
            return;
        }

        set<pair<string, int>> activePeers(peerList.begin(), peerList.end());

        int target = 0;
        if ((int)peerList.size() <= 6) {
            target = (int)peerList.size();
        } else {
            target = max(3, (int)peerList.size() / 3);
        }

        vector<pair<string, int>> newlyAdded;
        {
            lock_guard<mutex> lock(neighborMutex);

            neighbors.erase(
                remove_if(neighbors.begin(), neighbors.end(), [&](const PeerConnection& n) {
                    return activePeers.find({n.ip, n.port}) == activePeers.end();
                }),
                neighbors.end());

            int neighborsNeeded = target - (int)neighbors.size();

            if (neighborsNeeded > 0) {
                vector<pair<string, int>> candidates = peerList;
                sort(candidates.begin(), candidates.end());

                random_device rd;
                mt19937 gen(rd());
                uniform_real_distribution<> dis(0.0, 1.0);

                vector<pair<string, int>> selected;
                for (size_t i = 0; i < candidates.size() && (int)selected.size() < neighborsNeeded; i++) {
                    if (hasNeighborUnlocked(candidates[i].first, candidates[i].second) ||
                        (candidates[i].first == selfIp && candidates[i].second == selfPort)) {
                        continue;
                    }

                    double probability = 1.0 / (i + 1);
                    double roll = dis(gen);

                    if (roll <= probability) {
                        selected.push_back(candidates[i]);
                    }
                }

                for (size_t i = 0; i < candidates.size() && (int)selected.size() < neighborsNeeded; i++) {
                    if (!hasNeighborUnlocked(candidates[i].first, candidates[i].second) &&
                        !(candidates[i].first == selfIp && candidates[i].second == selfPort)) {
                        
                        bool alreadySelected = false;
                        for(auto& s : selected) {
                            if(s == candidates[i]) alreadySelected = true;
                        }
                        
                        if(!alreadySelected) {
                            selected.push_back(candidates[i]);
                        }
                    }
                }

                for (const auto& p : selected) {
                    neighbors.push_back({p.first, p.second, false, 0, false});
                    newlyAdded.push_back(p);
                }
            }
            logMessage("Neighbor set now: " + buildNeighborSummaryUnlocked());
        }

        for (const auto& p : newlyAdded) {
            int sock = connectWithTimeout(p.first, p.second, 1000);
            bool ok = (sock >= 0);
            if (sock >= 0) {
                close(sock);
            }

            lock_guard<mutex> lock(neighborMutex);
            for (auto& n : neighbors) {
                if (n.ip == p.first && n.port == p.second) {
                    n.isConnected = ok;
                    break;
                }
            }

            logMessage(string("Connected to neighbor ") + p.first + ":" + to_string(p.second) +
                       (ok ? "" : " (pending/failed initial connect)"));
        }
    }

    void connectToNeighbors(const vector<pair<string, int>>& peerList) {
        reconcileNeighbors(peerList);
        lock_guard<mutex> lock(neighborMutex);
        logMessage("Selected " + to_string(neighbors.size()) + " neighbors for connection");
    }

    void periodicNeighborRefresh() {
        while (true) {
            this_thread::sleep_for(chrono::seconds(8));
            auto latestPeers = getPeerListFromSeeds();
            reconcileNeighbors(latestPeers);
        }
    }

    string generateMessageHash(const string& content) {
        unsigned int hash = 0;
        for (char c : content) {
            hash = ((hash << 5) + hash) + c;
        }
        return to_string(hash);
    }

    string nodeKey(const string& ip, int port) {
        return ip + ":" + to_string(port);
    }

    int requiredPeerConsensus(const string& deadIp, int deadPort) {
        lock_guard<mutex> lock(neighborMutex);
        int otherWitnesses = 0;
        for (const auto& n : neighbors) {
            if (n.ip == deadIp && n.port == deadPort) {
                continue;
            }
            otherWitnesses++;
        }

        int totalVoters = otherWitnesses + 1;
        int threshold = (totalVoters / 2) + 1;
        return max(2, threshold);
    }

    void sendSuspicionToNeighbors(const string& deadIp, int deadPort) {
        lock_guard<mutex> lock(neighborMutex);

        for (auto& n : neighbors) {
            if (n.ip == deadIp && n.port == deadPort) {
                continue;
            }

            int sock = connectWithTimeout(n.ip, n.port, 1000);
            if (sock < 0) {
                n.isConnected = false;
                continue;
            }

            string wire = "SUSPECT|" + deadIp + "|" + to_string(deadPort) + "|" + selfIp + "|" + to_string(selfPort);
            int sent = send(sock, wire.c_str(), wire.size(), 0);
            if (sent < 0) {
                n.isConnected = false;
            } else {
                n.isConnected = true;
            }
            close(sock);
        }
    }

    void maybeReportAfterConsensus(const string& deadIp, int deadPort) {
        string key = nodeKey(deadIp, deadPort);
        string selfKey = nodeKey(selfIp, selfPort);

        int threshold = requiredPeerConsensus(deadIp, deadPort);
        bool shouldReport = false;

        {
            lock_guard<mutex> lock(suspicionMutex);
            if (selfSuspectedDeadNodes.find(key) == selfSuspectedDeadNodes.end()) {
                return;
            }

            if (reportedDeadNodes.find(key) != reportedDeadNodes.end()) {
                return;
            }

            auto& votes = suspicionVotes[key];
            votes.insert(selfKey);

            if ((int)votes.size() >= threshold) {
                reportedDeadNodes.insert(key);
                shouldReport = true;
            }

            logMessage("SUSPICION: " + key + " votes=" + to_string((int)votes.size()) +
                       " threshold=" + to_string(threshold));
        }

        if (shouldReport) {
            reportDeadNode(deadIp, deadPort);
        }
    }

    void initiateLocalSuspicion(const string& deadIp, int deadPort) {
        string key = nodeKey(deadIp, deadPort);
        string selfKey = nodeKey(selfIp, selfPort);

        {
            lock_guard<mutex> lock(suspicionMutex);
            selfSuspectedDeadNodes.insert(key);
            suspicionVotes[key].insert(selfKey);
        }

        logMessage("SUSPICION INITIATED: " + key + " by " + selfKey);
        sendSuspicionToNeighbors(deadIp, deadPort);
        maybeReportAfterConsensus(deadIp, deadPort);
    }

    void clearDeadSuspicion(const string& ip, int port) {
        string key = nodeKey(ip, port);
        lock_guard<mutex> lock(suspicionMutex);
        suspicionVotes.erase(key);
        selfSuspectedDeadNodes.erase(key);
        reportedDeadNodes.erase(key);
    }

    void broadcastMessage(const string& content, const string& senderIp, int senderPort) {
        lock_guard<mutex> lock(neighborMutex);

        for (auto& n : neighbors) {
            if (n.ip == senderIp && n.port == senderPort) {
                continue;
            }

            int sock = connectWithTimeout(n.ip, n.port, 1000);
            if (sock < 0) {
                n.isConnected = false;
                continue;
            }

            string wire = "GOSSIP|" + content + "|" + selfIp + "|" + to_string(selfPort);
            int sent = send(sock, wire.c_str(), wire.size(), 0);
            if (sent < 0) {
                n.isConnected = false;
            } else {
                n.isConnected = true;
            }
            close(sock);
        }
    }

    void generateAndBroadcastGossip() {
        const int maxMessages = 10;

        for (int i = 0; i < maxMessages; i++) {
            this_thread::sleep_for(chrono::seconds(5));

            auto now = chrono::system_clock::now();
            time_t t = chrono::system_clock::to_time_t(now);

            messageCounter++;
            string content = to_string(t) + ":" + selfIp + ":" + to_string(messageCounter);
            string hash = generateMessageHash(content);

            {
                lock_guard<mutex> lock(messageMutex);
                messageList[hash] = {hash, content, selfIp, now};
            }

            logMessage("GOSSIP: Generated message #" + to_string(messageCounter) + " - " + content);
            broadcastMessage(content, selfIp, selfPort);
        }
    }

    void handleIncomingMessage(int clientSocket) {
        char buffer[2048] = {0};
        int n = recv(clientSocket, buffer, sizeof(buffer) - 1, 0);
        if (n <= 0) {
            close(clientSocket);
            return;
        }

        string wire(buffer);
        close(clientSocket);

        if (wire.rfind("GOSSIP|", 0) != 0) {
            if (wire.rfind("SUSPECT|", 0) != 0) {
                return;
            }

            stringstream ss(wire);
            string cmd, deadIp, deadPortStr, reporterIp, reporterPortStr;
            getline(ss, cmd, '|');
            getline(ss, deadIp, '|');
            getline(ss, deadPortStr, '|');
            getline(ss, reporterIp, '|');
            getline(ss, reporterPortStr, '|');

            int deadPort = -1;
            int reporterPort = -1;
            try {
                deadPort = stoi(deadPortStr);
                reporterPort = stoi(reporterPortStr);
            } catch (...) {
                return;
            }

            string key = nodeKey(deadIp, deadPort);
            string reporterKey = nodeKey(reporterIp, reporterPort);

            {
                lock_guard<mutex> lock(suspicionMutex);
                suspicionVotes[key].insert(reporterKey);
            }

            logMessage("SUSPICION RECEIVED: " + key + " from " + reporterKey);
            maybeReportAfterConsensus(deadIp, deadPort);
            return;
        }

        size_t p1 = wire.find('|');
        size_t p3 = wire.rfind('|');
        if (p1 == string::npos || p3 == string::npos || p1 == p3) {
            return;
        }

        size_t p2 = wire.rfind('|', p3 - 1);
        if (p2 == string::npos || p2 <= p1) {
            return;
        }

        string content = wire.substr(p1 + 1, p2 - p1 - 1);
        string senderIp = wire.substr(p2 + 1, p3 - p2 - 1);

        int senderPort = -1;
        try {
            senderPort = stoi(wire.substr(p3 + 1));
        } catch (...) {
            return;
        }

        string hash = generateMessageHash(content);

        bool firstSeen = false;
        {
            lock_guard<mutex> lock(messageMutex);
            if (messageList.find(hash) == messageList.end()) {
                messageList[hash] = {hash, content, senderIp, chrono::system_clock::now()};
                firstSeen = true;
            }
        }

        if (firstSeen) {
            logMessage("GOSSIP RECEIVED: " + content + " from " + senderIp + ":" + to_string(senderPort));
            broadcastMessage(content, senderIp, senderPort);
        } else {
            logMessage("Duplicate gossip message ignored: " + content);
        }
    }

    void listenForIncomingMessages() {
        while (true) {
            sockaddr_in addr{};
            socklen_t len = sizeof(addr);
            int client = accept(serverSocket, (sockaddr*)&addr, &len);
            if (client < 0) {
                continue;
            }

            thread t(&PeerNode::handleIncomingMessage, this, client);
            t.detach();
        }
    }

    bool checkPeerLiveness(const string& ip, int port) {
        int sock = connectWithTimeout(ip, port, 800);
        if (sock < 0) {
            return false;
        }
        close(sock);
        return true;
    }

    void reportDeadNode(const string& deadIp, int deadPort) {
        auto now = chrono::system_clock::now();
        time_t t = chrono::system_clock::to_time_t(now);

        logMessage("DEAD NODE DETECTED: " + deadIp + ":" + to_string(deadPort));

        for (const auto& seed : seedNodes) {
            int sock = connectWithTimeout(seed.first, seed.second, 1200);
            if (sock < 0) {
                continue;
            }

            string report = "Dead Node:" + deadIp + ":" + to_string(deadPort) + ":" +
                           to_string(t) + ":" + selfIp + ":" + to_string(selfPort);
            send(sock, report.c_str(), report.size(), 0);
            close(sock);

            logMessage("Sent dead node report for " + deadIp + ":" + to_string(deadPort) +
                       " to seed " + seed.first + ":" + to_string(seed.second));
        }
    }

    void periodicLivenessCheck() {
        while (true) {
            this_thread::sleep_for(chrono::seconds(3));

            vector<pair<string, int>> endpoints;
            {
                lock_guard<mutex> lock(neighborMutex);
                for (const auto& n : neighbors) {
                    endpoints.push_back({n.ip, n.port});
                }
            }

            for (const auto& ep : endpoints) {
                bool alive = checkPeerLiveness(ep.first, ep.second);
                bool shouldReport = false;

                {
                    lock_guard<mutex> lock(neighborMutex);
                    for (auto& n : neighbors) {
                        if (n.ip == ep.first && n.port == ep.second) {
                            if (!alive) {
                                n.failureCount++;
                                n.isConnected = false;
                                if (n.failureCount >= 2 && !n.deadReported) {
                                    n.deadReported = true;
                                    shouldReport = true;
                                }
                            } else {
                                n.failureCount = 0;
                                n.deadReported = false;
                                n.isConnected = true;
                            }
                            break;
                        }
                    }
                }

                if (shouldReport) {
                    initiateLocalSuspicion(ep.first, ep.second);
                }

                if (alive) {
                    clearDeadSuspicion(ep.first, ep.second);
                }
            }
        }
    }

    void start() {
        logMessage("Starting peer node...");

        if (!registerWithSeeds()) {
            logMessage("ERROR: Could not register with enough seeds. Exiting.");
            return;
        }

        auto initialPeerList = getPeerListFromSeeds();
        connectToNeighbors(initialPeerList);

        this_thread::sleep_for(chrono::seconds(2));
        {
            lock_guard<mutex> lock(neighborMutex);
            logMessage("Network formed successfully with " + to_string(neighbors.size()) + " neighbors");
        }

        threadPool.emplace_back(&PeerNode::listenForIncomingMessages, this);
        threadPool.emplace_back(&PeerNode::periodicLivenessCheck, this);
        threadPool.emplace_back(&PeerNode::periodicNeighborRefresh, this);
        threadPool.emplace_back(&PeerNode::generateAndBroadcastGossip, this);

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
