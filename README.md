# Gossip-Based P2P Network with Consensus-Driven Membership Management

## Overview

This project implements a peer-to-peer gossip network with the following implemented features:

- **Quorum-based join decision at peer side**: A peer proceeds only after successful registration with majority of configured seeds
- **Two-stage dead-node removal**: Peer-level suspicion/report threshold + seed-level quorum votes
- **Gossip protocol**: Messages propagate through the network with duplicate prevention
- **Dynamic randomized topology**: Neighbors are periodically refreshed from current seed membership
- **Dual-level consensus**: Both peer-level and seed-level agreement for critical decisions

## Architecture

### Seed Nodes
- Maintain a peer list (PL) of known peers
- Accept peer registrations and handle duplicate entries
- Receive dead-node reports and validate before removal
- Serve peer lists to joining peers
- Exchange removal votes with other seeds before final dead-node removal

### Peer Nodes
- Register with at least ⌈n/2⌉ + 1 seed nodes
- Connect to randomly selected neighbors for overlay formation and periodic refresh
- Generate and broadcast gossip messages every 5 seconds (max 10 messages)
- Maintain a message list to prevent infinite loops
- Periodically check neighbor liveness using TCP connect checks
- Report dead nodes only after peer-level consensus
- Listen for incoming gossip messages from neighbors

## Files

### Source Code
- `seed.cpp` - Seed node implementation
- `peer.cpp` - Peer node implementation

### Configuration
- `config.txt` - Contains seed node IP:port pairs

### Output
- `seed_output_<port>.txt` - Seed node logs
- `peer_<ip>_<port>.txt` - Peer node logs

## Compilation

### Prerequisites
- GCC/G++ compiler (C++11 or later)
- Standard Linux development tools
- POSIX-compliant system (Linux, macOS, or WSL on Windows)

### Build Instructions

```bash
# Navigate to the project directory
cd B23CS1022-B23CS1094

# Compile seed node
g++ -std=c++11 -pthread -o seed seed.cpp

# Compile peer node
g++ -std=c++11 -pthread -o peer peer.cpp
```

## Running the System

### Step 1: Start Seed Nodes

Open multiple terminal windows and start seed nodes on different ports:

```bash
# Terminal 1 - Seed node on port 5001
./seed 5001

# Terminal 2 - Seed node on port 5002
./seed 5002

# Terminal 3 - Seed node on port 5003
./seed 5003
```

**Important**: Make sure the ports in your `config.txt` match the ports you use here. By default, the config file uses ports 5001, 5002, and 5003.

### Step 2: Update Configuration File

Edit `config.txt` to match your seed node setup:

```
127.0.0.1:5001
127.0.0.1:5002
127.0.0.1:5003
```

For multi-machine deployment, replace `127.0.0.1` with the actual IP addresses of your seed nodes.

### Step 3: Start Peer Nodes

Open new terminal windows and start peer nodes:

```bash
# Terminal 4 - Peer node on 127.0.0.1:6001
./peer 127.0.0.1 6001 config.txt

# Terminal 5 - Peer node on 127.0.0.1:6002
./peer 127.0.0.1 6002 config.txt

# Terminal 6 - Peer node on 127.0.0.1:6003
./peer 127.0.0.1 6003 config.txt

# Terminal 7 - Peer node on 127.0.0.1:6004
./peer 127.0.0.1 6004 config.txt

# Terminal 8 - Peer node on 127.0.0.1:6005
./peer 127.0.0.1 6005 config.txt
```

You can start as many peer nodes as desired. Each peer will:
1. Register with the seed nodes
2. Obtain the peer list
3. Connect to random neighbors
4. Start broadcasting gossip messages
5. Monitor neighbor liveness

## Protocol Details

### Message Formats

#### Registration
```
Client → Seed: REGISTER <IP>:<Port>
Seed → Client: REGISTERED or DUPLICATE
```

#### Peer List Request
```
Client → Seed: GETPEERLIST
Seed → Client: PEERLIST:<IP1>:<Port1>,<IP2>:<Port2>,...
```

#### Gossip Message (wire format)
```
Peer → Neighbor: GOSSIP|<timestamp>:<originPeerIP>:<msgNum>|<forwarderIP>|<forwarderPort>
```

#### Suspicion Message (wire format)
```
Peer → Neighbor: SUSPECT|<deadIp>|<deadPort>|<reporterIp>|<reporterPort>
```

#### Dead Node Report
```
Peer → Seed: Dead Node:<deadIp>:<deadPort>:<timestamp>:<reporterIp>:<reporterPort>
```

#### Seed Vote Exchange
```
Seed → Seed: SEEDVOTE_REMOVE <deadIp>:<deadPort> <voterSeedPort>
```

## Logging

### Seed Node Logs
- Peer registration and duplicate handling
- Peer-list responses
- Removal report vote counts
- Seed-to-seed vote receipts
- Quorum-based dead-node removals

### Peer Node Logs
- Successful registrations with seeds
- Received peer lists
- Generated gossip messages
- First-time gossip message receptions
- Duplicate gossip suppression
- Suspicion vote progress
- Dead node detections
- Reports sent to seeds

Logs are written to both console and output files for easy debugging.

## Key Features

### 1. Quorum-Based Consensus
- Peer startup requires successful registration with majority of configured seeds
- Dead node removal at seeds requires:
   - majority independent peer reports (active peers/2 + 1 report-vote threshold), then
   - seed quorum votes (majority of total seeds)

### 2. Gossip Protocol
- Message format: `<timestamp>:<IP>:<MsgNum>`
- Wire format used for forwarding: `GOSSIP|<content>|<senderIp>|<senderPort>`
- Automatic loop prevention via message list
- 5-second intervals between broadcasts
- Maximum 10 messages per peer

### 3. Liveness Detection
- Peer-level checking via TCP connection attempts
- Suspicion vote collection from peers before reporting
- Seed-level validation before removal
- Helps reduce false positives via thresholding

### 4. Network Topology
- Random neighbor selection from seed-union peer list
- Periodic neighbor refresh (every 8 seconds)
- Stale neighbor pruning when peers disappear from seed lists

## Testing Tips

### Local Testing
1. Start 3 seed nodes on localhost with different ports
2. Start 5-10 peer nodes on different ports
3. Verify logs for successful registrations and gossip messages
4. Kill a peer node and verify: suspicion votes -> dead-node reports -> seed votes -> quorum removal

### Multi-Machine Testing
1. Update `config.txt` with actual seed node IPs
2. Deploy seed nodes on different machines
3. Deploy peer nodes on various machines
4. Verify network connectivity and message propagation

### Expected Behavior

#### Successful Startup
```
[Timestamp] Seed node initialized on port 5001
[Timestamp] Peer node initialized on 127.0.0.1:6001
[Timestamp] Successfully registered with seed 127.0.0.1:5001
[Timestamp] Network formed successfully with 3 neighbors
[Timestamp] GOSSIP: Generated message #1 - 1234567890:127.0.0.1:1
```

#### Gossip Propagation
```
[Timestamp] GOSSIP RECEIVED: 1234567890:127.0.0.1:1 from 127.0.0.1
[Timestamp] Duplicate gossip message ignored: 1234567890:127.0.0.1:1
```

#### Dead Node Detection
```
[Timestamp] DEAD NODE DETECTED: 127.0.0.1:6002
[Timestamp] Sent dead node report to seed 127.0.0.1:5001
```

## Performance Considerations

- **Message overhead**: Gossip messages are bounded to 10 per peer
- **Network bandwidth**: Controlled by 5-second intervals
- **Liveness check interval**: 3 seconds
- **Neighbor refresh interval**: 8 seconds

## Security Notes

### Attack Mitigations

1. **Single-point failure reduction**
   - Peer requires majority seed responses before joining
   - Seed quorum required before final dead-node removal

2. **False death report resistance**
   - Peer-level consensus before reporting
   - Seed-level consensus before removal
   - Multiple independent confirmations

3. **Message replay/loop resistance**
   - Sender IP included in all messages
   - Duplicate detection via message list
   - First-time message reception logged

4. **Two-level thresholding**
   - Peer suspicion threshold + seed quorum threshold

## Group members

   - Japneet Singh (B23CS1022)
   - Naman Soni (B23CS1094)
