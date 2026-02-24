# Gossip-Based P2P Network with Consensus-Driven Membership Management

## Overview

This project implements a sophisticated peer-to-peer network system with the following key features:

- **Consensus-based membership management**: New peers must be registered with a quorum (majority) of seed nodes
- **Robust liveness detection**: Dead nodes are confirmed by multiple peers before reports are sent to seeds
- **Gossip protocol**: Messages propagate through the network with duplicate prevention
- **Power-law topology**: Network maintains a realistic degree distribution
- **Dual-level consensus**: Both peer-level and seed-level agreement for critical decisions

## Architecture

### Seed Nodes
- Maintain a peer list (PL) of known peers
- Accept peer registrations and validate them via consensus
- Receive dead-node reports and validate before removal
- Serve peer lists to joining peers
- Help maintain network topology with power-law distribution

### Peer Nodes
- Register with at least ⌈n/2⌉ + 1 seed nodes
- Connect to randomly selected neighbors for overlay formation
- Generate and broadcast gossip messages every 5 seconds (max 10 messages)
- Maintain a message list to prevent infinite loops
- Periodically check neighbor liveness using ping
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
cd cn_assignment1

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

#### Gossip Message
```
Peer → Neighbor: GOSSIP:<timestamp>:<IP>:<MsgNum>:<SenderIP>
```

#### Dead Node Report
```
Peer → Seed: DEADNODE <DeadIP>:<DeadPort> <timestamp> <ReporterIP>
```

## Logging

### Seed Node Logs
- Peer registration proposals and outcomes
- Consensus decisions
- Confirmed dead-node removals
- Peer list updates

### Peer Node Logs
- Successful registrations with seeds
- Received peer lists
- Generated gossip messages
- First-time gossip message receptions
- Dead node detections
- Reports sent to seeds

Logs are written to both console and output files for easy debugging.

## Key Features

### 1. Quorum-Based Consensus
- Peers register with ⌈n/2⌉ + 1 seeds (n = total seeds)
- Dead node removal requires consensus among seeds
- Prevents malicious unilateral decisions

### 2. Gossip Protocol
- Message format: `<timestamp>:<IP>:<MsgNum>`
- Automatic loop prevention via message list
- 5-second intervals between broadcasts
- Maximum 10 messages per peer

### 3. Liveness Detection
- Peer-level checking via system ping
- Consensus among neighbors before reporting
- Seed-level validation before removal
- Prevents false positives and Sybil attacks

### 4. Network Topology
- Power-law degree distribution
- Dynamic neighbor selection
- Automatic overlay formation

## Testing Tips

### Local Testing
1. Start 3 seed nodes on localhost with different ports
2. Start 5-10 peer nodes on different ports
3. Verify logs for successful registrations and gossip messages
4. Kill a peer node and verify dead node detection

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
[Timestamp] Message forwarded to 3 neighbors
```

#### Dead Node Detection
```
[Timestamp] DEAD NODE DETECTED: 127.0.0.1:6002
[Timestamp] Sent dead node report to seed 127.0.0.1:5001
```

## Performance Considerations

- **Message overhead**: Gossip messages are bounded to 10 per peer
- **Network bandwidth**: Controlled by 5-second intervals
- **Liveness check interval**: 3 seconds (configurable)
- **Thread pool**: One thread per client connection + background threads

## Security Analysis

### Attack Mitigations

1. **Sybil Attack Prevention**
   - Quorum consensus required for registration
   - Multiple seed nodes validate identities

2. **False Death Reports**
   - Peer-level consensus before reporting
   - Seed-level consensus before removal
   - Multiple independent confirmations

3. **Message Spoofing**
   - Sender IP included in all messages
   - Duplicate detection via message list
   - First-time message reception logged

4. **Collusion**
   - Threshold consensus (⌈n/2⌉ + 1) prevents majority compromise
   - Two-level consensus prevents concentration of power

## Known Limitations

1. No encryption (plaintext communication)
2. No authentication beyond IP validation
3. No Byzantine fault tolerance
4. Ping-based liveness may have false positives on congested networks
5. Power-law distribution maintained heuristically

## Future Improvements

1. TLS/SSL encryption for secure communication
2. Digital signatures for message authentication
3. Byzantine fault tolerance algorithms
4. More sophisticated topology management
5. Adaptive liveness check intervals
6. Message prioritization
7. NAT traversal support

## Troubleshooting

### Port Already in Use
```bash
# Find process using the port
lsof -i :5001

# Kill the process
kill -9 <PID>
```

### Connection Refused
- Verify seed nodes are running on specified ports
- Check firewall settings
- Ensure IP addresses in config.txt are correct

### No Gossip Messages
- Verify peers have connected to each other
- Check neighbor connection logs
- Ensure TCP connections are established

### Dead Nodes Not Detected
- Verify system has `ping` command
- Check network connectivity
- Adjust failure threshold if needed

## References

- Socket Programming: https://beej.us/guide/bgnet/html/split/
- Gossip Protocols: https://en.wikipedia.org/wiki/Gossip_protocol
- Consensus Algorithms: https://en.wikipedia.org/wiki/Consensus_(computer_science)
- Power-law Networks: https://en.wikipedia.org/wiki/Scale-free_network

## Submission

All files are packaged as follows:
- `seed.cpp` - Seed node source
- `peer.cpp` - Peer node source
- `config.txt` - Configuration file
- `README.md` - This file
- `seed_output_*.txt` - Seed logs (generated at runtime)
- `peer_*.txt` - Peer logs (generated at runtime)

Package as `rollno1-rollno2.tar.gz` for submission.

## Authors

Group members: [Your names here]

## License

This project is submitted as part of CSL3080 - Computer Networks course.
