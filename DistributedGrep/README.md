# Distributed Grep

A distributed log querying system that performs parallel grep operations across multiple servers. The client sends grep queries to all configured servers simultaneously, aggregates results, and displays them with statistics.

## Architecture

```
┌─────────┐       ┌──────────┐
│         │──────▶│ Server 1 │──▶ machine.1.log   (port 3491)
│         │       └──────────┘
│         │       ┌──────────┐
│         │──────▶│ Server 2 │──▶ machine.2.log   (port 3492)
│         │       └──────────┘
│ Client  │       ┌──────────┐
│         │──────▶│   ...    │
│         │       └──────────┘
│         │       ┌──────────┐
│         │──────▶│Server 10 │──▶ machine.10.log  (port 3500)
└─────────┘       └──────────┘
```

**Client (`client.py`):**
- Reads server configuration from `config.json`
- Sends grep queries to all 10 servers in parallel using thread pools
- Collects and aggregates results
- Displays matching lines and statistics

**Server (`server.py`):**
- Listens for incoming grep requests on a configured port
- Executes grep on the local log file
- Returns matching lines and count to the client

## File Structure

```
DistributedGrep/
├── client.py           # Client that queries all servers
├── server.py           # Server that handles grep requests
├── config.json         # Configuration for 10 servers
├── start_servers.py    # Helper script to start all servers
├── generate_logs.py    # Helper script to generate test logs
├── logs/               # Directory for log files
│   ├── machine.1.log
│   ├── machine.2.log
│   └── ...
│   └── machine.10.log
└── README.md
```

## Configuration

The `config.json` file defines 10 servers (ports 3491-3500):

```json
{
    "vms": [
        {"id": 1, "hostname": "localhost", "ip": "127.0.0.1", "port": 3491, "log_file": "logs/machine.1.log"},
        {"id": 2, "hostname": "localhost", "ip": "127.0.0.1", "port": 3492, "log_file": "logs/machine.2.log"},
        ...
        {"id": 10, "hostname": "localhost", "ip": "127.0.0.1", "port": 3500, "log_file": "logs/machine.10.log"}
    ]
}
```

Each server entry contains:
- `id`: Unique identifier for the server (1-10)
- `hostname`: Hostname (used for distributed deployments)
- `ip`: IP address to connect to
- `port`: Port number the server listens on (3491-3500)
- `log_file`: Path to the log file to search

## Quick Start (Local Testing)

### Option 1: Using Helper Scripts

```bash
# 1. Generate test log files
python generate_logs.py

# 2. Start all 10 servers (runs in background)
python start_servers.py

# 3. Run queries
python client.py "ERROR"

# 4. Stop all servers when done
python start_servers.py --stop
```

### Option 2: Manual Setup

#### 1. Generate Test Log Files

```bash
python generate_logs.py
```

Or manually:
```bash
mkdir -p logs
for i in {1..10}; do
    echo -e "INFO: Server $i started\nERROR: Connection failed on server $i\nWARN: High latency\nINFO: Request processed" > logs/machine.$i.log
done
```

#### 2. Start the Servers

Open 10 separate terminals and run:

```bash
# Terminal 1
python server.py --vm-id 1

# Terminal 2
python server.py --vm-id 2

# ... continue for all 10 servers

# Terminal 10
python server.py --vm-id 10
```

Each server will print:
```
Server VM1 listening on port 3491
Log file: logs/machine.1.log
```

#### 3. Run Grep Queries

In another terminal:

```bash
# Search for ERROR in all logs
python client.py "ERROR"

# Case-insensitive search
python client.py -i "error"

# Show line numbers
python client.py -n "INFO"

# Use extended regex
python client.py -e "ERROR|WARN"

# Verbose output (shows per-server status)
python client.py -v "Connection"
```

## Client Usage

```
python client.py [OPTIONS] PATTERN

Options:
  -e, --regexp        Use extended regular expressions
  -i, --ignore-case   Ignore case distinctions
  -c, --count         Only print count of matching lines
  -n, --line-number   Output with line numbers
  -v, --verbose       Verbose output (show per-server status)
  --options OPTIONS   Additional grep options
```

## Example Output

```
Executing grep: pattern='ERROR' options=''
------------------------------------------------------------

logs/machine.1.log:ERROR: Connection failed on server 1
logs/machine.2.log:ERROR: Connection failed on server 2
logs/machine.3.log:ERROR: Connection failed on server 3
...
logs/machine.10.log:ERROR: Connection failed on server 10

Line counts:
machine.1.log:       1
machine.2.log:       1
machine.3.log:       1
machine.4.log:       1
machine.5.log:       1
machine.6.log:       1
machine.7.log:       1
machine.8.log:       1
machine.9.log:       1
machine.10.log:      1

Sum of line counts: 10
Successful VMs: 10/10

Query completed in 0.025 seconds
```

## Distributed Deployment

For deployment across multiple physical/virtual machines:

1. Update `config.json` with actual hostnames/IPs:

```json
{
    "vms": [
        {
            "id": 1,
            "hostname": "server1.example.com",
            "ip": "192.168.1.101",
            "port": 3490,
            "log_file": "/var/log/app/server.log"
        },
        {
            "id": 2,
            "hostname": "server2.example.com",
            "ip": "192.168.1.102",
            "port": 3490,
            "log_file": "/var/log/app/server.log"
        }
    ]
}
```

2. Copy `server.py` and `config.json` to each server

3. Start the server on each machine:
```bash
python server.py --vm-id <ID>
```

4. Run queries from any machine with `client.py`

## Protocol

The client-server communication uses a simple JSON-based protocol over TCP:

**Request (Client → Server):**
```json
{
    "pattern": "ERROR",
    "options": "-i -n"
}
```

**Response (Server → Client):**
- 10-byte size header (zero-padded integer)
- JSON payload:
```json
{
    "vm_id": 1,
    "count": 5,
    "lines": ["file.log:ERROR: message", ...],
    "error": null
}
```

## Requirements

- Python 3.6+
- No external dependencies (uses only standard library)

## Credits

Network programming concepts adapted from [Beej's Guide to Network Programming](https://beej.us/guide/).
