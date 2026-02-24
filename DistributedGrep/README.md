# Distributed Grep

A distributed log querying system that performs parallel grep operations across multiple servers. The client sends grep queries to all configured servers simultaneously, aggregates results, and displays them with statistics.

## Architecture

```
┌─────────┐       ┌──────────┐
│         │──────▶│ Server 1 │──▶ machine.1.log
│         │       └──────────┘
│         │       ┌──────────┐
│ Client  │──────▶│ Server 2 │──▶ machine.2.log
│         │       └──────────┘
│         │       ┌──────────┐
│         │──────▶│ Server 3 │──▶ machine.3.log
└─────────┘       └──────────┘
```

**Client (`client.py`):**
- Reads server configuration from `config.json`
- Sends grep queries to all servers in parallel using thread pools
- Collects and aggregates results
- Displays matching lines and statistics

**Server (`server.py`):**
- Listens for incoming grep requests on a configured port
- Executes grep on the local log file
- Returns matching lines and count to the client

## File Structure

```
DistributedGrep/
├── client.py       # Client that queries all servers
├── server.py       # Server that handles grep requests
├── config.json     # Configuration for servers and log files
├── logs/           # Directory for log files (create for testing)
│   ├── machine.1.log
│   ├── machine.2.log
│   └── machine.3.log
└── README.md
```

## Configuration

The `config.json` file defines the servers to query:

```json
{
    "vms": [
        {
            "id": 1,
            "hostname": "localhost",
            "ip": "127.0.0.1",
            "port": 3491,
            "log_file": "logs/machine.1.log"
        },
        {
            "id": 2,
            "hostname": "localhost",
            "ip": "127.0.0.1",
            "port": 3492,
            "log_file": "logs/machine.2.log"
        },
        {
            "id": 3,
            "hostname": "localhost",
            "ip": "127.0.0.1",
            "port": 3493,
            "log_file": "logs/machine.3.log"
        }
    ]
}
```

Each server entry contains:
- `id`: Unique identifier for the server
- `hostname`: Hostname (used for distributed deployments)
- `ip`: IP address to connect to
- `port`: Port number the server listens on
- `log_file`: Path to the log file to search

## Local Testing

### 1. Create Test Log Files

```bash
mkdir -p logs

echo -e "INFO: Server started\nERROR: Connection failed\nINFO: Request received\nWARN: High latency detected" > logs/machine.1.log

echo -e "INFO: Database connected\nERROR: Query timeout\nINFO: Cache hit\nERROR: Disk full" > logs/machine.2.log

echo -e "INFO: User logged in\nWARN: Invalid token\nINFO: Session created\nERROR: Authentication failed" > logs/machine.3.log
```

### 2. Start the Servers

Open three separate terminals and run:

**Terminal 1:**
```bash
python server.py --vm-id 1
```

**Terminal 2:**
```bash
python server.py --vm-id 2
```

**Terminal 3:**
```bash
python server.py --vm-id 3
```

Each server will print:
```
Server VM1 listening on port 3491
Log file: logs/machine.1.log
```

### 3. Run Grep Queries

In a fourth terminal, run queries:

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

logs/machine.1.log:ERROR: Connection failed
logs/machine.2.log:ERROR: Query timeout
logs/machine.2.log:ERROR: Disk full
logs/machine.3.log:ERROR: Authentication failed

Line counts:
machine.1.log:      1
machine.2.log:      2
machine.3.log:      1

Sum of line counts: 4
Successful VMs: 3/3

Query completed in 0.015 seconds
```

## Distributed Deployment

For deployment across multiple machines:

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
