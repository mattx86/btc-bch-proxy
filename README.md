# BTC/BCH Stratum Proxy

A cross-platform Bitcoin/Bitcoin Cash stratum proxy with time-based server routing. Automatically switches between mining pools based on a configurable daily schedule.

## Features

- **Time-based routing**: Automatically switch between stratum servers on a schedule
- **Multiple miners**: Support for concurrent miner connections
- **Failover**: Configurable retry period (default 20 min) before failing over to backup server
- **Graceful switching**: Waits for pending share submissions before switching servers
- **Cross-platform**: Runs on Windows and Linux
- **Background mode**: Run as a daemon/background service
- **Config validation**: Detects overlapping timeframes and invalid configurations at startup
- **Statistics**: Tracks accepts/rejects per pool with periodic logging (every 15 min)
- **Version-rolling**: Supports ASICBoost/version-rolling negotiation with pools
- **Share validation**: Rejects duplicate shares and stale jobs before forwarding to pool
- **Loguru logging**: Structured logging with file rotation

## Installation

### Quick Start (Recommended)

```bash
git clone https://github.com/yourusername/btc-bch-proxy.git
cd btc-bch-proxy

# Windows
init.bat

# Linux/Mac
chmod +x *.sh
./init.sh
```

This creates a virtual environment, installs dependencies, and generates `config.yaml`.

### Manual Installation

```bash
git clone https://github.com/yourusername/btc-bch-proxy.git
cd btc-bch-proxy

# Create venv and install
python -m venv venv
venv\Scripts\activate     # Windows
source venv/bin/activate  # Linux/Mac

pip install -e .
btc-bch-proxy init --no-venv
```

### Dependencies

- Python 3.10+
- pydantic
- pyyaml
- loguru
- click

## Quick Start

1. **Initialize the project:**

   ```bash
   # Windows
   init.bat

   # Linux/Mac
   ./init.sh
   ```

   This creates a virtual environment, installs dependencies, and generates `config.yaml`.

2. **Edit the configuration** with your pool details:

   ```yaml
   proxy:
     bind_host: "0.0.0.0"
     bind_port: 3333

   servers:
     - name: "pool1"
       host: "stratum.pool1.com"
       port: 3333
       username: "your_wallet.worker1"
       password: "x"

     - name: "pool2"
       host: "stratum.pool2.com"
       port: 3333
       username: "your_wallet.worker2"
       password: "x"

   schedule:
     - start: "00:00"
       end: "12:00"
       server: "pool1"

     - start: "12:00"
       end: "24:00"
       server: "pool2"
   ```

3. **Validate the configuration:**

   ```bash
   btc-bch-proxy validate config.yaml
   ```

4. **Start the proxy:**

   ```bash
   # Windows
   start.bat              # Run in background
   start.bat -f           # Run in foreground (for debugging)

   # Linux/Mac
   ./start.sh             # Run in background
   ./start.sh -f          # Run in foreground (for debugging)
   ```

5. **Stop the proxy:**

   ```bash
   # Windows
   stop.bat

   # Linux/Mac
   ./stop.sh
   ```

6. **Point your miners** to the proxy address (e.g., `stratum+tcp://192.168.1.100:3333`)

## Helper Scripts

| Script | Description |
|--------|-------------|
| `init.bat` / `init.sh` | Initialize: create venv, install deps, create config |
| `start.bat` / `start.sh` | Start the proxy (pass `-f` for foreground) |
| `stop.bat` / `stop.sh` | Stop the running proxy |

## CLI Commands

| Command | Description |
|---------|-------------|
| `btc-bch-proxy init` | Initialize project (venv + config) |
| `btc-bch-proxy start` | Start the proxy |
| `btc-bch-proxy stop` | Stop the running proxy |
| `btc-bch-proxy status` | Check if proxy is running |
| `btc-bch-proxy validate <config>` | Validate a configuration file |

### Init Options

```bash
btc-bch-proxy init [OPTIONS]

Options:
  --no-venv              Skip virtual environment creation
  --venv-path PATH       Path for virtual environment (default: ./venv)
```

### Start Options

```bash
btc-bch-proxy start [OPTIONS]

Options:
  -c, --config PATH      Path to configuration file
  -f, --foreground       Run in foreground (don't daemonize)
  --log-level LEVEL      Override log level (DEBUG, INFO, WARNING, ERROR)
```

## Configuration Reference

### Proxy Settings

```yaml
proxy:
  bind_host: "0.0.0.0"        # Address to listen on
  bind_port: 3333             # Port to listen on
  max_connections: 100        # Maximum concurrent miner connections
  connection_timeout: 60      # Miner connection timeout (seconds)
  miner_read_timeout: 600     # Miner read timeout (seconds, default 10 min)
  send_timeout: 30            # Send to miner timeout (seconds)
  pending_shares_timeout: 10  # Wait for pending shares during switch (seconds)
```

### Server Settings

```yaml
servers:
  - name: "unique_name"     # Unique identifier for this server
    host: "pool.example.com"
    port: 3333
    username: "wallet.worker"
    password: "x"
    ssl: false              # Use SSL/TLS connection
    timeout: 30             # Connection timeout (seconds)
    retry_interval: 5       # Seconds between reconnection attempts
    max_retries: 240        # Max reconnection attempts (240 * 5s = 20 min)
```

### Schedule Settings

```yaml
schedule:
  - start: "00:00"          # Start time (HH:MM)
    end: "12:00"            # End time (HH:MM, use 24:00 for midnight)
    server: "server_name"   # Server to use during this period
```

**Notes:**
- Times are in 24-hour format (local time)
- Use `24:00` to represent end of day
- Timeframes must not overlap
- All servers referenced in schedule must be defined in `servers`

### Logging Settings

```yaml
logging:
  level: "INFO"             # DEBUG, INFO, WARNING, ERROR
  file: "/var/log/proxy.log"  # Log file path (null for console only)
  rotation: "50 MB"         # Rotate when file reaches this size
  retention: 10             # Keep this many rotated files
  format: "{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {message}"  # Log format
```

### Failover Settings

```yaml
failover:
  retry_timeout_minutes: 20  # Retry primary server for this long before failover
```

### Share Validation Settings

The proxy can validate shares locally before forwarding to the upstream pool, reducing unnecessary network traffic and improving pool acceptance rates.

```yaml
validation:
  reject_duplicates: true     # Reject duplicate share submissions
  reject_stale: true          # Reject shares for expired/unknown jobs
  validate_difficulty: false  # Validate share hash meets difficulty (CPU intensive)
  share_cache_size: 1000      # Max recent shares to track per session
  share_cache_ttl: 300        # Share cache TTL in seconds
  job_cache_size: 10          # Max jobs to track per session
```

**Options:**
- `reject_duplicates`: Prevents submitting the same share twice (reduces "duplicate share" rejections)
- `reject_stale`: Prevents submitting shares for jobs that are no longer valid (reduces "stale job" rejections)
- `validate_difficulty`: Validates that the share hash actually meets the difficulty target before submitting (CPU intensive, disabled by default)

## How It Works

```
┌─────────┐     ┌─────────────────┐     ┌─────────────────┐
│  Miner  │────▶│  Stratum Proxy  │────▶│  Pool 1 (Day)   │
│  Miner  │────▶│                 │     └─────────────────┘
│  Miner  │────▶│  Time Router    │────▶┌─────────────────┐
└─────────┘     └─────────────────┘     │  Pool 2 (Night) │
                                        └─────────────────┘
```

1. Miners connect to the proxy using any credentials
2. Proxy authenticates with upstream pools using configured credentials
3. At scheduled times, proxy switches all miners to the new pool
4. Pending share submissions complete before switching
5. Miners receive new job notifications automatically

## Use Cases

- **Time-of-use electricity**: Mine to different pools based on electricity rates
- **Pool rotation**: Distribute hashrate across pools on a schedule
- **Testing**: Easy switching between pools without reconfiguring miners

## License

MIT License - see [LICENSE](LICENSE) for details.
