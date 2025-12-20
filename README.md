# Crypto Stratum Proxy

> **Alpha Software**: This project is currently in alpha and undergoing active development. Use with caution in production environments. Please report any issues on GitHub.

A cross-platform cryptocurrency stratum proxy with time-based server routing. Automatically switches between mining pools based on a configurable daily schedule.

**Supported Algorithms:**
- **SHA-256**: Bitcoin (BTC), Bitcoin Cash (BCH), DigiByte (DGB), and other SHA-256 coins
- **RandomX**: Monero (XMR) and other RandomX coins
- **zkSNARK**: ALEO

## Features

- **Multi-coin support**: SHA-256 (BTC/BCH), RandomX (XMR), and ALEO with per-server coin type configuration
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
- **Connection health**: TCP keepalives and automatic reconnection on connection failures
- **Automatic difficulty management**: Coin-specific difficulty buffers with highest-seen tracking
- **Loguru logging**: Structured logging with file rotation

## Installation

### Quick Start (Recommended)

```bash
git clone https://github.com/mattx86/crypto-stratum-proxy.git
cd crypto-stratum-proxy

# Initialize proxy -- Windows
init.bat

# Initialize proxy -- Linux
chmod +x *.sh
./init.sh

# Edit config.yaml with pool and schedule configuration.

# Start proxy -- Windows
start.bat
# Start proxy -- Linux
./start.sh -f
```

This creates a virtual environment, upgrades pip, installs dependencies, and generates `config.yaml`.

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

   # Linux
   ./init.sh
   ```

   This creates a virtual environment, upgrades pip, installs dependencies, and generates `config.yaml`.

2. **Edit the configuration** with your pool details:

   ```yaml
   proxy:
     global:
       max_connections: 100
     sha256:
       enabled: true
       bind_host: "0.0.0.0"
       bind_port: 3333

   servers:
     sha256:
       - name: "btc"
         enabled: true
         host: "stratum.pool1.com"
         port: 3333
         username: "your_wallet.worker1"
         password: "x"

       - name: "bch"
         enabled: true
         host: "stratum.pool2.com"
         port: 3333
         username: "your_wallet.worker1"
         password: "x"

   schedule:
     sha256:
       - server: "btc"
         start: "00:00"
         end: "12:00"

       - server: "bch"
         start: "12:00"
         end: "24:00"
   ```

3. **Validate the configuration:**

   ```bash
   crypto-stratum-proxy validate config.yaml
   ```

4. **Start the proxy:**

   ```bash
   # Windows
   start.bat              # Run in background
   start.bat -f           # Run in foreground (for debugging)

   # Linux
   ./start.sh             # Run in background
   ./start.sh -f          # Run in foreground (for debugging)
   ```

5. **Stop the proxy:**

   ```bash
   # Windows
   stop.bat

   # Linux
   ./stop.sh
   ```

6. **Point your miners** to the proxy address (e.g., `stratum+tcp://192.168.1.100:3333`)

## Helper Scripts

| Script | Description |
|--------|-------------|
| `init.bat` / `init.sh` | Initialize: create venv, upgrade pip, install deps, create config |
| `start.bat` / `start.sh` | Start the proxy (pass `-f` for foreground) |
| `stop.bat` / `stop.sh` | Stop the running proxy |
| `status.bat` / `status.sh` | Check if proxy is running |
| `validate_config.bat` / `validate_config.sh` | Validate configuration file |

## CLI Commands

| Command | Description |
|---------|-------------|
| `crypto-stratum-proxy init` | Create sample configuration file |
| `crypto-stratum-proxy start` | Start the proxy |
| `crypto-stratum-proxy stop` | Stop the running proxy |
| `crypto-stratum-proxy status` | Check if proxy is running |
| `crypto-stratum-proxy validate <config>` | Validate a configuration file |

### Start Options

```bash
crypto-stratum-proxy start [OPTIONS]

Options:
  -c, --config PATH      Path to configuration file
  -f, --foreground       Run in foreground (don't daemonize)
  --log-level LEVEL      Override log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
```

## Configuration Reference

### Proxy Settings

```yaml
proxy:
  # Global settings shared by all algorithms
  global:
    max_connections: 100        # Maximum concurrent miner connections
    connection_timeout: 60      # Miner connection timeout (seconds)
    miner_read_timeout: 600     # Miner read timeout (seconds, default 10 min)
    send_timeout: 30            # Send to miner timeout (seconds)
    pending_shares_timeout: 10  # Wait for pending shares during switch (seconds)
    tcp_keepalive: true         # Enable TCP keepalive on connections
    keepalive_idle: 60          # Seconds before sending keepalive probes
    keepalive_interval: 10      # Seconds between keepalive probes
    keepalive_count: 3          # Failed probes before connection is dead
    share_submit_retries: 3     # Retries for failed share submissions
    upstream_health_timeout: 300  # Seconds without upstream messages before reconnecting

  # Failover settings
  failover:
    retry_timeout_minutes: 20   # Retry primary server for this long before failover

  # Share validation settings
  validation:
    reject_duplicates: true     # Reject duplicate share submissions
    reject_stale: true          # Reject shares for stale/unknown jobs
    validate_difficulty: false  # Validate share hash meets difficulty (CPU intensive)

  # Per-algorithm settings (each algorithm runs on its own port)
  sha256:
    enabled: true               # Enable SHA-256 proxy
    bind_host: "0.0.0.0"        # Address to listen on
    bind_port: 3333             # Port for SHA-256 miners

  randomx:
    enabled: false              # Enable RandomX proxy
    bind_host: "0.0.0.0"
    bind_port: 3334             # Port for RandomX miners

  aleo:
    enabled: false              # Enable ALEO proxy
    bind_host: "0.0.0.0"
    bind_port: 3335             # Port for ALEO miners
```

**Keepalive Settings:**
TCP keepalives help detect dead connections at the OS level. When enabled, the OS sends periodic probes to verify connections are still alive. If a connection fails to respond to `keepalive_count` probes sent `keepalive_interval` seconds apart, it is considered dead. The first probe is sent after `keepalive_idle` seconds of inactivity.

**Socket Optimizations (automatic):**
The proxy automatically applies socket optimizations for low-latency stratum communication:
- `TCP_NODELAY`: Disables Nagle's algorithm so small JSON-RPC messages are sent immediately
- `IP_TOS`: Marks packets as low-delay (IPTOS_LOWDELAY) for QoS prioritization by routers

**Share Retry Settings:**
When a share submission fails due to transient errors (timeouts, connection issues), the proxy will retry up to `share_submit_retries` times with exponential backoff. Explicit pool rejections (duplicate, stale, low difficulty) are not retried.

**Upstream Health Timeout:**
The proxy monitors connection health by tracking when the last message was received from the upstream pool. Pools typically send `mining.notify` messages every 30-60 seconds. If no messages are received for `upstream_health_timeout` seconds (default 300), the connection is considered unhealthy and will be reconnected. This detects zombie connections that TCP keepalive may not catch.

### Server Settings

Servers are organized by algorithm:

```yaml
servers:
  sha256:
    - name: "unique_name"     # Unique identifier for this server
      enabled: true           # Enable/disable this server
      host: "pool.example.com"
      port: 3333
      username: "wallet.worker"
      password: "x"
      ssl: false              # Use SSL/TLS connection
      timeout: 30             # Connection timeout (seconds)
      retry_interval: 5       # Seconds between reconnection attempts

  randomx:
    - name: "xmr_pool"
      enabled: true
      host: "xmr.pool.com"
      port: 3333
      username: "wallet_address"
      password: "x"
```

**Algorithm Types:**
| Algorithm | Coins | Difficulty Buffer |
|-----------|-------|-------------------|
| `sha256` | Bitcoin, Bitcoin Cash, DigiByte | Fixed +1000 |
| `randomx` | Monero (XMR) | 5% of pool difficulty |
| `aleo` | ALEO | 5% of pool difficulty |

**Note:** Hash validation (`validate_difficulty`) is only supported for SHA-256 coins. For other algorithms, this setting is automatically disabled.

### Schedule Settings

Schedules are organized by algorithm:

```yaml
schedule:
  sha256:
    - server: "server_name"   # Server to use during this period
      start: "00:00"          # Start time (HH:MM)
      end: "12:00"            # End time (HH:MM, use 24:00 for midnight)

  randomx:
    - server: "xmr_pool"
      start: "00:00"
      end: "24:00"            # Run 24/7 on this pool
```

**Notes:**
- Times are in 24-hour format (local time)
- Use `24:00` to represent end of day
- Timeframes must not overlap within each algorithm
- All servers referenced in schedule must be defined in the same algorithm's `servers` section

### Logging Settings

```yaml
logging:
  level: "INFO"             # DEBUG, INFO, WARNING, ERROR, CRITICAL
  file: "/var/log/proxy.log"  # Log file path (null for console only)
  rotation: "50 MB"         # Rotate when file reaches this size
  retention: 10             # Keep this many rotated files
  format: "{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {message}"  # Log format
```

### Automatic Difficulty Management

The proxy automatically manages difficulty to combat vardiff-induced duplicate share rejections. No configuration is required.

**Behavior:**
- When pool sets difficulty, proxy sends `(pool_difficulty + buffer)` to miner
- Buffer is algorithm-specific: +1000 for SHA-256, +5% for RandomX/ALEO
- Difficulty only increases, never decreases (highest-seen tracking)
- Miner `mining.suggest_difficulty` requests are forwarded to pool unchanged
- Difficulty resets on pool switch (new session with new pool)

**Auto-adjustment:**
- **Low difficulty rejection**: If pool rejects share as "low difficulty" (or similar), proxy raises difficulty to `(rejected_difficulty + buffer)` if that's higher than current
- **Stale share rejection**: If pool rejects share as "stale" or "job not found", proxy lowers difficulty by buffer (if still above pool difficulty)
- **Floor-reset**: If pool difficulty drops to less than 50% of miner difficulty, proxy resets to 75% of miner difficulty (25% reduction) if still > pool difficulty + buffer. This prevents excessive divergence from pool expectations.

**Note:** The automatic difficulty buffer prevents most vardiff-induced duplicate rejections while keeping the miner working at a difficulty close to what the pool expects.

## Statistics

The proxy logs detailed statistics every 15 minutes (at :00, :15, :30, :45). Example output:

```
============================================================
PROXY STATISTICS (uptime: 2h 30m)
============================================================
Servers:
  - pool1 (stratum.pool1.com:3333) Schedule: 00:00-12:00 Username: wallet.worker1
  - pool2 (stratum.pool2.com:3333) Schedule: 12:00-24:00 Username: wallet.worker2
Miners: 3 active | 5 total connections | 2 disconnections
Active server: pool1 (stratum.pool1.com:3333)
Previous server: pool2 (stratum.pool2.com:3333)
Last switch: 2025-12-16 12:00:00-0600
----------------------------------------
Server: pool1 (active)
  Connections: 1 | Reconnections: 0
  Shares: 12729 accepted / 127 rejected (99.0% / 1.0%)
  Rejection reasons: "duplicate share": 55, "low difficulty share": 45, "stale job": 6
----------------------------------------
Server: pool2
  Connections: 1 | Reconnections: 0
  Shares: 500 accepted / 10 rejected (98.0% / 2.0%)
  Rejection reasons: "duplicate share": 5, "job not found": 3
============================================================
```

**Tracked metrics:**
- Server configuration (address, schedule, username)
- Active miners and connection history
- Currently active server with address (marked with `(active)` in per-server stats)
- Previous server and last switch time with timezone
- Per-server share acceptance/rejection rates
- Rejection reasons (normalized into categories):
  - `duplicate share` - Share already submitted
  - `stale job` - Job expired or unknown
  - `low difficulty share` - Share doesn't meet pool difficulty
  - `job not found` - Pool doesn't recognize the job
  - `unauthorized` - Worker not authorized
  - `timeout` - Submission timed out

## How It Works

```
+--------+    +------------------+    +------------------+
| Miners |--->|  Stratum Proxy   |--->|  Pool 1 (Day)    |
|        |--->|                  |    +------------------+
|        |--->|  Time Router     |--->+------------------+
+--------+    +------------------+    |  Pool 2 (Night)  |
                                      +------------------+
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
