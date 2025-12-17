# BTC/BCH Stratum Proxy

> **Beta Software**: This project is currently in beta and under active development. Use with caution in production environments. Please report any issues on GitHub.

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
- **Connection health**: TCP keepalives and automatic reconnection on connection failures
- **Worker difficulty override**: Per-worker minimum difficulty settings to combat vardiff-induced duplicate shares
- **Loguru logging**: Structured logging with file rotation

## Installation

### Quick Start (Recommended)

```bash
git clone https://github.com/mattx86/btc-bch-proxy.git
cd btc-bch-proxy

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
     bind_host: "0.0.0.0"
     bind_port: 3333

   servers:
     - name: "btc"
       host: "stratum.pool1.com"
       port: 3333
       username: "your_wallet.worker1"
       password: "x"

     - name: "bch"
       host: "stratum.pool2.com"
       port: 3333
       username: "your_wallet.worker1"
       password: "x"

   schedule:
     - start: "00:00"
       end: "12:00"
       server: "btc"

     - start: "12:00"
       end: "24:00"
       server: "bch"
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

## CLI Commands

| Command | Description |
|---------|-------------|
| `btc-bch-proxy init` | Create sample configuration file |
| `btc-bch-proxy start` | Start the proxy |
| `btc-bch-proxy stop` | Stop the running proxy |
| `btc-bch-proxy status` | Check if proxy is running |
| `btc-bch-proxy validate <config>` | Validate a configuration file |

### Start Options

```bash
btc-bch-proxy start [OPTIONS]

Options:
  -c, --config PATH      Path to configuration file
  -f, --foreground       Run in foreground (don't daemonize)
  --log-level LEVEL      Override log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
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
  tcp_keepalive: true         # Enable TCP keepalive on connections
  keepalive_idle: 60          # Seconds before sending keepalive probes
  keepalive_interval: 10      # Seconds between keepalive probes
  keepalive_count: 3          # Failed probes before connection is dead
  share_submit_retries: 3     # Retries for failed share submissions
  upstream_health_timeout: 300  # Seconds without upstream messages before reconnecting
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
  level: "INFO"             # DEBUG, INFO, WARNING, ERROR, CRITICAL
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

### Worker Settings (Optional)

Configure per-worker difficulty overrides. Workers are identified by the username the miner uses when connecting to the proxy (via `mining.authorize`).

This feature was implemented to combat pools' variable difficulty (vardiff) causing duplicate share rejections. When vardiff sets difficulty too low for fast miners, the miner may exhaust nonce space or submit shares faster than new jobs arrive, resulting in duplicates. By forcing a higher minimum difficulty, share submission rate is reduced and duplicates are minimized.

```yaml
workers:
  - username: "miner1"           # Username the miner uses to connect to the proxy
    difficulty: 50000000         # Preferred difficulty for this worker
```

**Behavior:**
- The configured difficulty is only applied if it is **> the pool's difficulty**
- If the configured difficulty is lower than or equal to the pool's, the pool's difficulty is used
- This prevents rejected shares due to low difficulty

**Note:** Forcing a higher difficulty will cause the pool to report a lower hashrate for the worker. This is because the pool calculates hashrate based on its own difficulty setting, not the override. Your actual hashrate and earnings are unaffected.

**Use case:** Force higher difficulty for powerful miners to reduce share submission frequency and duplicate share rejections without changing pool settings.

**Suggestion:** Set the difficulty override to at least the pool's initial/minimum difficulty + 1000. For example, if the pool starts workers at difficulty 10000, set the override to 11000 or higher. Adjust based on observed duplicate rejection rates.

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
