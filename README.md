# Crypto Stratum Proxy

> **Alpha Software**: This project is currently in alpha and undergoing active development. Use with caution in production environments. Please report any issues on GitHub.

A cross-platform cryptocurrency stratum proxy with time-based server routing. Automatically switches between mining pools based on a configurable daily schedule.

**Supported Algorithms:**
- **SHA-256**: Bitcoin (BTC), Bitcoin Cash (BCH), DigiByte (DGB), and other SHA-256 coins
- **RandomX**: Monero (XMR) and other RandomX coins
- **zkSNARK**: ALEO and other zkSNARK coins

## Features

- **Multi-coin support**: SHA-256 (BTC/BCH), RandomX (XMR), and zkSNARK (ALEO) with per-algorithm proxy ports
- **Time-based routing**: Automatically switch between stratum servers on a schedule (configured per-server)
- **Multiple miners**: Support for concurrent miner connections (default 1024 per algorithm)
- **Circuit breaker**: Automatic failure handling with configurable thresholds and recovery
- **Graceful switching**: Waits for pending share submissions before switching servers, with grace period for in-flight work
- **Cross-platform**: Runs on Windows and Linux
- **Background mode**: Run as a daemon/background service
- **Config validation**: Detects overlapping timeframes and invalid configurations at startup
- **Statistics**: Tracks accepts/rejects per pool with periodic logging (every 15 min)
- **Version-rolling**: Supports ASICBoost/version-rolling negotiation with pools
- **Share validation**: Rejects duplicate shares and stale jobs before forwarding to pool
- **Connection health**: TCP keepalives, upstream health monitoring, and automatic reconnection
- **Adaptive difficulty management**: Automatic buffer adjustment, share rate monitoring, and per-server overrides
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
       max_connections: 1024
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
         start: "00:00"        # Schedule start time
         end: "12:00"          # Schedule end time

       - name: "bch"
         enabled: true
         host: "stratum.pool2.com"
         port: 3333
         username: "your_wallet.worker1"
         password: "x"
         start: "12:00"
         end: "24:00"          # Use 24:00 for end of day
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
    max_connections: 1024         # Maximum concurrent miner connections
    miner_read_timeout: 180       # Miner read timeout (seconds, default 3 min)
    miner_write_timeout: 30       # Miner write timeout (seconds)
    upstream_idle_timeout: 300    # Seconds without upstream messages before reconnecting
    server_switch_timeout: 900    # Seconds to retry connecting to new server during switch (15 min)
    tcp_keepalive: true           # Enable TCP keepalive on connections
    keepalive_idle: 60            # Seconds before sending keepalive probes
    keepalive_interval: 10        # Seconds between keepalive probes
    keepalive_count: 3            # Failed probes before connection is dead
    share_submit_retries: 3       # Retries for failed share submissions
    pending_shares_timeout: 10    # Wait for pending shares during switch (seconds)

  # Share validation settings
  validation:
    reject_duplicates: true       # Reject duplicate share submissions
    reject_stale: true            # Reject shares for stale/unknown jobs
    share_cache_size: 1000        # Max recent shares to track per session
    share_cache_ttl: 300          # Share cache TTL in seconds
    job_cache_size: 10            # Max jobs to track per session

  # Difficulty management settings
  difficulty:
    change_cooldown: 30           # Minimum seconds between difficulty changes
    decay_enabled: true           # Enable gradual decay toward pool difficulty
    decay_interval: 300           # Seconds between decay checks (5 min)
    decay_percent: 0.05           # Decay percentage per interval (5%)
    floor_trigger_ratio: 0.50     # Trigger floor-reset when pool < 50% of miner difficulty
    floor_reset_ratio: 0.75       # Reset to 75% of current miner difficulty
    share_rate_window: 300        # Window in seconds to track share rate
    share_rate_min_shares: 3      # Minimum shares needed before analyzing rate
    share_rate_low_multiplier: 0.5  # Lower difficulty if rate drops to this fraction
    buffer_start: 0.05            # Starting buffer percentage (5%)
    buffer_min: 0.02              # Minimum buffer percentage (2%)
    buffer_max: 0.20              # Maximum buffer percentage (20%)
    buffer_increase_step: 0.01    # Buffer increase step on rejection (1%)
    buffer_decrease_interval: 3600  # Seconds without rejection before decreasing buffer

  # Circuit breaker - stop trying failed pools temporarily
  circuit_breaker:
    enabled: true                 # Enable circuit breaker pattern
    failure_threshold: 5          # Failures before opening circuit
    recovery_timeout: 60          # Seconds to wait before retry
    success_threshold: 2          # Successes needed to close circuit

  # Per-algorithm settings (each algorithm runs on its own port)
  sha256:
    enabled: true                 # Enable SHA-256 proxy
    bind_host: "0.0.0.0"          # Address to listen on
    bind_port: 3333               # Port for SHA-256 miners

  randomx:
    enabled: false                # Enable RandomX proxy
    bind_host: "0.0.0.0"
    bind_port: 3334               # Port for RandomX miners

  zksnark:
    enabled: false                # Enable zkSNARK (ALEO) proxy
    bind_host: "0.0.0.0"
    bind_port: 3335               # Port for zkSNARK miners
```

**Keepalive Settings:**
TCP keepalives help detect dead connections at the OS level. When enabled, the OS sends periodic probes to verify connections are still alive. If a connection fails to respond to `keepalive_count` probes sent `keepalive_interval` seconds apart, it is considered dead. The first probe is sent after `keepalive_idle` seconds of inactivity.

**Socket Optimizations (automatic):**
The proxy automatically applies socket optimizations for low-latency stratum communication:
- `TCP_NODELAY`: Disables Nagle's algorithm so small JSON-RPC messages are sent immediately
- `IP_TOS`: Marks packets as low-delay (IPTOS_LOWDELAY) for QoS prioritization by routers

**Share Retry Settings:**
When a share submission fails due to transient errors (timeouts, connection issues), the proxy will retry up to `share_submit_retries` times with exponential backoff. Explicit pool rejections (duplicate, stale, low difficulty) are not retried.

**Upstream Idle Timeout:**
The proxy monitors connection health by tracking when the last message was received from the upstream pool. Pools typically send `mining.notify` messages every 30-60 seconds. If no messages are received for `upstream_idle_timeout` seconds (default 300), the connection is considered unhealthy and will be reconnected. This detects zombie connections that TCP keepalive may not catch.

**Circuit Breaker:**
The circuit breaker pattern prevents repeated connection attempts to failed pools. After `failure_threshold` consecutive failures, the circuit "opens" and the proxy stops trying that pool for `recovery_timeout` seconds. After the timeout, the circuit enters "half-open" state and allows test connections. After `success_threshold` successful connections, the circuit "closes" and normal operation resumes.

### Server Settings

Servers are organized by algorithm, with schedule times embedded in each server:

```yaml
servers:
  sha256:
    - name: "unique_name"       # Unique identifier for this server
      enabled: true             # Enable/disable this server
      host: "pool.example.com"
      port: 3333
      username: "wallet.worker"
      password: "x"
      ssl: false                # Use SSL/TLS connection
      connect_timeout: 30       # Connection timeout (seconds)
      retry_interval: 5         # Seconds between reconnection attempts
      start: "00:00"            # Schedule start time (HH:MM)
      end: "12:00"              # Schedule end time (HH:MM, use 24:00 for midnight)
      # Per-server difficulty overrides (optional)
      # buffer_percent: 0.10    # Override global buffer (e.g., 10% for this pool)
      # min_difficulty: 1000    # Minimum difficulty for this pool
      # max_difficulty: 1000000 # Maximum difficulty for this pool

  randomx:
    - name: "xmr_pool"
      enabled: true
      host: "xmr.pool.com"
      port: 3333
      username: "wallet_address"
      password: "x"
      start: "00:00"
      end: "24:00"              # Run 24/7 on this pool

  zksnark:
    - name: "aleo_pool"
      enabled: true
      host: "aleo.pool.com"
      port: 3335
      username: "wallet_address"
      password: "x"
      start: "00:00"
      end: "24:00"
```

**Algorithm Types:**
| Algorithm | Coins | Default Buffer |
|-----------|-------|----------------|
| `sha256` | Bitcoin, Bitcoin Cash, DigiByte | 5% (adaptive) |
| `randomx` | Monero (XMR) | 5% (adaptive) |
| `zksnark` | ALEO | 5% (adaptive) |

**Per-Server Difficulty Overrides:**
Each server can override the global difficulty settings:
- `buffer_percent`: Override the buffer percentage (0.01-0.50)
- `min_difficulty`: Set a minimum difficulty floor
- `max_difficulty`: Set a maximum difficulty ceiling

### Schedule Settings

Schedules are configured directly on each server using `start` and `end` fields:

```yaml
servers:
  sha256:
    - name: "pool1"
      # ... other settings ...
      start: "00:00"          # Start time (HH:MM)
      end: "12:00"            # End time (HH:MM)

    - name: "pool2"
      # ... other settings ...
      start: "12:00"
      end: "24:00"            # Use 24:00 for end of day
```

**Notes:**
- Times are in 24-hour format (local time)
- Use `24:00` to represent end of day (midnight)
- Timeframes must not overlap within each algorithm
- Schedules must cover all 24 hours (no gaps allowed)
- End time is exclusive: schedule "00:00-12:00" includes 11:59:59 but not 12:00:00
- Midnight crossover is supported (e.g., "22:00" to "02:00")

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

The proxy automatically manages difficulty to combat vardiff-induced duplicate share rejections. All settings are configurable but work well with defaults.

**Core Behavior:**
- When pool sets difficulty, proxy sends `(pool_difficulty + buffer)` to miner
- Buffer starts at 5% and adapts based on rejection patterns (range: 2%-20%)
- Difficulty only increases normally (highest-seen tracking)
- Miner `mining.suggest_difficulty` requests are forwarded to pool unchanged
- Difficulty resets on pool switch (new session with new pool)

**Adaptive Buffer:**
- Starts at `buffer_start` (default 5%)
- Increases by `buffer_increase_step` (1%) after each low-difficulty rejection
- Decreases by `buffer_increase_step` after `buffer_decrease_interval` (1 hour) without rejections
- Stays within `buffer_min` (2%) and `buffer_max` (20%) bounds

**Auto-adjustment:**
- **Low difficulty rejection**: If pool rejects share as "low difficulty", proxy raises difficulty to `(rejected_difficulty + buffer)` and increases adaptive buffer
- **Floor-reset**: If pool difficulty drops below `floor_trigger_ratio` (50%) of miner difficulty, proxy resets to `floor_reset_ratio` (75%) of miner difficulty
- **Time-based decay**: Every `decay_interval` (5 min), difficulty decays by `decay_percent` (5%) toward pool+buffer if above target
- **Cooldown**: Changes are rate-limited to once per `change_cooldown` (30s) except for low-diff rejections

**Share Rate Monitoring:**
- Tracks share submission rate over `share_rate_window` (5 min)
- Learns a baseline share rate from initial mining
- If rate drops below `share_rate_low_multiplier` (50%) of baseline, lowers difficulty (may indicate difficulty is too high)

**Per-Server Overrides:**
Configure different settings per pool using server-level options:
- `buffer_percent`: Override the adaptive buffer for this pool
- `min_difficulty`: Set a minimum difficulty floor
- `max_difficulty`: Set a maximum difficulty ceiling

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
5. A grace period allows in-flight work from the old pool to be submitted
6. Miners receive new job notifications automatically

## Use Cases

- **Time-of-use electricity**: Mine to different pools based on electricity rates
- **Pool rotation**: Distribute hashrate across pools on a schedule
- **Testing**: Easy switching between pools without reconfiguring miners

## License

MIT License - see [LICENSE](LICENSE) for details.
