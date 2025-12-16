"""Shared constants for the proxy module."""

# Socket buffer size for reading data (bytes)
SOCKET_READ_BUFFER_SIZE = 8192

# Short sleep duration for polling loops (seconds)
POLL_SLEEP_INTERVAL = 0.1

# Short timeout for non-blocking reads (seconds)
NON_BLOCKING_READ_TIMEOUT = 0.1

# Maximum length for miner-provided strings (usernames, user agents)
MAX_MINER_STRING_LENGTH = 256

# Initial delay for share submit retry (seconds)
SHARE_SUBMIT_INITIAL_RETRY_DELAY = 0.5

# Maximum delay between share submit retries (seconds)
# Caps exponential backoff to prevent excessively long waits
SHARE_SUBMIT_MAX_RETRY_DELAY = 5.0

# Maximum total time for share submit retries (seconds)
# Prevents indefinite blocking if retries + reconnects take too long
SHARE_SUBMIT_MAX_TOTAL_TIME = 30.0

# Default upstream health timeout when no config provided (seconds)
DEFAULT_UPSTREAM_HEALTH_TIMEOUT = 300

# Scheduler check interval (seconds)
SCHEDULER_CHECK_INTERVAL = 60

# Maximum size for miner send queue (provides backpressure)
MINER_SEND_QUEUE_MAX_SIZE = 1000

# Timeout for miner write loop queue get (seconds)
# Short timeout allows periodic state checks while waiting for messages
MINER_WRITE_LOOP_TIMEOUT = 1.0

# Health check interval for upstream connection (iterations)
# At 0.1s read timeout, 100 iterations = ~10 seconds between health checks
UPSTREAM_HEALTH_CHECK_INTERVAL = 100

# Default timeout for waiting on pending shares during server switch (seconds)
DEFAULT_PENDING_SHARES_WAIT_TIMEOUT = 10.0

# Timeout for draining miner queue during shutdown (seconds)
# Prevents indefinite shutdown if miner socket is broken
QUEUE_DRAIN_TIMEOUT = 5.0

# Timeout for individual write operations during queue drain (seconds)
QUEUE_DRAIN_WRITE_TIMEOUT = 1.0

# Maximum length for error messages in logs (prevents log bloat from verbose pool errors)
MAX_ERROR_MESSAGE_LENGTH = 200

# Maximum length for background task exception messages (more verbose for debugging)
MAX_BACKGROUND_ERROR_LENGTH = 500

# Grace period after server switch for stale shares (seconds)
# Shares from the previous pool during this period are routed to the old pool
SERVER_SWITCH_GRACE_PERIOD = 30.0

# Grace period extension when a share is routed to old pool (seconds)
# Each time a share is successfully routed to the old pool, the grace period
# is extended by this amount, allowing in-flight work to complete
GRACE_PERIOD_EXTENSION = 15.0
