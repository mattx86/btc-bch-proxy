"""Share validation and duplicate detection."""

from __future__ import annotations

import time
from collections import OrderedDict
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Optional

from loguru import logger

from crypto_stratum_proxy.proxy.constants import JOB_SOURCE_TTL, MAX_MERKLE_BRANCHES

if TYPE_CHECKING:
    from crypto_stratum_proxy.config.models import ValidationConfig


@dataclass
class JobInfo:
    """Information about a mining job from mining.notify."""

    job_id: str
    clean_jobs: bool
    received_at: float = field(default_factory=time.time)
    source_server: Optional[str] = None  # Which pool issued this job (for grace period routing)

    # SHA-256 specific fields (optional for other algorithms)
    prevhash: Optional[str] = None
    coinbase1: Optional[str] = None
    coinbase2: Optional[str] = None
    merkle_branches: Optional[list[str]] = None
    version: Optional[str] = None
    nbits: Optional[str] = None
    ntime: Optional[str] = None

    # zkSNARK/ALEO specific fields (optional)
    height: Optional[int] = None
    target: Optional[int] = None
    block_header_root: Optional[str] = None
    hashed_beacons_root: Optional[str] = None

    # RandomX specific fields (optional)
    blob: Optional[str] = None  # Block template blob (hex)
    target_hex: Optional[str] = None  # Mining target (hex string)
    seed_hash: Optional[str] = None  # RandomX seed hash


@dataclass
class ShareKey:
    """Unique identifier for a SHA-256 share submission."""

    job_id: str
    extranonce2: str
    ntime: str
    nonce: str
    version_bits: Optional[str] = None  # For version-rolling (ASICBoost)

    def __hash__(self):
        return hash((self.job_id, self.extranonce2, self.ntime, self.nonce, self.version_bits))

    def __eq__(self, other):
        if not isinstance(other, ShareKey):
            return False
        return (
            self.job_id == other.job_id
            and self.extranonce2 == other.extranonce2
            and self.ntime == other.ntime
            and self.nonce == other.nonce
            and self.version_bits == other.version_bits
        )


@dataclass
class SimpleShareKey:
    """
    Unique identifier for a simple share submission (job_id + nonce).

    Used by zkSNARK/ALEO and RandomX algorithms which only use
    job_id and nonce to identify a share (unlike SHA-256 which also
    includes extranonce2, ntime, and version_bits).
    """

    job_id: str
    nonce: str

    def __hash__(self):
        return hash((self.job_id, self.nonce))

    def __eq__(self, other):
        if not isinstance(other, SimpleShareKey):
            return False
        return self.job_id == other.job_id and self.nonce == other.nonce


class ShareValidator:
    """
    Validates shares before submission to upstream pool.

    Features:
    - Duplicate share detection
    - Stale job detection
    - Difficulty tracking (for stats)

    Thread Safety:
        This class is NOT thread-safe. It is designed to be used from a single
        async task (the MinerSession that owns it). Each MinerSession creates
        its own ShareValidator instance, so no locking is required.

        IMPORTANT: Do NOT share a ShareValidator instance between tasks or call
        its methods concurrently. The internal OrderedDict structures are not
        protected by locks and concurrent access will cause race conditions.
        If you need thread-safe validation, add asyncio.Lock protection.
    """

    def __init__(
        self,
        session_id: str,
        config: Optional[ValidationConfig] = None,
        algorithm: str = "sha256",
    ):
        """
        Initialize the share validator.

        Args:
            session_id: Session identifier for logging.
            config: Validation configuration.
            algorithm: Mining algorithm (sha256, randomx, zksnark).
        """
        self.session_id = session_id
        self._algorithm = algorithm

        # Configuration (use defaults if not provided)
        self._reject_duplicates = config.reject_duplicates if config else True
        self._reject_stale = config.reject_stale if config else True
        self._max_share_cache = config.share_cache_size if config else 1000
        self._share_cache_ttl = config.share_cache_ttl if config else 300
        self._max_job_cache = config.job_cache_size if config else 10

        # Recent shares for duplicate detection (OrderedDict for LRU behavior)
        # Note: Cache cleanup (_clean_share_cache) runs on each validate_share() call.
        # Expired entries may persist if no shares are submitted, but this is acceptable:
        # - Cache size is bounded by _max_share_cache (prevents memory growth)
        # - Memory overhead is minimal (~100 bytes per entry, max ~100KB total)
        # - Periodic background cleanup would add complexity without meaningful benefit
        self._recent_shares: OrderedDict[ShareKey, float] = OrderedDict()

        # Current and recent jobs
        self._jobs: OrderedDict[str, JobInfo] = OrderedDict()
        self._current_job_id: Optional[str] = None

        # Job source tracking for grace period routing
        # Maps job_id -> (source_server, timestamp) - persists across cache clears
        self._job_sources: OrderedDict[str, tuple[str, float]] = OrderedDict()
        self._max_job_sources = 4096  # Limit to prevent unbounded growth
        self._last_job_source_cleanup: float = 0.0

        # Jobs known to be stale (rejected by pool with "unknown-work")
        # Used to reject subsequent shares locally without forwarding to pool
        # Dict for FIFO ordering (Python 3.7+) - value is timestamp
        self._stale_jobs: dict[str, float] = {}

        # Current difficulty
        self._difficulty: float = 1.0

        # zkSNARK/ALEO target tracking (for logging target changes)
        self._last_zksnark_target: Optional[int] = None

        # Expected extranonce2 size (in bytes, set by pool)
        self._extranonce2_size: Optional[int] = None

        # Statistics
        self.duplicates_rejected: int = 0
        self.stale_rejected: int = 0
        self.invalid_format_rejected: int = 0

        # Cache cleanup throttling (cleanup at most once per 60 seconds)
        self._last_cache_cleanup: float = 0.0
        self._cache_cleanup_interval: float = 60.0

        # Log prefix (can be updated by session for full context)
        self._log_prefix: str = f"[{session_id}]"

    def set_log_prefix(self, prefix: str) -> None:
        """Update the log prefix (called by session when context is available)."""
        self._log_prefix = prefix

    def set_difficulty(self, difficulty: float) -> None:
        """
        Update the current difficulty.

        Args:
            difficulty: New difficulty value.
        """
        if difficulty > 0:
            self._difficulty = difficulty
            logger.debug(f"[{self.session_id}] Difficulty set to {difficulty}")
        else:
            logger.warning(f"[{self.session_id}] Ignoring invalid difficulty: {difficulty}")

    # Minimum extranonce2 size - values below this are unusual and may indicate
    # pool misconfiguration. Most pools use 4-8 bytes.
    MIN_EXTRANONCE2_SIZE = 2

    def set_extranonce2_size(self, size: int) -> None:
        """
        Set the expected extranonce2 size from pool subscription.

        Args:
            size: Size in bytes (e.g., 4 means 8 hex characters).
        """
        if size <= 0:
            logger.warning(f"[{self.session_id}] Ignoring invalid extranonce2 size: {size}")
            return

        if size < self.MIN_EXTRANONCE2_SIZE:
            logger.warning(
                f"[{self.session_id}] Unusually small extranonce2 size: {size} bytes "
                f"(expected >= {self.MIN_EXTRANONCE2_SIZE}). Pool may be misconfigured."
            )
            # Still set it - the pool knows what it's doing (maybe)

        self._extranonce2_size = size
        logger.debug(f"[{self.session_id}] Extranonce2 size set to {size} bytes")

    def add_job(self, job_info: JobInfo, source_server: Optional[str] = None) -> None:
        """
        Add a new job from mining.notify.

        Args:
            job_info: Job information.
            source_server: Optional server name that issued this job (for grace period routing).
        """
        now = time.time()

        # Periodic cleanup of expired job sources (throttled)
        if now - self._last_job_source_cleanup > 30.0:  # Check every 30 seconds
            self._clean_job_sources()
            self._last_job_source_cleanup = now

        # Store source server in job info if provided
        if source_server:
            job_info.source_server = source_server
            # Track in job_sources dict with timestamp (persists for grace period)
            self._job_sources[job_info.job_id] = (source_server, now)
            # Limit job source tracking to prevent unbounded growth
            while len(self._job_sources) > self._max_job_sources:
                self._job_sources.popitem(last=False)

        self._jobs[job_info.job_id] = job_info
        self._current_job_id = job_info.job_id

        # If clean_jobs is true, clear old jobs AND share cache
        # New block found - all previous work is now stale
        if job_info.clean_jobs:
            # For zkSNARK/ALEO: Keep recent jobs even with clean_jobs=True
            # because the pool cycles jobs very rapidly and miners may still
            # be working on older jobs. We need a buffer for retry attempts.
            if self._algorithm == "zksnark":
                # Keep last N jobs for retry purposes (limit to 10)
                max_zksnark_jobs = 10
                while len(self._jobs) > max_zksnark_jobs:
                    self._jobs.popitem(last=False)
                logger.debug(
                    f"[{self.session_id}] zkSNARK: kept {len(self._jobs)} jobs despite clean_jobs"
                )
            else:
                # For other algorithms: keep only the current job
                self._jobs = OrderedDict([(job_info.job_id, job_info)])
                # Clear share cache - old shares are worthless after new block
                old_share_count = len(self._recent_shares)
                self._recent_shares.clear()
                if old_share_count > 0:
                    logger.debug(
                        f"[{self.session_id}] Cleared {old_share_count} cached shares (new block)"
                    )
            # NOTE: Do NOT clear _job_sources here - needed for grace period routing
        else:
            # Limit job cache size
            while len(self._jobs) > self._max_job_cache:
                self._jobs.popitem(last=False)

        # NOTE: We do NOT clear stale job markers here anymore.
        # Previously we cleared them on new job arrival, but this was wrong because:
        # - The miner may still have queued shares for old stale jobs
        # - Clearing would cause those shares to be forwarded to pool (and rejected)
        # - Stale markers now expire via FIFO eviction in mark_job_stale()

        logger.debug(
            f"[{self.session_id}] New job {job_info.job_id} from {source_server or 'unknown'} "
            f"(clean={job_info.clean_jobs}, total jobs={len(self._jobs)})"
        )

    def add_job_from_notify(self, params: list, source_server: Optional[str] = None) -> None:
        """
        Parse and add a job from mining.notify parameters.

        Args:
            params: mining.notify parameters.
            source_server: Optional server name that issued this job (for grace period routing).
        """
        if self._algorithm == "sha256":
            self._add_sha256_job(params, source_server)
        elif self._algorithm == "zksnark":
            self._add_zksnark_job(params, source_server)
        elif self._algorithm == "randomx":
            self._add_randomx_job(params, source_server)
        else:
            # Fallback: try to extract at least job_id and clean_jobs
            self._add_generic_job(params, source_server)

    def _add_sha256_job(self, params: list, source_server: Optional[str] = None) -> None:
        """Parse SHA-256 (Bitcoin-style) mining.notify params."""
        if len(params) < 9:
            logger.warning(f"[{self.session_id}] Invalid SHA-256 mining.notify params: {params}")
            return

        # Parse merkle branches with DoS protection
        merkle_branches = []
        if isinstance(params[4], list):
            if len(params[4]) > MAX_MERKLE_BRANCHES:
                logger.warning(
                    f"[{self.session_id}] Excessive merkle branches ({len(params[4])}), "
                    f"truncating to {MAX_MERKLE_BRANCHES}"
                )
                merkle_branches = [str(b) for b in params[4][:MAX_MERKLE_BRANCHES]]
            else:
                merkle_branches = [str(b) for b in params[4]]

        job_info = JobInfo(
            job_id=str(params[0]),
            clean_jobs=bool(params[8]),
            prevhash=str(params[1]),
            coinbase1=str(params[2]),
            coinbase2=str(params[3]),
            merkle_branches=merkle_branches,
            version=str(params[5]),
            nbits=str(params[6]),
            ntime=str(params[7]),
        )
        self.add_job(job_info, source_server)

    def _add_zksnark_job(self, params: list, source_server: Optional[str] = None) -> None:
        """
        Parse zkSNARK/ALEO mining.notify params.

        ALEO stratum format: [job_id, height, target, block_header_root, hashed_beacons_root, clean_jobs]
        """
        if len(params) < 6:
            logger.warning(f"[{self.session_id}] Invalid zkSNARK mining.notify params: {params}")
            return

        try:
            # Parse clean_jobs carefully - could be bool, int, or string
            clean_jobs_raw = params[5]
            if isinstance(clean_jobs_raw, bool):
                clean_jobs = clean_jobs_raw
            elif isinstance(clean_jobs_raw, int):
                clean_jobs = clean_jobs_raw != 0
            elif isinstance(clean_jobs_raw, str):
                clean_jobs = clean_jobs_raw.lower() in ("true", "1", "yes")
            else:
                clean_jobs = bool(clean_jobs_raw)

            height_val = int(params[1]) if params[1] is not None else None
            new_target = int(params[2]) if params[2] is not None else None
            job_info = JobInfo(
                job_id=str(params[0]),
                clean_jobs=clean_jobs,
                height=height_val,
                target=new_target,
                block_header_root=str(params[3]) if params[3] else None,
                hashed_beacons_root=str(params[4]) if params[4] else None,
            )

            # Log target changes for difficulty tracking visibility
            if new_target is not None and self._last_zksnark_target is not None:
                if new_target != self._last_zksnark_target:
                    # Calculate change direction and magnitude
                    if new_target > self._last_zksnark_target:
                        change = "increased"
                        ratio = new_target / self._last_zksnark_target
                    else:
                        change = "decreased"
                        ratio = self._last_zksnark_target / new_target
                    logger.info(
                        f"{self._log_prefix} zkSNARK target {change}: "
                        f"{self._last_zksnark_target} -> {new_target} ({ratio:.1f}x)"
                    )

            self._last_zksnark_target = new_target

            logger.debug(
                f"{self._log_prefix} zkSNARK job: id={job_info.job_id}, height={height_val}, "
                f"target={job_info.target}, clean={clean_jobs}"
            )
            self.add_job(job_info, source_server)
        except (ValueError, TypeError) as e:
            logger.warning(f"{self._log_prefix} Error parsing zkSNARK job params: {e}")

    def _add_randomx_job(self, params: list, source_server: Optional[str] = None) -> None:
        """
        Parse RandomX mining.notify params.

        RandomX stratum format varies by pool but commonly:
        - [job_id, blob, target] - minimal format
        - [job_id, blob, target, height] - with height
        - [job_id, blob, target, height, seed_hash] - with RandomX seed
        - [job_id, blob, target, height, seed_hash, next_seed_hash] - full format

        The last param may be clean_jobs boolean in some implementations.
        """
        if len(params) < 3:
            logger.warning(f"[{self.session_id}] Invalid RandomX mining.notify params: {params}")
            return

        try:
            job_id = str(params[0])
            blob = str(params[1])
            target_hex = str(params[2])

            # Height is optional (4th param if present and numeric)
            height = None
            if len(params) > 3 and params[3] is not None:
                try:
                    height = int(params[3])
                except (ValueError, TypeError):
                    pass

            # Seed hash is optional (5th param if present)
            seed_hash = None
            if len(params) > 4 and params[4] is not None:
                seed_hash = str(params[4])

            # Clean jobs - check last param if it's a boolean
            clean_jobs = False
            if params and isinstance(params[-1], bool):
                clean_jobs = params[-1]

            job_info = JobInfo(
                job_id=job_id,
                clean_jobs=clean_jobs,
                height=height,
                blob=blob,
                target_hex=target_hex,
                seed_hash=seed_hash,
            )
            self.add_job(job_info, source_server)
        except (ValueError, TypeError) as e:
            logger.warning(f"[{self.session_id}] Error parsing RandomX job params: {e}")

    def add_job_from_randomx(self, job_data: dict, source_server: Optional[str] = None) -> None:
        """
        Parse and add a job from RandomX pool 'job' notification.

        RandomX job format is a dict with:
        - job_id: Job identifier
        - blob: Block template blob (hex)
        - target: Mining target (hex string)
        - seed_hash: RandomX seed hash (optional)
        - height: Block height (optional)
        - algo: Algorithm identifier (optional, e.g., "rx/0")

        Args:
            job_data: Dict containing RandomX job fields.
            source_server: Optional server name that issued this job.
        """
        if not isinstance(job_data, dict):
            logger.warning(f"[{self.session_id}] Invalid RandomX job data type: {type(job_data)}")
            return

        try:
            job_id = str(job_data.get("job_id", ""))
            if not job_id:
                logger.warning(f"[{self.session_id}] RandomX job missing job_id")
                return

            blob = str(job_data.get("blob", ""))
            target_hex = str(job_data.get("target", ""))
            seed_hash = job_data.get("seed_hash")
            if seed_hash:
                seed_hash = str(seed_hash)

            # Height is optional
            height = None
            if job_data.get("height") is not None:
                try:
                    height = int(job_data["height"])
                except (ValueError, TypeError):
                    pass

            # Clean jobs - some pools set this explicitly, default False
            clean_jobs = bool(job_data.get("clean_jobs", False))

            job_info = JobInfo(
                job_id=job_id,
                clean_jobs=clean_jobs,
                height=height,
                blob=blob,
                target_hex=target_hex,
                seed_hash=seed_hash,
            )
            self.add_job(job_info, source_server)

            logger.debug(
                f"[{self.session_id}] Added RandomX job: id={job_id}, "
                f"algo={job_data.get('algo', 'rx/0')}, clean={clean_jobs}"
            )
        except Exception as e:
            logger.warning(f"[{self.session_id}] Error parsing RandomX job: {e}")

    def _add_generic_job(self, params: list, source_server: Optional[str] = None) -> None:
        """
        Parse generic mining.notify params when algorithm is unknown or unsupported.

        Extracts job_id (first param) and clean_jobs (last param if boolean).
        """
        if len(params) < 2:
            logger.warning(f"[{self.session_id}] Invalid mining.notify params (too few): {params}")
            return

        # First param is always job_id
        job_id = str(params[0])

        # Last param is usually clean_jobs (boolean)
        clean_jobs = bool(params[-1]) if isinstance(params[-1], bool) else False

        job_info = JobInfo(
            job_id=job_id,
            clean_jobs=clean_jobs,
        )
        self.add_job(job_info, source_server)

    def _clean_job_sources(self) -> None:
        """Remove expired job sources based on TTL."""
        now = time.time()
        expired = []
        for job_id, (_, timestamp) in self._job_sources.items():
            if now - timestamp > JOB_SOURCE_TTL:
                expired.append(job_id)
            else:
                # OrderedDict is ordered by insertion, so we can stop early
                break

        for job_id in expired:
            del self._job_sources[job_id]

        if expired:
            logger.debug(
                f"[{self.session_id}] Cleaned {len(expired)} expired job sources "
                f"(remaining: {len(self._job_sources)})"
            )

    def get_job_source(self, job_id: str) -> Optional[str]:
        """
        Look up which server issued a job.

        Used during grace period to route shares to the correct pool.
        This lookup persists across cache clears because _job_sources
        is maintained separately from _jobs.

        Args:
            job_id: The job ID to look up.

        Returns:
            Server name that issued the job, or None if unknown/expired.
        """
        entry = self._job_sources.get(job_id)
        if entry is None:
            return None
        server, timestamp = entry
        # Check TTL
        if time.time() - timestamp > JOB_SOURCE_TTL:
            return None
        return server

    def get_current_zksnark_target(self) -> Optional[int]:
        """
        Get the target from the current zkSNARK job.

        ALEO pools embed difficulty target in job notifications. This method
        returns that target for use in logging and diagnostics.

        Returns:
            Target value from current job, or None if no job or not zkSNARK.
        """
        if self._algorithm != "zksnark":
            return None
        if self._current_job_id is None:
            return None
        job = self._jobs.get(self._current_job_id)
        if job is None:
            return None
        return job.target

    def get_job_target(self, job_id: str) -> Optional[int]:
        """
        Get the target from a specific job (zkSNARK only).

        Args:
            job_id: Job ID to look up.

        Returns:
            Target value from job, or None if not found or not zkSNARK.
        """
        if self._algorithm != "zksnark":
            return None
        job = self._jobs.get(job_id)
        if job is None:
            return None
        return job.target

    def validate_share(
        self,
        job_id: str,
        extranonce2: str,
        ntime: str,
        nonce: str,
        version_bits: Optional[str] = None,
    ) -> tuple[bool, Optional[str]]:
        """
        Validate a share before submission.

        Args:
            job_id: Job ID.
            extranonce2: Extranonce2 value.
            ntime: Block time.
            nonce: Block nonce.
            version_bits: Version bits for version-rolling.

        Returns:
            Tuple of (valid, error_reason).
        """
        # Clean expired shares from cache (throttled to avoid overhead at high share rates)
        now = time.time()
        if now - self._last_cache_cleanup > self._cache_cleanup_interval:
            self._clean_share_cache()
            self._last_cache_cleanup = now

        # Validate extranonce2 length if we know the expected size
        if self._extranonce2_size is not None:
            expected_hex_len = self._extranonce2_size * 2  # bytes to hex chars
            if len(extranonce2) != expected_hex_len:
                self.invalid_format_rejected += 1
                return False, (
                    f"Invalid extranonce2 length: expected {expected_hex_len} "
                    f"hex chars, got {len(extranonce2)}"
                )

        # Check for duplicate (include version_bits for version-rolling miners)
        if self._reject_duplicates:
            share_key = ShareKey(job_id, extranonce2, ntime, nonce, version_bits)
            if share_key in self._recent_shares:
                self.duplicates_rejected += 1
                return False, f"Duplicate share (job={job_id}, nonce={nonce})"

        # Check for stale job (only if we have received at least one job)
        if self._reject_stale and self._jobs and job_id not in self._jobs:
            self.stale_rejected += 1
            return False, f"Stale job (job={job_id})"

        # Share passes local validation
        # NOTE: Do NOT add to cache here - only cache after pool accepts
        # This allows legitimate retries if the pool rejects the share
        return True, None

    def record_accepted_share(
        self,
        job_id: str,
        extranonce2: str,
        ntime: str,
        nonce: str,
        version_bits: Optional[str] = None,
    ) -> None:
        """
        Record a share that was accepted by the upstream pool.

        Call this ONLY after the pool confirms acceptance. This prevents
        blocking legitimate retries when the pool rejects a share.

        Args:
            job_id: Job ID.
            extranonce2: Extranonce2 value.
            ntime: Block time.
            nonce: Block nonce.
            version_bits: Version bits for version-rolling.
        """
        if not self._reject_duplicates:
            return

        share_key = ShareKey(job_id, extranonce2, ntime, nonce, version_bits)
        self._recent_shares[share_key] = time.time()

        # Maintain cache size
        while len(self._recent_shares) > self._max_share_cache:
            self._recent_shares.popitem(last=False)

    def validate_zksnark_share(
        self,
        job_id: str,
        nonce: str,
    ) -> tuple[bool, Optional[str]]:
        """
        Validate a zkSNARK/ALEO share before submission.

        ALEO miners use the pool's job ID directly in share submissions,
        so we can track stale jobs and reject them locally.

        Args:
            job_id: Job ID (pool's job ID, used directly by miner).
            nonce: ALEO nonce value.

        Returns:
            Tuple of (valid, error_reason).
        """
        # Clean expired shares from cache (throttled)
        now = time.time()
        if now - self._last_cache_cleanup > self._cache_cleanup_interval:
            self._clean_share_cache()
            self._last_cache_cleanup = now

        # Check for duplicate (still useful - uses miner's job ID)
        if self._reject_duplicates:
            share_key = SimpleShareKey(job_id, nonce)
            if share_key in self._recent_shares:
                self.duplicates_rejected += 1
                return False, f"Duplicate share (job={job_id})"

        # Check if this job was already rejected as stale/unknown-work
        # This prevents flooding the pool with shares for jobs it no longer recognizes
        if job_id in self._stale_jobs:
            self.stale_rejected += 1
            return False, f"Stale job (already rejected by pool)"

        # Record share BEFORE submission to prevent duplicate submissions in same batch
        # This is critical for high-speed miners that may send many shares at once
        if self._reject_duplicates:
            share_key = SimpleShareKey(job_id, nonce)
            self._recent_shares[share_key] = time.time()
            # Maintain cache size
            while len(self._recent_shares) > self._max_share_cache:
                self._recent_shares.popitem(last=False)

        # Share passes local validation
        return True, None

    def record_accepted_zksnark_share(
        self,
        job_id: str,
        nonce: str,
    ) -> None:
        """
        Record a zkSNARK/ALEO share that was accepted by the upstream pool.

        Args:
            job_id: Job ID.
            nonce: ALEO nonce value.
        """
        if not self._reject_duplicates:
            return

        share_key = SimpleShareKey(job_id, nonce)
        self._recent_shares[share_key] = time.time()

        # Maintain cache size
        while len(self._recent_shares) > self._max_share_cache:
            self._recent_shares.popitem(last=False)

    def mark_job_stale(self, job_id: str) -> None:
        """
        Mark a job as stale (rejected by pool with unknown-work).

        Subsequent shares for this job will be rejected locally without
        forwarding to the pool. This prevents flooding the pool with
        shares for jobs it no longer recognizes.

        Args:
            job_id: Job ID that was rejected as unknown-work.
        """
        # Add with timestamp (dict maintains insertion order in Python 3.7+)
        self._stale_jobs[job_id] = time.time()
        # FIFO eviction - keep only the most recent 100 stale jobs
        while len(self._stale_jobs) > 100:
            # Remove oldest entry (first inserted)
            oldest_job = next(iter(self._stale_jobs))
            del self._stale_jobs[oldest_job]
        logger.debug(
            f"[{self.session_id}] Marked job {job_id} as stale "
            f"(total stale jobs: {len(self._stale_jobs)})"
        )

    def clear_stale_jobs(self) -> None:
        """
        Clear all stale job markers.

        NOTE: This is no longer called automatically on new job arrival.
        Stale markers now persist and expire via FIFO eviction to prevent
        the miner from flooding the pool with shares for old stale jobs.
        """
        if self._stale_jobs:
            count = len(self._stale_jobs)
            self._stale_jobs.clear()
            logger.debug(f"[{self.session_id}] Cleared {count} stale job markers")

    def validate_randomx_share(
        self,
        job_id: str,
        nonce: str,
    ) -> tuple[bool, Optional[str]]:
        """
        Validate a RandomX share before submission.

        Args:
            job_id: Job ID.
            nonce: Nonce value (4 bytes as hex string).

        Returns:
            Tuple of (valid, error_reason).
        """
        # Clean expired shares from cache (throttled)
        now = time.time()
        if now - self._last_cache_cleanup > self._cache_cleanup_interval:
            self._clean_share_cache()
            self._last_cache_cleanup = now

        # Check for duplicate
        if self._reject_duplicates:
            share_key = SimpleShareKey(job_id, nonce)
            if share_key in self._recent_shares:
                self.duplicates_rejected += 1
                return False, f"Duplicate share (job={job_id})"

        # Check for stale job (only if we have received at least one job)
        if self._reject_stale and self._jobs and job_id not in self._jobs:
            self.stale_rejected += 1
            return False, f"Stale job (job={job_id})"

        # Share passes local validation
        return True, None

    def record_accepted_randomx_share(
        self,
        job_id: str,
        nonce: str,
    ) -> None:
        """
        Record a RandomX share that was accepted by the upstream pool.

        Args:
            job_id: Job ID.
            nonce: Nonce value (4 bytes as hex string).
        """
        if not self._reject_duplicates:
            return

        share_key = SimpleShareKey(job_id, nonce)
        self._recent_shares[share_key] = time.time()

        # Maintain cache size
        while len(self._recent_shares) > self._max_share_cache:
            self._recent_shares.popitem(last=False)

    def _clean_share_cache(self) -> None:
        """Remove expired shares from the cache."""
        now = time.time()
        expired = []
        for key, timestamp in self._recent_shares.items():
            if now - timestamp > self._share_cache_ttl:
                expired.append(key)
            else:
                # OrderedDict is ordered by insertion, so we can stop early
                break

        for key in expired:
            del self._recent_shares[key]

    def clear_share_cache(self) -> None:
        """
        Clear all cached shares and jobs.

        Call this when switching to a new upstream server. The new pool will have
        different job IDs, and old share data is no longer relevant. Keeping stale
        data could cause false duplicate detection if the new pool happens to reuse
        job IDs or if the miner resubmits previously rejected shares.
        """
        old_share_count = len(self._recent_shares)
        old_job_count = len(self._jobs)

        self._recent_shares.clear()
        self._jobs.clear()
        self._current_job_id = None

        if old_share_count > 0 or old_job_count > 0:
            logger.debug(
                f"[{self.session_id}] Cleared share cache on upstream switch "
                f"(shares={old_share_count}, jobs={old_job_count})"
            )

    def get_stats(self) -> dict:
        """Get validation statistics."""
        return {
            "duplicates_rejected": self.duplicates_rejected,
            "stale_rejected": self.stale_rejected,
            "invalid_format_rejected": self.invalid_format_rejected,
            "cached_shares": len(self._recent_shares),
            "tracked_jobs": len(self._jobs),
            "current_difficulty": self._difficulty,
        }
