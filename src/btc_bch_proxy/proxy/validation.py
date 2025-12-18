"""Share validation and duplicate detection."""

from __future__ import annotations

import hashlib
import struct
import time
from collections import OrderedDict
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Optional

from loguru import logger

from btc_bch_proxy.proxy.constants import JOB_SOURCE_TTL, MAX_MERKLE_BRANCHES

if TYPE_CHECKING:
    from btc_bch_proxy.config.models import ValidationConfig


@dataclass
class JobInfo:
    """Information about a mining job from mining.notify."""

    job_id: str
    prevhash: str
    coinbase1: str
    coinbase2: str
    merkle_branches: list[str]
    version: str
    nbits: str
    ntime: str
    clean_jobs: bool
    received_at: float = field(default_factory=time.time)
    source_server: Optional[str] = None  # Which pool issued this job (for grace period routing)


@dataclass
class ShareKey:
    """Unique identifier for a share submission."""

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


class ShareValidator:
    """
    Validates shares before submission to upstream pool.

    Features:
    - Duplicate share detection
    - Stale job detection
    - Difficulty validation
    - Optional share hash validation

    Thread Safety:
        This class is NOT thread-safe. It is designed to be used from a single
        async task (the MinerSession that owns it). Each MinerSession creates
        its own ShareValidator instance, so no locking is required.

        IMPORTANT: Do NOT share a ShareValidator instance between tasks or call
        its methods concurrently. The internal OrderedDict structures are not
        protected by locks and concurrent access will cause race conditions.
        If you need thread-safe validation, add asyncio.Lock protection.
    """

    def __init__(self, session_id: str, config: Optional[ValidationConfig] = None):
        """
        Initialize the share validator.

        Args:
            session_id: Session identifier for logging.
            config: Validation configuration.
        """
        self.session_id = session_id

        # Configuration (use defaults if not provided)
        self._reject_duplicates = config.reject_duplicates if config else True
        self._reject_stale = config.reject_stale if config else True
        self._validate_difficulty = config.validate_difficulty if config else False
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

        # Current difficulty
        self._difficulty: float = 1.0

        # Expected extranonce2 size (in bytes, set by pool)
        self._extranonce2_size: Optional[int] = None

        # Statistics
        self.duplicates_rejected: int = 0
        self.stale_rejected: int = 0
        self.over_target_rejected: int = 0
        self.invalid_format_rejected: int = 0

        # Cache cleanup throttling (cleanup at most once per 60 seconds)
        self._last_cache_cleanup: float = 0.0
        self._cache_cleanup_interval: float = 60.0

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
            # Keep only the current job
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
        if len(params) < 9:
            logger.warning(f"[{self.session_id}] Invalid mining.notify params: {params}")
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
            prevhash=str(params[1]),
            coinbase1=str(params[2]),
            coinbase2=str(params[3]),
            merkle_branches=merkle_branches,
            version=str(params[5]),
            nbits=str(params[6]),
            ntime=str(params[7]),
            clean_jobs=bool(params[8]),  # len(params) >= 9 guaranteed by check above
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

    def validate_share(
        self,
        job_id: str,
        extranonce2: str,
        ntime: str,
        nonce: str,
        extranonce1: Optional[str] = None,
        version_bits: Optional[str] = None,
    ) -> tuple[bool, Optional[str]]:
        """
        Validate a share before submission.

        Args:
            job_id: Job ID.
            extranonce2: Extranonce2 value.
            ntime: Block time.
            nonce: Block nonce.
            extranonce1: Extranonce1 from pool (for hash validation).
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

        # Optionally validate share hash meets difficulty
        if self._validate_difficulty and extranonce1:
            job = self._jobs.get(job_id)
            if job:
                valid, reason = self._validate_share_hash(
                    job, extranonce1, extranonce2, ntime, nonce, version_bits
                )
                if not valid:
                    return False, reason

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

    # Maximum values for 32-bit hex fields
    MAX_UINT32 = 0xFFFFFFFF

    def _validate_share_hash(
        self,
        job: JobInfo,
        extranonce1: str,
        extranonce2: str,
        ntime: str,
        nonce: str,
        version_bits: Optional[str] = None,
    ) -> tuple[bool, Optional[str]]:
        """
        Validate that the share hash meets the difficulty target.

        Args:
            job: Job information.
            extranonce1: Extranonce1 from pool.
            extranonce2: Extranonce2 from miner.
            ntime: Block time from miner.
            nonce: Nonce from miner.
            version_bits: Version bits for version-rolling.

        Returns:
            Tuple of (valid, error_reason).
        """
        try:
            # Validate hex string lengths to prevent overflow
            # These should all be 8-character hex strings (32-bit values)
            for name, value in [
                ("ntime", ntime),
                ("nonce", nonce),
                ("version", job.version),
                ("nbits", job.nbits),
            ]:
                if len(value) > 8:
                    return False, f"Invalid {name}: too long (max 8 hex chars)"

            # Calculate coinbase
            coinbase = bytes.fromhex(job.coinbase1 + extranonce1 + extranonce2 + job.coinbase2)
            coinbase_hash = sha256d(coinbase)

            # Calculate merkle root
            merkle_root = coinbase_hash
            for branch in job.merkle_branches:
                merkle_root = sha256d(merkle_root + bytes.fromhex(branch))

            # Build block header - parse and validate 32-bit values
            version = int(job.version, 16)
            if version > self.MAX_UINT32:
                return False, f"Invalid version: {job.version} exceeds uint32"

            if version_bits:
                # Apply version-rolling bits with bounds check
                if len(version_bits) > 8:
                    return False, f"Invalid version_bits: too long (max 8 hex chars)"
                version_mask = int(version_bits, 16)
                if version_mask > self.MAX_UINT32:
                    return False, f"Invalid version_bits: exceeds uint32"
                version = (version & 0xe0000000) | (version_mask & 0x1fffffff)

            # Stratum prevhash is 8x4-byte words, each word byte-swapped
            # Convert back to normal byte order for block header
            prevhash_bytes = swap_endian_words(bytes.fromhex(job.prevhash))

            # Parse and validate ntime and nonce
            ntime_int = int(ntime, 16)
            nonce_int = int(nonce, 16)
            nbits_int = int(job.nbits, 16)

            if ntime_int > self.MAX_UINT32:
                return False, f"Invalid ntime: {ntime} exceeds uint32"
            if nonce_int > self.MAX_UINT32:
                return False, f"Invalid nonce: {nonce} exceeds uint32"
            if nbits_int > self.MAX_UINT32:
                return False, f"Invalid nbits: {job.nbits} exceeds uint32"

            header = struct.pack("<I", version)  # Version (little-endian)
            header += prevhash_bytes  # Previous block hash (already in correct order)
            header += merkle_root  # Merkle root (already in correct order)
            header += struct.pack("<I", ntime_int)  # Timestamp
            header += struct.pack("<I", nbits_int)  # Bits (difficulty)
            header += struct.pack("<I", nonce_int)  # Nonce

            # Calculate block hash
            block_hash = sha256d(header)
            hash_int = int.from_bytes(block_hash, byteorder="little")

            # Calculate target from difficulty
            target = difficulty_to_target(self._difficulty)

            if hash_int > target:
                self.over_target_rejected += 1
                logger.warning(
                    f"[{self.session_id}] Share over target: "
                    f"hash={block_hash.hex()}, diff={self._difficulty}"
                )
                return False, "Share does not meet difficulty target"

            return True, None

        except ValueError as e:
            # Invalid hex/int conversion - data format issue
            # Fail-closed: reject malformed shares to prevent DoS/invalid submissions
            self.invalid_format_rejected += 1
            logger.warning(
                f"[{self.session_id}] Hash validation format error (rejecting share): {e}"
            )
            return False, f"Invalid share format: {e}"
        except struct.error as e:
            # Struct packing error - data size issue
            self.invalid_format_rejected += 1
            logger.warning(
                f"[{self.session_id}] Hash validation struct error (rejecting share): {e}"
            )
            return False, f"Invalid share data size: {e}"
        except (TypeError, KeyError, IndexError) as e:
            # Unexpected data type or missing data
            self.invalid_format_rejected += 1
            logger.warning(
                f"[{self.session_id}] Hash validation data error (rejecting share): "
                f"{type(e).__name__}: {e}"
            )
            return False, f"Invalid share data: {type(e).__name__}"

    def get_stats(self) -> dict:
        """Get validation statistics."""
        return {
            "duplicates_rejected": self.duplicates_rejected,
            "stale_rejected": self.stale_rejected,
            "over_target_rejected": self.over_target_rejected,
            "invalid_format_rejected": self.invalid_format_rejected,
            "cached_shares": len(self._recent_shares),
            "tracked_jobs": len(self._jobs),
            "current_difficulty": self._difficulty,
        }


# SHA-256 difficulty 1 target (used for difficulty <-> target conversion)
# This is the standard target used by Bitcoin, Bitcoin Cash, DigiByte, and
# other SHA-256 based cryptocurrencies for stratum mining difficulty calculation.
MAX_TARGET = 0x00000000FFFF0000000000000000000000000000000000000000000000000000


def sha256d(data: bytes) -> bytes:
    """Double SHA256 hash."""
    return hashlib.sha256(hashlib.sha256(data).digest()).digest()


def swap_endian_words(data: bytes, word_size: int = 4) -> bytes:
    """
    Swap byte order within each word of the data.

    Stratum sends prevhash as 8x4-byte words with bytes swapped within each word.
    This function reverses that transformation.

    Args:
        data: Input bytes.
        word_size: Size of each word (default 4 bytes).

    Returns:
        Data with byte order swapped within each word.
    """
    result = b""
    for i in range(0, len(data), word_size):
        word = data[i:i + word_size]
        result += word[::-1]
    return result


def difficulty_to_target(difficulty: float) -> int:
    """
    Convert difficulty to target value.

    The target is the maximum hash value that a share must be below.

    Args:
        difficulty: Mining difficulty (must be positive).

    Returns:
        Target value as integer.

    Raises:
        ValueError: If difficulty is zero or negative.
    """
    if difficulty <= 0:
        raise ValueError(f"Difficulty must be positive, got {difficulty}")
    return int(MAX_TARGET / difficulty)


def target_to_difficulty(target: int) -> float:
    """Convert target to difficulty."""
    return MAX_TARGET / target if target > 0 else float("inf")
