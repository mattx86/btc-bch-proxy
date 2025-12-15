"""Share validation and duplicate detection."""

from __future__ import annotations

import hashlib
import struct
import time
from collections import OrderedDict
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Optional

from loguru import logger

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
        self._recent_shares: OrderedDict[ShareKey, float] = OrderedDict()

        # Current and recent jobs
        self._jobs: OrderedDict[str, JobInfo] = OrderedDict()
        self._current_job_id: Optional[str] = None

        # Current difficulty
        self._difficulty: float = 1.0

        # Statistics
        self.duplicates_rejected: int = 0
        self.stale_rejected: int = 0
        self.low_diff_rejected: int = 0
        self.over_target_rejected: int = 0

    def set_difficulty(self, difficulty: float) -> None:
        """
        Update the current difficulty.

        Args:
            difficulty: New difficulty value.
        """
        if difficulty > 0:
            self._difficulty = difficulty
            logger.debug(f"[{self.session_id}] Difficulty set to {difficulty}")

    def add_job(self, job_info: JobInfo) -> None:
        """
        Add a new job from mining.notify.

        Args:
            job_info: Job information.
        """
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
        else:
            # Limit job cache size
            while len(self._jobs) > self._max_job_cache:
                self._jobs.popitem(last=False)

        logger.debug(
            f"[{self.session_id}] New job {job_info.job_id} "
            f"(clean={job_info.clean_jobs}, total jobs={len(self._jobs)})"
        )

    def add_job_from_notify(self, params: list) -> None:
        """
        Parse and add a job from mining.notify parameters.

        Args:
            params: mining.notify parameters.
        """
        if len(params) < 9:
            logger.warning(f"[{self.session_id}] Invalid mining.notify params: {params}")
            return

        job_info = JobInfo(
            job_id=str(params[0]),
            prevhash=str(params[1]),
            coinbase1=str(params[2]),
            coinbase2=str(params[3]),
            merkle_branches=[str(b) for b in params[4]] if isinstance(params[4], list) else [],
            version=str(params[5]),
            nbits=str(params[6]),
            ntime=str(params[7]),
            clean_jobs=bool(params[8]) if len(params) > 8 else False,
        )
        self.add_job(job_info)

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
        # Clean expired shares from cache
        self._clean_share_cache()

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
            # Calculate coinbase
            coinbase = bytes.fromhex(job.coinbase1 + extranonce1 + extranonce2 + job.coinbase2)
            coinbase_hash = sha256d(coinbase)

            # Calculate merkle root
            merkle_root = coinbase_hash
            for branch in job.merkle_branches:
                merkle_root = sha256d(merkle_root + bytes.fromhex(branch))

            # Build block header
            version = int(job.version, 16)
            if version_bits:
                # Apply version-rolling bits
                version_mask = int(version_bits, 16)
                version = (version & 0xe0000000) | (version_mask & 0x1fffffff)

            # Stratum prevhash is 8x4-byte words, each word byte-swapped
            # Convert back to normal byte order for block header
            prevhash_bytes = swap_endian_words(bytes.fromhex(job.prevhash))

            header = struct.pack("<I", version)  # Version (little-endian)
            header += prevhash_bytes  # Previous block hash (already in correct order)
            header += merkle_root  # Merkle root (already in correct order)
            header += struct.pack("<I", int(ntime, 16))  # Timestamp
            header += struct.pack("<I", int(job.nbits, 16))  # Bits (difficulty)
            header += struct.pack("<I", int(nonce, 16))  # Nonce

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

        except Exception as e:
            logger.debug(f"[{self.session_id}] Hash validation error: {e}")
            # Don't reject on validation errors - let the pool decide
            return True, None

    def get_stats(self) -> dict:
        """Get validation statistics."""
        return {
            "duplicates_rejected": self.duplicates_rejected,
            "stale_rejected": self.stale_rejected,
            "low_diff_rejected": self.low_diff_rejected,
            "over_target_rejected": self.over_target_rejected,
            "cached_shares": len(self._recent_shares),
            "tracked_jobs": len(self._jobs),
            "current_difficulty": self._difficulty,
        }


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
    """
    # Bitcoin difficulty 1 target
    MAX_TARGET = 0x00000000FFFF0000000000000000000000000000000000000000000000000000
    return int(MAX_TARGET / difficulty)


def target_to_difficulty(target: int) -> float:
    """Convert target to difficulty."""
    MAX_TARGET = 0x00000000FFFF0000000000000000000000000000000000000000000000000000
    return MAX_TARGET / target if target > 0 else float("inf")
