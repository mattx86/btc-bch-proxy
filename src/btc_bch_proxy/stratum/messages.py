"""Stratum protocol message dataclasses."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, List, Optional, Union


@dataclass
class StratumRequest:
    """A stratum JSON-RPC request from client to server."""

    id: int
    method: str
    params: List[Any] = field(default_factory=list)

    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization."""
        return {
            "id": self.id,
            "method": self.method,
            "params": self.params,
        }


# Type alias for error formats:
# - Stratum style: [code, message, traceback] (List)
# - JSON-RPC 2.0 style: {"code": N, "message": "..."} (Dict)
StratumError = Union[List[Any], dict]


@dataclass
class StratumResponse:
    """A stratum JSON-RPC response from server to client."""

    id: int
    result: Any
    # Error can be Stratum style [code, msg, traceback] or JSON-RPC 2.0 style {code, message}
    error: Optional[StratumError] = None
    # Non-standard reject-reason field used by some pools (e.g., "Above target")
    reject_reason: Optional[str] = None

    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization."""
        return {
            "id": self.id,
            "result": self.result,
            "error": self.error,
        }

    @property
    def is_error(self) -> bool:
        """Check if this is an error response."""
        return self.error is not None

    @property
    def is_rejected(self) -> bool:
        """Check if this is a rejected share (result=False with optional reject_reason)."""
        return self.result is False


@dataclass
class StratumNotification:
    """A stratum JSON-RPC notification (no id, no response expected)."""

    method: str
    params: List[Any] = field(default_factory=list)

    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization."""
        return {
            "id": None,
            "method": self.method,
            "params": self.params,
        }


# Union type for any stratum message
StratumMessage = Union[StratumRequest, StratumResponse, StratumNotification]


# Stratum method constants
class StratumMethods:
    """Constants for stratum method names."""

    # Client -> Server methods
    MINING_SUBSCRIBE = "mining.subscribe"
    MINING_AUTHORIZE = "mining.authorize"
    MINING_SUBMIT = "mining.submit"
    MINING_CONFIGURE = "mining.configure"
    MINING_EXTRANONCE_SUBSCRIBE = "mining.extranonce.subscribe"
    MINING_GET_TRANSACTIONS = "mining.get_transactions"
    MINING_SUGGEST_DIFFICULTY = "mining.suggest_difficulty"

    # Server -> Client notifications
    MINING_NOTIFY = "mining.notify"
    MINING_SET_DIFFICULTY = "mining.set_difficulty"
    MINING_SET_EXTRANONCE = "mining.set_extranonce"

    # Client acknowledgment
    CLIENT_GET_VERSION = "client.get_version"
    CLIENT_RECONNECT = "client.reconnect"
    CLIENT_SHOW_MESSAGE = "client.show_message"


# Stratum error codes
class StratumErrors:
    """Constants for stratum error codes."""

    UNKNOWN = (20, "Unknown error")
    JOB_NOT_FOUND = (21, "Job not found")
    DUPLICATE_SHARE = (22, "Duplicate share")
    LOW_DIFFICULTY = (23, "Low difficulty share")
    UNAUTHORIZED = (24, "Unauthorized worker")
    NOT_SUBSCRIBED = (25, "Not subscribed")

    @classmethod
    def get_error(cls, code: int, message: Optional[str] = None) -> List[Any]:
        """Get an error list for a given code."""
        for name in dir(cls):
            attr = getattr(cls, name)
            if isinstance(attr, tuple) and len(attr) == 2 and attr[0] == code:
                return [code, message or attr[1], None]
        return [code, message or "Unknown error", None]
