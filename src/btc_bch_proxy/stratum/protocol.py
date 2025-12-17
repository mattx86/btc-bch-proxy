"""Stratum protocol JSON-RPC message parsing and building."""

from __future__ import annotations

import json
from typing import Any, Optional, Union

from loguru import logger

from btc_bch_proxy.stratum.messages import (
    StratumMessage,
    StratumNotification,
    StratumRequest,
    StratumResponse,
)


class StratumProtocolError(Exception):
    """Error in stratum protocol handling."""

    pass


class StratumProtocol:
    """
    Handles parsing and building of Stratum JSON-RPC messages.

    Stratum uses newline-delimited JSON messages over TCP.
    """

    ENCODING = "utf-8"
    DELIMITER = b"\n"
    MAX_BUFFER_SIZE = 1024 * 1024  # 1MB max buffer to prevent DoS
    MAX_PARAMS_LIST_SIZE = 100  # Max size for params list conversion (DoS protection)
    MAX_RESULT_LIST_SIZE = 1000  # Max size for result arrays (DoS protection)
    MAX_RESULT_STRING_SIZE = 65536  # Max size for result strings (64KB)

    def __init__(self):
        """Initialize the protocol handler."""
        self._buffer = b""

    def feed_data(self, data: bytes) -> list[StratumMessage]:
        """
        Feed raw data into the buffer and extract complete messages.

        Args:
            data: Raw bytes received from the socket.

        Returns:
            List of parsed stratum messages.

        Raises:
            StratumProtocolError: If buffer exceeds max size (DoS protection).
        """
        # DoS protection: check size BEFORE accumulating to prevent memory exhaustion
        if len(self._buffer) + len(data) > self.MAX_BUFFER_SIZE:
            self._buffer = b""
            raise StratumProtocolError(
                f"Buffer would exceed max size ({self.MAX_BUFFER_SIZE} bytes), dropping data"
            )

        self._buffer += data

        messages = []

        while self.DELIMITER in self._buffer:
            line, self._buffer = self._buffer.split(self.DELIMITER, 1)
            if line:
                try:
                    msg = self.parse_message(line)
                    if msg:
                        messages.append(msg)
                except StratumProtocolError as e:
                    logger.warning(f"Failed to parse message: {e}")
                    # Continue processing remaining messages

        return messages

    def parse_message(self, data: bytes) -> Optional[StratumMessage]:
        """
        Parse a single JSON-RPC message.

        Args:
            data: Raw bytes of a single message (without newline).

        Returns:
            Parsed stratum message or None if invalid.

        Raises:
            StratumProtocolError: If the message cannot be parsed.
        """
        try:
            text = data.decode(self.ENCODING).strip()
            if not text:
                return None

            obj = json.loads(text)
        except (UnicodeDecodeError, json.JSONDecodeError) as e:
            raise StratumProtocolError(f"Invalid JSON: {e}") from e

        if not isinstance(obj, dict):
            raise StratumProtocolError(f"Expected JSON object, got {type(obj).__name__}")

        return self._parse_object(obj)

    def _parse_object(self, obj: dict) -> StratumMessage:
        """
        Parse a JSON object into a stratum message.

        Args:
            obj: Parsed JSON dictionary.

        Returns:
            Typed stratum message.
        """
        msg_id = obj.get("id")
        method = obj.get("method")
        params = obj.get("params", [])
        result = obj.get("result")
        error = obj.get("error")
        # Non-standard field used by some pools for share rejection reasons
        reject_reason = obj.get("reject-reason")

        # DoS protection: limit result field size
        result = self._limit_result_size(result)

        # Ensure params is always a list (some miners/pools send dict or other types)
        if not isinstance(params, list):
            if isinstance(params, dict):
                # Convert dict with numeric keys to list: {"0": a, "1": b} -> [a, b]
                try:
                    max_key = max(int(k) for k in params.keys()) + 1
                    # DoS protection: limit max key to prevent huge sparse list allocation
                    # Also check for sparse arrays: if we'd create many None entries, use values instead
                    num_entries = len(params)
                    is_sparse = num_entries < max_key // 2  # More than half would be None
                    if max_key > self.MAX_PARAMS_LIST_SIZE or is_sparse:
                        if max_key > self.MAX_PARAMS_LIST_SIZE:
                            logger.warning(
                                f"Params dict max key {max_key} exceeds limit "
                                f"{self.MAX_PARAMS_LIST_SIZE}, using values only"
                            )
                        else:
                            logger.debug(
                                f"Sparse params dict detected ({num_entries}/{max_key}), "
                                f"using values only"
                            )
                        params = list(params.values())[:self.MAX_PARAMS_LIST_SIZE]
                    else:
                        params = [params.get(str(i), params.get(i)) for i in range(max_key)]
                except (ValueError, TypeError):
                    # Keys aren't numeric, just use values in arbitrary order
                    params = list(params.values())[:self.MAX_PARAMS_LIST_SIZE]
            else:
                params = [params] if params is not None else []

        # Response (has id, has result or error, no method)
        if msg_id is not None and (result is not None or error is not None) and method is None:
            return StratumResponse(id=msg_id, result=result, error=error, reject_reason=reject_reason)

        # Request (has id and method)
        if msg_id is not None and method is not None:
            return StratumRequest(id=msg_id, method=method, params=params or [])

        # Notification (has method but no id or null id)
        if method is not None:
            return StratumNotification(method=method, params=params or [])

        raise StratumProtocolError(f"Cannot determine message type: {obj}")

    def _limit_result_size(self, result: Any) -> Any:
        """
        Limit the size of result fields to prevent memory exhaustion.

        This protects against malicious pools sending huge result arrays
        or excessively long strings in responses.

        Args:
            result: The result value from a response.

        Returns:
            Size-limited result value.
        """
        if result is None:
            return None

        if isinstance(result, list):
            if len(result) > self.MAX_RESULT_LIST_SIZE:
                logger.warning(
                    f"Result list size {len(result)} exceeds limit "
                    f"{self.MAX_RESULT_LIST_SIZE}, truncating"
                )
                return result[:self.MAX_RESULT_LIST_SIZE]
        elif isinstance(result, str):
            if len(result) > self.MAX_RESULT_STRING_SIZE:
                logger.warning(
                    f"Result string size {len(result)} exceeds limit "
                    f"{self.MAX_RESULT_STRING_SIZE}, truncating"
                )
                return result[:self.MAX_RESULT_STRING_SIZE]

        return result

    def build_request(self, id: int, method: str, params: list = None) -> bytes:
        """
        Build a stratum request message.

        Args:
            id: Request ID.
            method: Method name.
            params: Method parameters.

        Returns:
            Encoded message bytes with newline delimiter.
        """
        msg = StratumRequest(id=id, method=method, params=params or [])
        return self._encode(msg.to_dict())

    def build_response(
        self, id: int, result: Any, error: list = None
    ) -> bytes:
        """
        Build a stratum response message.

        Args:
            id: Request ID being responded to.
            result: Result value.
            error: Error list [code, message, traceback] or None.

        Returns:
            Encoded message bytes with newline delimiter.
        """
        msg = StratumResponse(id=id, result=result, error=error)
        return self._encode(msg.to_dict())

    def build_notification(self, method: str, params: list = None) -> bytes:
        """
        Build a stratum notification message.

        Args:
            method: Method name.
            params: Method parameters.

        Returns:
            Encoded message bytes with newline delimiter.
        """
        msg = StratumNotification(method=method, params=params or [])
        return self._encode(msg.to_dict())

    def _encode(self, obj: dict) -> bytes:
        """
        Encode a dictionary to JSON bytes with newline.

        Args:
            obj: Dictionary to encode.

        Returns:
            JSON bytes with newline delimiter.

        Raises:
            StratumProtocolError: If encoding fails.
        """
        try:
            return json.dumps(obj, separators=(",", ":")).encode(self.ENCODING) + self.DELIMITER
        except (TypeError, ValueError) as e:
            raise StratumProtocolError(f"Failed to encode message: {e}") from e

    def reset_buffer(self) -> None:
        """Clear the internal buffer."""
        self._buffer = b""

    @staticmethod
    def encode_message(msg: StratumMessage) -> bytes:
        """
        Encode any stratum message to bytes.

        Args:
            msg: Stratum message to encode.

        Returns:
            Encoded message bytes with newline delimiter.

        Raises:
            StratumProtocolError: If encoding fails.
        """
        try:
            return json.dumps(msg.to_dict(), separators=(",", ":")).encode("utf-8") + b"\n"
        except (TypeError, ValueError) as e:
            raise StratumProtocolError(f"Failed to encode message: {e}") from e


def parse_raw_message(data: Union[bytes, str]) -> Optional[StratumMessage]:
    """
    Convenience function to parse a single raw message.

    Args:
        data: Raw message data (bytes or string).

    Returns:
        Parsed message or None.
    """
    protocol = StratumProtocol()
    if isinstance(data, str):
        data = data.encode("utf-8")
    return protocol.parse_message(data)
