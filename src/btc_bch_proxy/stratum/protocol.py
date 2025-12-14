"""Stratum protocol JSON-RPC message parsing and building."""

from __future__ import annotations

import json
from typing import Optional, Union

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
        """
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

        # Response (has id, has result or error, no method)
        if msg_id is not None and (result is not None or error is not None) and method is None:
            return StratumResponse(id=msg_id, result=result, error=error)

        # Request (has id and method)
        if msg_id is not None and method is not None:
            return StratumRequest(id=msg_id, method=method, params=params or [])

        # Notification (has method but no id or null id)
        if method is not None:
            return StratumNotification(method=method, params=params or [])

        raise StratumProtocolError(f"Cannot determine message type: {obj}")

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
        self, id: int, result: any, error: list = None
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
        """
        return json.dumps(obj, separators=(",", ":")).encode(self.ENCODING) + self.DELIMITER

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
        """
        return json.dumps(msg.to_dict(), separators=(",", ":")).encode("utf-8") + b"\n"


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
