"""Error hierarchy and machine-readable rejection reasons.

Every rejected input is mapped to one of the :class:`RejectReason` values so
that callers can distinguish ordinary duplicates, conflicting duplicates,
late events, schema problems, backpressure and checkpoint regressions.
"""
from __future__ import annotations

from enum import Enum


class RejectReason(str, Enum):
    """Stable, machine-readable reasons for a rejected event/request."""

    # schema / protocol
    SCHEMA_UNSUPPORTED_VERSION = "SCHEMA_UNSUPPORTED_VERSION"
    SCHEMA_MISSING_FIELD = "SCHEMA_MISSING_FIELD"
    SCHEMA_TYPE_MISMATCH = "SCHEMA_TYPE_MISMATCH"
    SCHEMA_INVALID_VALUE = "SCHEMA_INVALID_VALUE"
    SCHEMA_UNKNOWN_FIELD = "SCHEMA_UNKNOWN_FIELD"
    # dedupe
    DUPLICATE_IDENTICAL = "DUPLICATE_IDENTICAL"
    DUPLICATE_CONFLICT = "DUPLICATE_CONFLICT"
    SEQ_REGRESSED = "SEQ_REGRESSED"
    # watermark / ordering
    LATE_BEYOND_WATERMARK = "LATE_BEYOND_WATERMARK"
    # capacity / backpressure
    BACKPRESSURE_DELAYED = "BACKPRESSURE_DELAYED"
    BACKPRESSURE_REJECTED = "BACKPRESSURE_REJECTED"
    # checkpoint / replay
    CHECKPOINT_REGRESSED = "CHECKPOINT_REGRESSED"
    CHECKPOINT_UNKNOWN = "CHECKPOINT_UNKNOWN"
    # generic
    INTERNAL_ERROR = "INTERNAL_ERROR"


class IngestionError(Exception):
    """Base error; carries a stable :class:`RejectReason`."""

    reason: RejectReason = RejectReason.INTERNAL_ERROR

    def __init__(self, message: str, *, reason: RejectReason | None = None) -> None:
        super().__init__(message)
        if reason is not None:
            self.reason = reason


class SchemaError(IngestionError):
    """Payload does not conform to any supported event schema."""


class DuplicateError(IngestionError):
    """Event id seen before.

    ``conflicting=True`` means the content differed from the first occurrence.
    """

    def __init__(self, message: str, *, conflicting: bool) -> None:
        super().__init__(
            message,
            reason=(
                RejectReason.DUPLICATE_CONFLICT
                if conflicting
                else RejectReason.DUPLICATE_IDENTICAL
            ),
        )
        self.conflicting = conflicting


class SequenceError(IngestionError):
    """Source sequence number regressed."""

    reason = RejectReason.SEQ_REGRESSED


class LateEventError(IngestionError):
    """Event time is behind the published watermark for the symbol."""

    reason = RejectReason.LATE_BEYOND_WATERMARK


class BackpressureError(IngestionError):
    """Resource cap exceeded and policy says reject (not delay)."""

    reason = RejectReason.BACKPRESSURE_REJECTED


class CheckpointError(IngestionError):
    """Checkpoint request is invalid (regression / unknown checkpoint)."""
