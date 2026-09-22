"""Versioned event schema parsing with explicit evolution rules.

Protocol contract
-----------------
Required identity/ordering fields (all versions):

    event_id        str, non-empty  -- stable business id (idempotency key)
    source          str, non-empty  -- feed/source name
    seq             int >= 0        -- per-source monotonic sequence number
    symbol          str, non-empty  -- instrument
    event_time_ms   int >= 0        -- event time (windowing/watermark clock)
    price           number > 0      -- trade/quote price, must be finite
    quantity        number > 0      -- traded quantity, must be finite

Versioning
~~~~~~~~~~
``schema_version`` is optional and defaults to ``v1``.

* **v1**: the fields above.
* **v2**: adds ``notional_ccy`` (default ``"USD"`` when absent/null) and
  ``venue`` (default ``"UNKNOWN"`` when absent/null). Missing new fields on
  older payloads are normalized to the same deterministic defaults, so a
  normalized event is identical no matter which wire version carried it.

Deprecated fields
~~~~~~~~~~~~~~~~~
In **v2** the v1-era alias ``ccy`` is deprecated: when ``notional_ccy`` is
absent it is used as the value and the use is reported in
``ParseResult.deprecated_fields``. If both are present with different values
the payload is rejected (``SCHEMA_INVALID_VALUE``) instead of silently
choosing one.

Unknown fields
~~~~~~~~~~~~~~
Per :class:`UnknownFieldPolicy`: stripped and counted (``IGNORE``, forward
compatibility) or rejected with ``SCHEMA_UNKNOWN_FIELD``. Unknown fields are
*never* silently mapped onto known columns.

Type mismatches are always rejected with ``SCHEMA_TYPE_MISMATCH`` (booleans
are not accepted as numbers; NaN/Inf are rejected as invalid values).
"""
from __future__ import annotations

import logging
import math
from dataclasses import dataclass
from enum import Enum
from typing import Any

from .config import UnknownFieldPolicy
from .errors import RejectReason, SchemaError
from .models import Event

logger = logging.getLogger("ingestion.schema")

REQUIRED_FIELDS: tuple[str, ...] = (
    "event_id",
    "source",
    "seq",
    "symbol",
    "event_time_ms",
    "price",
    "quantity",
)
V2_FIELDS: tuple[str, ...] = ("notional_ccy", "venue")
V2_DEPRECATED_ALIASES = {"ccy": "notional_ccy"}
DEFAULT_CCY = "USD"
DEFAULT_VENUE = "UNKNOWN"


class ParseStatus(str, Enum):
    OK = "OK"
    REJECTED = "REJECTED"


@dataclass(frozen=True, slots=True)
class ParseResult:
    status: ParseStatus
    event: Event | None = None
    reason: RejectReason | None = None
    detail: str = ""
    unknown_fields: tuple[str, ...] = ()
    deprecated_fields: tuple[str, ...] = ()

    @property
    def ok(self) -> bool:
        return self.status is ParseStatus.OK


def _rejected(reason: RejectReason, detail: str,
              payload: Any) -> ParseResult:
    eid = payload.get("event_id") if isinstance(payload, dict) else None
    logger.info(
        "schema reject event_id=%s reason=%s basis=%s", eid, reason.value, detail
    )
    return ParseResult(status=ParseStatus.REJECTED, reason=reason, detail=detail)


def _is_real_number(v: Any) -> bool:
    return isinstance(v, (int, float)) and not isinstance(v, bool)


def _is_nonempty_str(v: Any) -> bool:
    return isinstance(v, str) and v.strip() != ""


@dataclass
class SchemaRegistry:
    supported_versions: tuple[str, ...] = ("v1", "v2")
    unknown_field_policy: UnknownFieldPolicy = UnknownFieldPolicy.IGNORE

    # ------------------------------------------------------------------ parse
    def parse(self, payload: Any) -> ParseResult:
        if not isinstance(payload, dict):
            return _rejected(
                RejectReason.SCHEMA_INVALID_VALUE,
                "payload must be a JSON object",
                {},
            )

        version = payload.get("schema_version", "v1")
        if not isinstance(version, str) or version not in self.supported_versions:
            return _rejected(
                RejectReason.SCHEMA_UNSUPPORTED_VERSION,
                f"unsupported schema_version={version!r}; "
                f"supported={list(self.supported_versions)}",
                payload,
            )

        known = set(REQUIRED_FIELDS) | {"schema_version"}
        if version == "v2":
            known |= set(V2_FIELDS) | set(V2_DEPRECATED_ALIASES)

        unknown = sorted(k for k in payload if k not in known)
        deprecated = tuple(
            sorted(k for k in payload if k in V2_DEPRECATED_ALIASES and version == "v2")
        )
        if unknown and self.unknown_field_policy is UnknownFieldPolicy.REJECT:
            return _rejected(
                RejectReason.SCHEMA_UNKNOWN_FIELD,
                f"unknown fields {unknown} are not allowed by policy REJECT",
                payload,
            )

        data = self._validate_required(payload)
        if isinstance(data, ParseResult):
            return data

        data = self._validate_v2(payload, data, deprecated)
        if isinstance(data, ParseResult):
            return data

        event = Event(schema_version=version, **data)
        logger.info(
            "schema accept event_id=%s event_time_ms=%s version=%s "
            "unknown=%s deprecated=%s basis=fields-present-and-typed",
            event.event_id, event.event_time_ms, version,
            list(unknown), list(deprecated),
        )
        return ParseResult(
            status=ParseStatus.OK,
            event=event,
            unknown_fields=tuple(unknown),
            deprecated_fields=deprecated,
        )

    # ------------------------------------------------------------- validation
    def _validate_required(self, payload: dict[str, Any]) -> dict[str, Any] | ParseResult:
        for name in REQUIRED_FIELDS:
            if name not in payload:
                return _rejected(
                    RejectReason.SCHEMA_MISSING_FIELD,
                    f"required field {name!r} is missing",
                    payload,
                )

        for name in ("event_id", "source", "symbol"):
            if not _is_nonempty_str(payload[name]):
                return _rejected(
                    RejectReason.SCHEMA_TYPE_MISMATCH,
                    f"field {name!r} must be a non-empty string, got "
                    f"{type(payload[name]).__name__}",
                    payload,
                )

        for name in ("seq", "event_time_ms"):
            v = payload[name]
            if not isinstance(v, int) or isinstance(v, bool):
                return _rejected(
                    RejectReason.SCHEMA_TYPE_MISMATCH,
                    f"field {name!r} must be an integer, got {type(v).__name__}",
                    payload,
                )
            if v < 0:
                return _rejected(
                    RejectReason.SCHEMA_INVALID_VALUE,
                    f"field {name!r} must be >= 0, got {v}",
                    payload,
                )

        for name in ("price", "quantity"):
            v = payload[name]
            if not _is_real_number(v):
                return _rejected(
                    RejectReason.SCHEMA_TYPE_MISMATCH,
                    f"field {name!r} must be a number, got {type(v).__name__}",
                    payload,
                )
            if not math.isfinite(v) or v <= 0:
                return _rejected(
                    RejectReason.SCHEMA_INVALID_VALUE,
                    f"field {name!r} must be a finite number > 0, got {v}",
                    payload,
                )

        return {
            "event_id": payload["event_id"].strip(),
            "source": payload["source"].strip(),
            "seq": payload["seq"],
            "symbol": payload["symbol"].strip(),
            "event_time_ms": payload["event_time_ms"],
            "price": float(payload["price"]),
            "quantity": float(payload["quantity"]),
        }

    def _validate_v2(
        self,
        payload: dict[str, Any],
        data: dict[str, Any],
        deprecated: tuple[str, ...],
    ) -> dict[str, Any] | ParseResult:
        # Deterministic defaults apply on every normalized event (also v1),
        # so normalized content is independent of the wire version.
        data["notional_ccy"] = DEFAULT_CCY
        data["venue"] = DEFAULT_VENUE
        if payload.get("schema_version", "v1") != "v2":
            return data

        ccy = payload.get("notional_ccy", None)
        legacy_ccy = payload.get("ccy", None)
        if ccy is None and legacy_ccy is not None:
            ccy = legacy_ccy  # deprecated alias fallback, reported to caller
        if ccy is None:
            ccy = DEFAULT_CCY
        if not _is_nonempty_str(ccy):
            return _rejected(
                RejectReason.SCHEMA_TYPE_MISMATCH,
                f"field 'notional_ccy' must be a non-empty string, got "
                f"{type(ccy).__name__}",
                payload,
            )
        if (
            legacy_ccy is not None
            and payload.get("notional_ccy") is not None
            and legacy_ccy != payload["notional_ccy"]
        ):
            return _rejected(
                RejectReason.SCHEMA_INVALID_VALUE,
                f"deprecated 'ccy'={legacy_ccy!r} conflicts with "
                f"'notional_ccy'={payload['notional_ccy']!r}",
                payload,
            )

        venue = payload.get("venue", None)
        if venue is None:
            venue = DEFAULT_VENUE
        if not _is_nonempty_str(venue):
            return _rejected(
                RejectReason.SCHEMA_TYPE_MISMATCH,
                f"field 'venue' must be a non-empty string, got "
                f"{type(venue).__name__}",
                payload,
            )

        data["notional_ccy"] = ccy.strip()
        data["venue"] = venue.strip()
        return data

    # convenience used by tests / API
    def parse_strict(self, payload: Any) -> Event:
        result = self.parse(payload)
        if not result.ok or result.event is None:
            raise SchemaError(
                result.detail, reason=result.reason or RejectReason.INTERNAL_ERROR
            )
        return result.event
