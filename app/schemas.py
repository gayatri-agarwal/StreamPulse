"""Validated event types used by the library event-processing core."""

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
import math
from typing import Any, Mapping, Optional
from uuid import UUID


class EventType(str, Enum):
    """Actions supported by the library simulation."""

    BOOK_ISSUED = "BOOK_ISSUED"
    BOOK_RETURNED = "BOOK_RETURNED"
    BOOK_RESERVED = "BOOK_RESERVED"


@dataclass(frozen=True)
class LibraryEvent:
    """A validated library action. Timestamps may be datetime or ISO 8601 input."""

    event_id: str
    event_type: EventType
    student_id: str
    book_id: str
    timestamp: datetime
    value: Optional[float] = None
    run_id: Optional[str] = None

    def __post_init__(self) -> None:
        for field_name in ("event_id", "student_id", "book_id"):
            field_value = getattr(self, field_name)
            if not isinstance(field_value, str) or not field_value.strip():
                raise ValueError(f"{field_name} must be a non-empty string")

        try:
            normalized_type = EventType(self.event_type)
        except (TypeError, ValueError) as exc:
            raise ValueError("event_type must be BOOK_ISSUED, BOOK_RETURNED, or BOOK_RESERVED") from exc
        object.__setattr__(self, "event_type", normalized_type)

        parsed_timestamp = self.timestamp
        if isinstance(parsed_timestamp, str):
            try:
                parsed_timestamp = datetime.fromisoformat(parsed_timestamp.replace("Z", "+00:00"))
            except ValueError as exc:
                raise ValueError("timestamp must be a valid ISO 8601 timestamp") from exc
        if not isinstance(parsed_timestamp, datetime):
            raise ValueError("timestamp must be a datetime or ISO 8601 string")
        object.__setattr__(self, "timestamp", parsed_timestamp)

        if self.run_id is not None:
            if not isinstance(self.run_id, str) or not self.run_id.strip():
                raise ValueError("run_id must be a non-empty UUID string")
            try:
                normalized_run_id = str(UUID(self.run_id))
            except (TypeError, ValueError, AttributeError) as exc:
                raise ValueError("run_id must be a valid UUID") from exc
            object.__setattr__(self, "run_id", normalized_run_id)

        if self.value is None:
            return
        if isinstance(self.value, bool) or not isinstance(self.value, (int, float)):
            raise ValueError("value must be a finite number")
        if not math.isfinite(self.value):
            raise ValueError("value must be a finite number")
        object.__setattr__(self, "value", float(self.value))

    @classmethod
    def from_mapping(cls, data: Mapping[str, Any]) -> "LibraryEvent":
        """Build an event from a mapping, rejecting missing or extra fields."""
        expected = {"event_id", "event_type", "student_id", "book_id", "timestamp"}
        allowed = expected | {"value", "run_id"}  # Legacy publishers may still send a value; consumers replace it.
        if not isinstance(data, Mapping):
            raise ValueError("event data must be a mapping")
        missing = expected - data.keys()
        extra = data.keys() - allowed
        if missing:
            raise ValueError(f"missing event fields: {', '.join(sorted(missing))}")
        if extra:
            raise ValueError(f"unexpected event fields: {', '.join(sorted(extra))}")
        return cls(**data)
