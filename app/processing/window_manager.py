"""In-memory tumbling and sliding windows, independent of external services."""

from collections import deque
from typing import Deque, List, Optional

from app.processing.metrics import Metrics, calculate_metrics
from app.schemas import LibraryEvent


def _validate_window_size(size: int) -> None:
    if isinstance(size, bool) or not isinstance(size, int) or size <= 0:
        raise ValueError("window size must be a positive integer")


class TumblingWindow:
    """Emit one summary for each complete, fixed-size, non-overlapping group."""

    def __init__(self, size: int) -> None:
        _validate_window_size(size)
        self.size = size
        self._events: List[LibraryEvent] = []

    @property
    def pending_count(self) -> int:
        """Number of events held in the current, possibly incomplete window."""
        return len(self._events)

    def add(self, event: LibraryEvent) -> Optional[Metrics]:
        """Add an event; return metrics only when this window reaches its size."""
        if not isinstance(event, LibraryEvent):
            raise TypeError("event must be a LibraryEvent")
        self._events.append(event)
        if len(self._events) < self.size:
            return None
        completed_metrics = calculate_metrics(self._events)
        self._events.clear()
        return completed_metrics

    def copy(self) -> "TumblingWindow":
        """Return an independent window with the same pending events."""
        duplicate = TumblingWindow(self.size)
        duplicate._events = self._events.copy()
        return duplicate

    def commit_from(self, other: "TumblingWindow") -> None:
        """Commit staged state without replacing this window object."""
        if not isinstance(other, TumblingWindow) or other.size != self.size:
            raise ValueError("staged tumbling window must have the same size")
        self._events = other._events.copy()

    def snapshot(self) -> list[dict]:
        """Return the pending events as JSON-safe values for failover storage."""
        return [_event_snapshot(event) for event in self._events]

    def restore(self, events: list[dict]) -> None:
        """Restore pending events, rejecting snapshots that exceed this window."""
        if not isinstance(events, list) or len(events) >= self.size:
            raise ValueError("tumbling snapshot is invalid for this window size")
        self._events = [_event_from_snapshot(event) for event in events]


class SlidingWindow:
    """Recalculate over the latest N events at the configured event interval.

    Before N events arrive, each result summarizes the available partial window.
    """

    def __init__(self, size: int, interval: int = 1) -> None:
        _validate_window_size(size)
        if isinstance(interval, bool) or not isinstance(interval, int) or interval <= 0:
            raise ValueError("sliding interval must be a positive integer")
        if interval >= size:
            raise ValueError("sliding interval must be smaller than window size")
        self.size = size
        self.interval = interval
        self._events: Deque[LibraryEvent] = deque(maxlen=size)
        self._events_since_update = 0

    @property
    def event_count(self) -> int:
        """Current number of retained events (at most the configured size)."""
        return len(self._events)

    def add(self, event: LibraryEvent) -> Optional[Metrics]:
        """Add an event and return metrics at each configured interval."""
        if not isinstance(event, LibraryEvent):
            raise TypeError("event must be a LibraryEvent")
        self._events.append(event)
        self._events_since_update += 1
        if self._events_since_update < self.interval:
            return None
        self._events_since_update = 0
        return calculate_metrics(self._events)

    def copy(self) -> "SlidingWindow":
        """Return an independent window with the same buffer and interval phase."""
        duplicate = SlidingWindow(self.size, self.interval)
        duplicate._events = deque(self._events, maxlen=self.size)
        duplicate._events_since_update = self._events_since_update
        return duplicate

    def commit_from(self, other: "SlidingWindow") -> None:
        """Commit staged state without replacing this window object."""
        if (
            not isinstance(other, SlidingWindow)
            or other.size != self.size
            or other.interval != self.interval
        ):
            raise ValueError("staged sliding window must have the same size and interval")
        self._events = deque(other._events, maxlen=self.size)
        self._events_since_update = other._events_since_update

    def snapshot(self) -> dict:
        """Return retained events and interval phase for failover storage."""
        return {
            "events": [_event_snapshot(event) for event in self._events],
            "events_since_update": self._events_since_update,
        }

    def restore(self, snapshot: dict) -> None:
        """Restore retained events and interval phase from a validated snapshot."""
        if not isinstance(snapshot, dict) or set(snapshot) != {"events", "events_since_update"}:
            raise ValueError("sliding snapshot is invalid")
        events = snapshot["events"]
        phase = snapshot["events_since_update"]
        if (not isinstance(events, list) or len(events) > self.size
                or type(phase) is not int or not 0 <= phase < self.interval):
            raise ValueError("sliding snapshot is invalid for this window configuration")
        self._events = deque((_event_from_snapshot(event) for event in events), maxlen=self.size)
        self._events_since_update = phase


def _event_snapshot(event: LibraryEvent) -> dict:
    return {
        "event_id": event.event_id,
        "event_type": event.event_type.value,
        "student_id": event.student_id,
        "book_id": event.book_id,
        "timestamp": event.timestamp.isoformat(),
        "value": event.value,
        "run_id": event.run_id,
        "simulation_id": event.simulation_id,
        "published_at": event.published_at.isoformat() if event.published_at else None,
    }


def _event_from_snapshot(value: dict) -> LibraryEvent:
    return LibraryEvent.from_mapping(value)
