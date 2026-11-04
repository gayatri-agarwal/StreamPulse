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
