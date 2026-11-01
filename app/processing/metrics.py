"""Aggregation helpers for numeric library-event values."""

from dataclasses import dataclass
from typing import Iterable, Optional

from app.schemas import LibraryEvent


@dataclass(frozen=True)
class Metrics:
    """Summary statistics for one event group; empty groups have no average/min/max."""

    count: int
    total: float
    average: Optional[float]
    minimum: Optional[float]
    maximum: Optional[float]


def calculate_metrics(events: Iterable[LibraryEvent]) -> Metrics:
    """Calculate count, sum, average, minimum, and maximum from event values."""
    values = [event.value for event in events]
    if not values:
        return Metrics(count=0, total=0.0, average=None, minimum=None, maximum=None)
    total = sum(values)
    return Metrics(
        count=len(values),
        total=total,
        average=total / len(values),
        minimum=min(values),
        maximum=max(values),
    )
