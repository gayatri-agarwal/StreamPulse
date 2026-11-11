"""Measurement calculations shared by live monitoring and future experiment runs."""

from dataclasses import dataclass
from math import ceil
from typing import Iterable, Optional


def average_latency(samples: Iterable[float]) -> Optional[float]:
    """Return the arithmetic mean of actual samples, or None for no samples."""
    values = [float(value) for value in samples]
    return sum(values) / len(values) if values else None


def percentile_95(samples: Iterable[float]) -> Optional[float]:
    """Return nearest-rank P95 (rank=ceil(.95*n)); never infer from aggregates."""
    values = sorted(float(value) for value in samples)
    if not values:
        return None
    rank = max(1, ceil(0.95 * len(values)))
    return values[rank - 1]


def failure_rate(failures: int, attempts: int) -> Optional[float]:
    """Return failures/attempts in [0,1], or None when no attempts exist."""
    if attempts <= 0:
        return None
    return failures / attempts


@dataclass(frozen=True)
class MeasurementSummary:
    """Stage-specific measurements; unavailable values are represented as null."""

    publish_attempts: int
    publish_successes: int
    publish_failures: int
    publish_failure_rate: Optional[float]
    consumer_attempts: int
    consumer_successes: int
    consumer_failures: int
    consumer_failure_rate: Optional[float]
    throughput_events_per_second: Optional[float]
    sample_count: dict
    average_latency_ms: dict
    p95_latency_ms: dict
    consumer_unique_successes: int = 0
    inter_arrival_rate_events_per_second: Optional[float] = None
    measurements_present: bool = False

