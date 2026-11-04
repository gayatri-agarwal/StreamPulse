"""Validated configuration values for tumbling and sliding windows."""

from dataclasses import dataclass
from typing import ClassVar, Tuple


@dataclass(frozen=True)
class WindowConfiguration:
    """Describe valid window sizes and a per-event sliding interval.

    This model is intentionally independent of the active window instances,
    Redis storage, and the HTTP API.
    """

    tumbling_size: int = 10
    sliding_size: int = 5
    sliding_interval: int = 1

    ALLOWED_WINDOW_SIZES: ClassVar[Tuple[int, ...]] = (5, 8, 10)

    def __post_init__(self) -> None:
        for field_name in ("tumbling_size", "sliding_size"):
            size = getattr(self, field_name)
            if type(size) is not int:
                raise ValueError(f"{field_name} must be an integer")
            if size not in self.ALLOWED_WINDOW_SIZES:
                raise ValueError("window sizes must be one of 5, 8, or 10")

        if type(self.sliding_interval) is not int or self.sliding_interval <= 0:
            raise ValueError("sliding_interval must be a positive integer")
        if self.sliding_interval >= self.sliding_size:
            raise ValueError("sliding_interval must be smaller than sliding_size")
