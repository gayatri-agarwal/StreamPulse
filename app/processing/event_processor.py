"""Connect existing window managers to metrics storage without duplicating logic."""

import asyncio
from typing import Dict, Optional

from app.processing.metrics import Metrics
from app.processing.window_manager import SlidingWindow, TumblingWindow
from app.schemas import LibraryEvent


class LibraryEventProcessor:
    """Update one in-memory window pair and persist each newly available summary.

    A single-active RabbitMQ consumer and this lock serialize event processing for
    this processor instance. Window state is in memory and is lost on restart.
    """

    def __init__(self, tumbling: TumblingWindow, sliding: SlidingWindow, metrics_store: object) -> None:
        self.tumbling = tumbling
        self.sliding = sliding
        self.metrics_store = metrics_store
        self._lock = asyncio.Lock()

    async def process(self, event: LibraryEvent) -> Dict[str, Metrics]:
        """Update both windows, persisting tumbling only when complete."""
        if not isinstance(event, LibraryEvent):
            raise TypeError("event must be a validated LibraryEvent")
        async with self._lock:
            tumbling_metrics: Optional[Metrics] = self.tumbling.add(event)
            sliding_metrics = self.sliding.add(event)
            if tumbling_metrics is not None:
                await self.metrics_store.save_metrics("tumbling", tumbling_metrics)
            await self.metrics_store.save_metrics("sliding", sliding_metrics)
            result = {"sliding": sliding_metrics}
            if tumbling_metrics is not None:
                result["tumbling"] = tumbling_metrics
            return result
