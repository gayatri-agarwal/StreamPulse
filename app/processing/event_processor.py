"""Connect existing window managers to metrics storage without duplicating logic."""

import asyncio
import logging
from typing import Dict, Optional

from app.processing.metrics import Metrics
from app.processing.window_config import WindowConfiguration
from app.processing.window_manager import SlidingWindow, TumblingWindow
from app.schemas import LibraryEvent


logger = logging.getLogger(__name__)


class LibraryEventProcessor:
    """Update one in-memory window pair and persist each newly available summary.

    A single-active RabbitMQ consumer and this lock serialize event processing for
    this processor instance. Window state is in memory and is lost on restart.
    """

    def __init__(
        self,
        tumbling: TumblingWindow,
        sliding: SlidingWindow,
        metrics_store: object,
        active_configuration: Optional[WindowConfiguration] = None,
    ) -> None:
        self.tumbling = tumbling
        self.sliding = sliding
        self.metrics_store = metrics_store
        self.active_configuration = active_configuration
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
            if sliding_metrics is not None:
                await self.metrics_store.save_metrics("sliding", sliding_metrics)
            result = {}
            if sliding_metrics is not None:
                result["sliding"] = sliding_metrics
            if tumbling_metrics is not None:
                result["tumbling"] = tumbling_metrics
            pending_configuration = await self._read_pending_configuration()
            if tumbling_metrics is not None and pending_configuration is not None:
                await self._activate_pending_configuration_at_boundary()
            return result

    async def _read_pending_configuration(self) -> Optional[WindowConfiguration]:
        """Observe pending config during event processing without affecting the event."""
        read_pending = getattr(self.metrics_store, "read_pending_configuration", None)
        if not callable(read_pending):
            return None
        try:
            return await read_pending()
        except Exception:
            logger.warning(
                "Could not read pending window configuration; event processing continues."
            )
            return None

    async def _activate_pending_configuration_at_boundary(self) -> None:
        """Promote and install pending config only after completed summaries persist."""
        promote = getattr(self.metrics_store, "promote_pending_configuration", None)
        if not callable(promote):
            return
        try:
            configuration = await promote()
            if configuration is None:
                return
            if not isinstance(configuration, WindowConfiguration):
                raise TypeError("promoted configuration must be a WindowConfiguration")
            tumbling = TumblingWindow(configuration.tumbling_size)
            sliding = SlidingWindow(configuration.sliding_size, configuration.sliding_interval)
        except Exception:
            logger.warning(
                "Could not activate pending window configuration; current windows remain active."
            )
            return
        self.tumbling, self.sliding = tumbling, sliding
        self.active_configuration = configuration
