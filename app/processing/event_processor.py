"""Connect existing window managers to metrics storage without duplicating logic."""

import asyncio
import logging
import time
from typing import Dict, Optional
from uuid import UUID

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
        *,
        run_id: Optional[str] = None,
    ) -> None:
        if active_configuration is not None:
            if not isinstance(active_configuration, WindowConfiguration):
                raise TypeError("active_configuration must be a WindowConfiguration")
            active_configuration = WindowConfiguration(
                active_configuration.tumbling_size,
                active_configuration.sliding_size,
                active_configuration.sliding_interval,
            )
        if run_id is not None:
            try:
                run_id = str(UUID(run_id))
            except (TypeError, ValueError, AttributeError) as exc:
                raise ValueError("run_id must be a valid UUID") from exc
            if active_configuration is None:
                raise ValueError("run processors require an immutable WindowConfiguration")
            if (
                tumbling.size != active_configuration.tumbling_size
                or sliding.size != active_configuration.sliding_size
                or sliding.interval != active_configuration.sliding_interval
            ):
                raise ValueError("run processor windows must match its configuration")
        self.tumbling = tumbling
        self.sliding = sliding
        self.metrics_store = metrics_store
        self.active_configuration = active_configuration
        self.run_id = run_id
        self._lock = asyncio.Lock()
        self.last_redis_persistence_latency_ms: Optional[float] = None

    async def process(self, event: LibraryEvent) -> Dict[str, Metrics]:
        """Update both windows, persisting tumbling only when complete."""
        if not isinstance(event, LibraryEvent):
            raise TypeError("event must be a validated LibraryEvent")
        async with self._lock:
            self.last_redis_persistence_latency_ms = None
            tumbling_metrics: Optional[Metrics] = self.tumbling.add(event)
            sliding_metrics = self.sliding.add(event)
            persistence_started = time.perf_counter_ns()
            if tumbling_metrics is not None or sliding_metrics is not None:
                try:
                    if tumbling_metrics is not None:
                        if self.run_id is None:
                            await self.metrics_store.save_metrics("tumbling", tumbling_metrics)
                        else:
                            await self.metrics_store.save_run_metrics(
                                self.run_id, "tumbling", tumbling_metrics
                            )
                    if sliding_metrics is not None:
                        if self.run_id is None:
                            await self.metrics_store.save_metrics("sliding", sliding_metrics)
                        else:
                            await self.metrics_store.save_run_metrics(
                                self.run_id, "sliding", sliding_metrics
                            )
                finally:
                    self.last_redis_persistence_latency_ms = (
                        time.perf_counter_ns() - persistence_started
                    ) / 1_000_000.0
            result = {}
            if sliding_metrics is not None:
                result["sliding"] = sliding_metrics
            if tumbling_metrics is not None:
                result["tumbling"] = tumbling_metrics
            if self.run_id is None:
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
