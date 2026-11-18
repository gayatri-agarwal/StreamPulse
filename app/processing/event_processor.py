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
        simulation_id: Optional[str] = None,
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
        if simulation_id is not None:
            try:
                simulation_id = str(UUID(simulation_id))
            except (TypeError, ValueError, AttributeError) as exc:
                raise ValueError("simulation_id must be a valid UUID") from exc
            if run_id is not None:
                raise ValueError("run_id and simulation_id are mutually exclusive")
            if active_configuration is None:
                raise ValueError("simulation processors require an immutable WindowConfiguration")
        self.tumbling = tumbling
        self.sliding = sliding
        self.metrics_store = metrics_store
        self.active_configuration = active_configuration
        self.run_id = run_id
        self.simulation_id = simulation_id
        self._lock = asyncio.Lock()
        self.last_redis_persistence_latency_ms: Optional[float] = None
        self._checkpoint_version = 0
        self._checkpoint_loaded = False

    async def _load_durable_state(self) -> None:
        if self._checkpoint_loaded:
            return
        loader = getattr(self.metrics_store, "load_processing_checkpoint", None)
        if callable(loader):
            checkpoint = await loader(run_id=self.run_id, simulation_id=self.simulation_id)
            self._checkpoint_version = checkpoint["version"]
            expected_configuration = (
                self.tumbling.size, self.sliding.size, self.sliding.interval,
            )
            stored_configuration = (
                checkpoint["tumbling_size"], checkpoint["sliding_size"],
                checkpoint["sliding_interval"],
            )
            if checkpoint["version"] and stored_configuration != expected_configuration:
                if self.run_id is not None or self.simulation_id is not None:
                    raise ValueError("Persisted window state does not match its immutable configuration")
                # Global configuration changes intentionally reset windows at a
                # completed tumbling boundary; retain the revision and commit the
                # new dimensions atomically with the next event.
            else:
                self.tumbling.restore(checkpoint["tumbling"])
                self.sliding.restore(checkpoint["sliding"])
        self._checkpoint_loaded = True

    async def process(self, event: LibraryEvent) -> Dict[str, Metrics]:
        """Update both windows, persisting tumbling only when complete."""
        if not isinstance(event, LibraryEvent):
            raise TypeError("event must be a validated LibraryEvent")
        async with self._lock:
            await self._load_durable_state()
            self.last_redis_persistence_latency_ms = None
            # Work on copies so a failed calculation or persistence attempt cannot
            # leave an unprocessed event in either live window.
            staged_tumbling = self.tumbling.copy()
            staged_sliding = self.sliding.copy()
            tumbling_metrics: Optional[Metrics] = staged_tumbling.add(event)
            sliding_metrics = staged_sliding.add(event)
            persistence_started = time.perf_counter_ns()
            checkpoint_writer = getattr(self.metrics_store, "commit_processing_checkpoint", None)
            store_methods = vars(type(self.metrics_store))
            checkpoint_store_type = next((
                cls for cls in type(self.metrics_store).__mro__
                if "commit_processing_checkpoint" in vars(cls)
            ), None)
            legacy_overrides = checkpoint_store_type is not None and any(
                name in store_methods
                and store_methods[name] is not getattr(checkpoint_store_type, name, None)
                for name in (
                    "save_metrics", "save_run_metrics", "save_simulation_metrics",
                    "save_metrics_batch",
                )
            )
            checkpointed = callable(checkpoint_writer) and not legacy_overrides
            summaries = {
                name: metrics for name, metrics in (
                    ("tumbling", tumbling_metrics), ("sliding", sliding_metrics)
                ) if metrics is not None
            }
            if checkpointed:
                try:
                    next_version = self._checkpoint_version + 1
                    committed = await checkpoint_writer(
                        event_id=event.event_id,
                        state={
                            "version": next_version,
                            "tumbling_size": staged_tumbling.size,
                            "sliding_size": staged_sliding.size,
                            "sliding_interval": staged_sliding.interval,
                            "tumbling": staged_tumbling.snapshot(),
                            "sliding": staged_sliding.snapshot(),
                        },
                        expected_version=self._checkpoint_version,
                        summaries=summaries,
                        run_id=self.run_id,
                        simulation_id=self.simulation_id,
                    )
                    if not committed:
                        checkpoint = await self.metrics_store.load_processing_checkpoint(
                            run_id=self.run_id, simulation_id=self.simulation_id,
                        )
                        self.tumbling.restore(checkpoint["tumbling"])
                        self.sliding.restore(checkpoint["sliding"])
                        self._checkpoint_version = checkpoint["version"]
                        return {}
                    self._checkpoint_version = next_version
                finally:
                    self.last_redis_persistence_latency_ms = (
                        time.perf_counter_ns() - persistence_started
                    ) / 1_000_000.0
            if tumbling_metrics is not None or sliding_metrics is not None:
                try:
                    if checkpointed:
                        summaries = {}
                    batch_writer = getattr(self.metrics_store, "save_metrics_batch", None)
                    store_methods = vars(type(self.metrics_store))
                    legacy_overrides = {
                        "save_metrics", "save_run_metrics", "save_simulation_metrics",
                    }.intersection(store_methods)
                    supports_batch = callable(batch_writer) and (
                        "save_metrics_batch" in store_methods or not legacy_overrides
                    )
                    if not summaries:
                        pass
                    elif supports_batch:
                        await batch_writer(
                            summaries,
                            run_id=self.run_id,
                            simulation_id=self.simulation_id,
                        )
                    else:
                        # Preserve compatibility with injected legacy stores. The
                        # production Redis store uses one atomic batch transaction.
                        for window_type, metrics in summaries.items():
                            if self.simulation_id is not None:
                                await self.metrics_store.save_simulation_metrics(
                                    self.simulation_id, window_type, metrics
                                )
                            elif self.run_id is None:
                                await self.metrics_store.save_metrics(window_type, metrics)
                            else:
                                await self.metrics_store.save_run_metrics(
                                    self.run_id, window_type, metrics
                                )
                finally:
                    self.last_redis_persistence_latency_ms = (
                        time.perf_counter_ns() - persistence_started
                    ) / 1_000_000.0
            self.tumbling.commit_from(staged_tumbling)
            self.sliding.commit_from(staged_sliding)
            result = {}
            if sliding_metrics is not None:
                result["sliding"] = sliding_metrics
            if tumbling_metrics is not None:
                result["tumbling"] = tumbling_metrics
            # Dashboard simulations still run the live window configuration;
            # only experiment runs use an immutable configuration snapshot.
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
