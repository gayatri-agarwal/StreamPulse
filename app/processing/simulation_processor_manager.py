"""Create isolated event-count processors for active simulation sessions."""

import asyncio
from dataclasses import dataclass
from typing import Dict

from app.processing.event_processor import LibraryEventProcessor
from app.processing.window_config import WindowConfiguration
from app.processing.window_manager import SlidingWindow, TumblingWindow


@dataclass
class _SimulationProcessorContext:
    configuration: WindowConfiguration
    processor: LibraryEventProcessor


class SimulationProcessorManager:
    """Keep each simulation's windows isolated from ordinary and run traffic."""

    def __init__(self, metrics_store: object) -> None:
        self._metrics_store = metrics_store
        self._contexts: Dict[str, _SimulationProcessorContext] = {}
        self._lock = asyncio.Lock()

    async def acquire(self, simulation_id: str) -> LibraryEventProcessor:
        async with self._lock:
            context = self._contexts.get(simulation_id)
            if context is not None:
                return context.processor
            configuration = await self._metrics_store.read_simulation_configuration(simulation_id)
            if configuration is None:
                raise ValueError("Simulation configuration is unavailable")
            processor = LibraryEventProcessor(
                TumblingWindow(configuration.tumbling_size),
                SlidingWindow(configuration.sliding_size, configuration.sliding_interval),
                self._metrics_store,
                configuration,
                simulation_id=simulation_id,
            )
            self._contexts[simulation_id] = _SimulationProcessorContext(configuration, processor)
            return processor

