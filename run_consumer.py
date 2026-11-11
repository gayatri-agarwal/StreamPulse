"""Run the StreamPulse RabbitMQ consumer as a separate process."""
from dotenv import load_dotenv

load_dotenv()
import asyncio
import logging
from pathlib import Path
from typing import Tuple

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent
load_dotenv(dotenv_path=PROJECT_ROOT / ".env", override=False)

from app.messaging.rabbitmq import RabbitMQConsumer, RabbitMQSettings
from app.processing.event_processor import LibraryEventProcessor
from app.processing.window_config import WindowConfiguration
from app.processing.window_manager import SlidingWindow, TumblingWindow
from app.experiments.store import ExperimentRepository
from app.storage.redis_metrics import RedisMetricsStore


# Demonstration defaults: tumbling summarizes each group of 10 events; sliding
# summarizes the latest 5 events after every arrival (including partial groups).
TUMBLING_WINDOW_SIZE = 10
SLIDING_WINDOW_SIZE = 5
logger = logging.getLogger(__name__)


async def build_consumer() -> Tuple[RabbitMQConsumer, RedisMetricsStore]:
    """Recover persisted window config, then construct the consumer graph."""
    settings = RabbitMQSettings.from_env()
    metrics_store = RedisMetricsStore()
    try:
        active_configuration = await metrics_store.read_active_configuration()
        if active_configuration is None:
            active_configuration = await metrics_store.initialize_active_configuration(
                WindowConfiguration()
            )
    except Exception:
        try:
            await metrics_store.close()
        except Exception:
            logger.warning("Could not close Redis after consumer configuration recovery failed.")
        raise
    processor = LibraryEventProcessor(
        TumblingWindow(active_configuration.tumbling_size),
        SlidingWindow(
            active_configuration.sliding_size,
            active_configuration.sliding_interval,
        ),
        metrics_store,
        active_configuration,
    )
    return RabbitMQConsumer(
        processor,
        settings,
        experiment_repository=ExperimentRepository(),
    ), metrics_store


async def run_consumer() -> None:
    """Run until interrupted, then close Redis after the consumer stops."""
    consumer, metrics_store = await build_consumer()
    try:
        await consumer.run_forever()
    finally:
        # A timed-out callback may still be unwinding cancellation. Keep its
        # Redis dependency alive until the process exits rather than closing a
        # store that an active handler could still be using.
        drain_result = getattr(consumer, "last_drain_result", None)
        if drain_result is not None and drain_result.active_handlers_remaining:
            logger.error(
                "Leaving Redis open because %d consumer handler(s) remain active",
                drain_result.active_handlers_remaining,
            )
        else:
            await metrics_store.close()


if __name__ == "__main__":
    try:
        asyncio.run(run_consumer())
    except KeyboardInterrupt:
        pass
