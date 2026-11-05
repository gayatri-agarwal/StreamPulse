"""Run the StreamPulse RabbitMQ consumer as a separate process."""
from dotenv import load_dotenv

load_dotenv()
import asyncio
import logging
from typing import Tuple

from app.messaging.rabbitmq import RabbitMQConsumer, RabbitMQSettings
from app.processing.event_processor import LibraryEventProcessor
from app.processing.window_config import WindowConfiguration
from app.processing.window_manager import SlidingWindow, TumblingWindow
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
    return RabbitMQConsumer(processor, settings), metrics_store


async def run_consumer() -> None:
    """Run until interrupted, then close Redis after the consumer stops."""
    consumer, metrics_store = await build_consumer()
    try:
        await consumer.run_forever()
    finally:
        # run_forever closes the RabbitMQ connection in its own finally block.
        await metrics_store.close()


if __name__ == "__main__":
    try:
        asyncio.run(run_consumer())
    except KeyboardInterrupt:
        pass
