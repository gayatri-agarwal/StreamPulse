"""Run the StreamPulse RabbitMQ consumer as a separate process."""

import asyncio
from typing import Tuple

from app.messaging.rabbitmq import RabbitMQConsumer, RabbitMQSettings
from app.processing.event_processor import LibraryEventProcessor
from app.processing.window_manager import SlidingWindow, TumblingWindow
from app.storage.redis_metrics import RedisMetricsStore


# Demonstration defaults: tumbling summarizes each group of 10 events; sliding
# summarizes the latest 5 events after every arrival (including partial groups).
TUMBLING_WINDOW_SIZE = 10
SLIDING_WINDOW_SIZE = 5


def build_consumer() -> Tuple[RabbitMQConsumer, RedisMetricsStore]:
    """Construct the consumer graph using the application's environment config."""
    settings = RabbitMQSettings.from_env()
    metrics_store = RedisMetricsStore()
    processor = LibraryEventProcessor(
        TumblingWindow(TUMBLING_WINDOW_SIZE),
        SlidingWindow(SLIDING_WINDOW_SIZE),
        metrics_store,
    )
    return RabbitMQConsumer(processor, settings), metrics_store


async def run_consumer() -> None:
    """Run until interrupted, then close Redis after the consumer stops."""
    consumer, metrics_store = build_consumer()
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
