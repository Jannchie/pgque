import asyncio
import logging
from collections.abc import Callable
from typing import Any

from pgque.async_queue import AsyncMessageQueue
from pgque.sync_queue import MessageQueue

logger = logging.getLogger(__name__)


class MessageWorker:
    """Synchronous message worker"""

    def __init__(
        self,
        queue: MessageQueue,
        queue_name: str,
        handler: Callable[[dict[str, Any]], None],
    ):
        self.queue = queue
        self.queue_name = queue_name
        self.handler = handler
        self.running = False
        self._async_worker = AsyncMessageWorker(queue._async_queue, queue_name, handler)  # noqa: SLF001

    def start(self, poll_interval: int = 1, max_messages_per_batch: int = 1):
        """Start the synchronous worker"""
        self.running = True
        logger.debug("Starting synchronous worker for queue %s", self.queue_name)
        asyncio.run(self._async_worker.start(poll_interval, max_messages_per_batch))

    def stop(self):
        """Stop the worker"""
        self.running = False
        logger.debug("Stopping synchronous worker for queue %s", self.queue_name)
        self._async_worker.stop()


class AsyncMessageWorker:
    """Asynchronous message worker"""

    def __init__(
        self,
        queue: AsyncMessageQueue,
        queue_name: str,
        handler: Callable[[dict[str, Any]], Any],
    ):
        self.queue = queue
        self.queue_name = queue_name
        self.handler = handler
        self.running = False

    async def start(self, poll_interval: float = 1, max_messages_per_batch: int = 1):
        """Start the asynchronous worker"""
        self.running = True
        logger.debug("Starting asynchronous worker for queue %s", self.queue_name)

        while self.running:
            try:
                processed_count = 0

                for _ in range(max_messages_per_batch):
                    message = await self.queue.receive_message(self.queue_name)
                    if message:
                        try:
                            if asyncio.iscoroutinefunction(self.handler):
                                await self.handler(message["payload"])
                            else:
                                self.handler(message["payload"])

                            await self.queue.complete_message(message["id"])
                            processed_count += 1
                        except Exception as e:
                            error_msg = f"Handler error: {e!s}"
                            logger.exception("Error processing message %s: %s", message["id"], error_msg)
                            await self.queue.fail_message(message["id"], error_msg)
                            processed_count += 1
                    else:
                        break

                if processed_count == 0:
                    await asyncio.sleep(poll_interval)

            except Exception:
                logger.exception("Async worker error")
                await asyncio.sleep(poll_interval)

    def stop(self):
        """Stop the worker"""
        self.running = False
        logger.debug("Stopping asynchronous worker for queue %s", self.queue_name)
