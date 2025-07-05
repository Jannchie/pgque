import asyncio
import logging
import threading
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
        poll_interval: float = 1,  # Added poll_interval
        max_messages_per_batch: int = 1,  # Added max_messages_per_batch
    ):
        self.queue = queue
        self.queue_name = queue_name
        self.handler = handler
        self.running = False
        self.poll_interval = poll_interval  # Store poll_interval
        self.max_messages_per_batch = max_messages_per_batch  # Store max_messages_per_batch
        self._async_worker = AsyncMessageWorker(
            queue._async_queue,  # noqa: SLF001
            queue_name,
            handler,
            poll_interval=poll_interval,  # Pass to async worker
            max_messages_per_batch=max_messages_per_batch,  # Pass to async worker
        )
        self._thread = None
        self._loop = None
        self._task = None

    def start(self):  # Removed poll_interval and max_messages_per_batch from here
        """Start the synchronous worker"""
        self.running = True
        self._async_worker.running = True  # Ensure async worker is also set to running
        logger.debug("Starting synchronous worker for queue %s", self.queue_name)

        def run_in_thread() -> None:
            self._loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self._loop)
            try:
                self._task = self._loop.create_task(self._async_worker._run())  # noqa: SLF001
                self._loop.run_until_complete(self._task)
            except asyncio.CancelledError:
                pass
            finally:
                self._loop.close()

        self._thread = threading.Thread(target=run_in_thread, daemon=True)
        self._thread.start()

    def stop(self):
        """Stop the worker"""
        self.running = False
        logger.debug("Stopping synchronous worker for queue %s", self.queue_name)
        self._async_worker.stop()

        if self._task and self._loop and not self._loop.is_closed():
            self._loop.call_soon_threadsafe(self._task.cancel)

        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=1.0)


class AsyncMessageWorker:
    """Asynchronous message worker"""

    def __init__(
        self,
        queue: AsyncMessageQueue,
        queue_name: str,
        handler: Callable[[dict[str, Any]], Any],
        poll_interval: float = 1,  # Added poll_interval
        max_messages_per_batch: int = 1,  # Added max_messages_per_batch
    ):
        self.queue = queue
        self.queue_name = queue_name
        self.handler = handler
        self.running = False
        self.poll_interval = poll_interval  # Store poll_interval
        self.max_messages_per_batch = max_messages_per_batch  # Store max_messages_per_batch
        self._task = None

    def start(self):  # Removed poll_interval and max_messages_per_batch from here
        """Start the asynchronous worker"""
        self.running = True
        logger.debug("Starting asynchronous worker for queue %s", self.queue_name)
        self._task = asyncio.create_task(self._run())
        return self._task

    async def _process_message(self, message: dict[str, Any]) -> None:
        """Process a single message"""
        try:
            if asyncio.iscoroutinefunction(self.handler):
                result = await self.handler(message["payload"])
            else:
                result = self.handler(message["payload"])

            if result is False:
                await self.queue.fail_message(message["id"], "Handler returned False")
            else:
                await self.queue.complete_message(message["id"])
        except Exception as e:
            error_msg = f"Handler error: {e!s}"
            logger.exception("Error processing message %s: %s", message["id"], error_msg)
            await self.queue.fail_message(message["id"], error_msg)

    async def _process_batch(self) -> int:
        """Process a batch of messages and return count of processed messages"""
        processed_count = 0

        for _ in range(self.max_messages_per_batch):
            if not self.running:
                break

            message = await self.queue.receive_message(self.queue_name)
            if not message:
                break

            await self._process_message(message)
            processed_count += 1

        return processed_count

    async def _sleep_with_cancellation(self) -> None:
        """Sleep while handling cancellation"""
        await asyncio.sleep(self.poll_interval)

    async def _run(self) -> None:
        """Internal run method"""
        while self.running:
            try:
                processed_count = await self._process_batch()

                if processed_count == 0 and self.running:
                    await self._sleep_with_cancellation()

            except asyncio.CancelledError:
                break
            except Exception:
                logger.exception("Async worker error")
                if self.running:
                    await self._sleep_with_cancellation()

    def stop(self):
        """Stop the worker"""
        self.running = False
        logger.debug("Stopping asynchronous worker for queue %s", self.queue_name)
        if self._task:
            self._task.cancel()
