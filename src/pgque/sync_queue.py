import asyncio
from collections.abc import Callable
from contextlib import contextmanager
from typing import Any

from pgque.async_queue import AsyncMessageQueue
from pgque.models import get_message_class


class MessageQueue:
    """Synchronous message queue"""

    def __init__(
        self,
        database_url: str,
        *,
        echo: bool = False,
        table_name: str = "messages",
        create_table: bool = False,
    ):
        """
        Initialize the message queue

        Args:
            database_url: PostgreSQL connection string
            echo: Whether to print SQL statements
            table_name: The name of the message table
            create_table: If True, the message table will be created if it does not exist.
        """
        self.Message = get_message_class(table_name)
        self._async_queue = AsyncMessageQueue(
            database_url,
            echo=echo,
            table_name=table_name,
        )
        if create_table:

            async def _create_tables_async() -> None:
                async with self._async_queue.engine.begin() as conn:
                    await conn.run_sync(self._async_queue.Message.metadata.create_all)

            asyncio.run(_create_tables_async())

    @contextmanager
    def get_session(self):
        """Get a context manager for the database session"""
        error_message = "Synchronous session is not supported. Use async session instead."
        raise NotImplementedError(error_message)

    def reset_stuck_messages(
        self,
        queue_name: str,
        timeout_seconds: int = 600,
    ) -> int:
        """
        Reset processing messages that have timed out to pending.
        """
        return asyncio.run(self._async_queue.reset_stuck_messages(queue_name, timeout_seconds))

    def send_message(
        self,
        queue_name: str,
        payload: dict[Any, Any],
        priority: int = 0,
        delay_seconds: int = 0,
        max_retries: int = 3,
    ) -> str:
        """
        Send a message to the queue
        """
        return asyncio.run(self._async_queue.send_message(queue_name, payload, priority, delay_seconds, max_retries))

    def receive_message(
        self,
        queue_name: str,
    ) -> dict[str, Any] | None:
        """
        Receive a message (using FOR UPDATE SKIP LOCKED)
        """
        return asyncio.run(self._async_queue.receive_message(queue_name))

    def complete_message(self, message_id: str) -> bool:
        """Mark message as completed"""
        return asyncio.run(self._async_queue.complete_message(message_id))

    def fail_message(self, message_id: str, error_message: str | None = None) -> bool:
        """Mark message as failed"""
        return asyncio.run(self._async_queue.fail_message(message_id, error_message))

    def get_queue_stats(self, queue_name: str) -> dict[str, int]:
        """Get queue statistics"""
        return asyncio.run(self._async_queue.get_queue_stats(queue_name))

    def purge_completed_messages(
        self,
        queue_name: str,
        older_than_days: int = 7,
    ) -> int:
        """
        Purge completed messages"""
        return asyncio.run(self._async_queue.purge_completed_messages(queue_name, older_than_days))

    def requeue_dead_letter_messages(
        self,
        queue_name: str,
        max_retries: int = 3,
    ) -> int:
        """
        Requeue dead letter messages"""
        return asyncio.run(self._async_queue.requeue_dead_letter_messages(queue_name, max_retries))

    def create_worker(self, queue_name: str, handler: Callable[[dict[str, Any]], None]):
        """
        Create a synchronous message worker
        """
        from pgque.workers import MessageWorker  # noqa: PLC0415

        return MessageWorker(self, queue_name, handler)
