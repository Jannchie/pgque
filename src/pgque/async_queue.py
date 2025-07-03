import json
import logging
import uuid
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import delete, func, select, update
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from pgque.base import _BaseMessageQueue
from pgque.models import MessageStatus, get_message_class

logger = logging.getLogger(__name__)


class AsyncMessageQueue(_BaseMessageQueue):
    """Asynchronous message queue"""

    def __init__(
        self,
        database_url: str,
        *,
        echo: bool = False,
        table_name: str = "messages",
    ):
        """
        Initialize the asynchronous message queue
        """
        super().__init__(get_message_class(table_name))
        self.engine = create_async_engine(
            database_url,
            echo=echo,
            pool_size=50,
            max_overflow=50,
            pool_pre_ping=True,
            pool_recycle=3600,
        )
        self.async_session = async_sessionmaker(self.engine)

    @asynccontextmanager
    async def get_session(self):
        """Get a context manager for the async database session"""
        async with self.async_session() as session:
            try:
                yield session
                await session.commit()
            except Exception:
                await session.rollback()
                raise

    async def reset_stuck_messages(
        self,
        queue_name: str,
        timeout_seconds: int = 600,
    ) -> int:
        """
        Asynchronously reset processing messages that have timed out to pending.
        """
        threshold = datetime.now(tz=UTC) - timedelta(seconds=timeout_seconds)
        logger.debug("Resetting stuck messages for queue %s older than %s seconds", queue_name, timeout_seconds)
        async with self.get_session() as session:
            stmt = (
                update(self.Message)
                .where(
                    self.Message.queue_name == queue_name,
                    self.Message.status == MessageStatus.PROCESSING,
                    self.Message.updated_at < threshold,
                )
                .values(
                    status=MessageStatus.PENDING,
                    updated_at=datetime.now(tz=UTC),
                )
            )
            result = await session.execute(stmt)
            logger.debug("Reset %s stuck messages for queue %s", result.rowcount, queue_name)
            return result.rowcount

    async def send_message(
        self,
        queue_name: str,
        payload: dict[Any, Any],
        priority: int = 0,
        delay_seconds: int = 0,
        max_retries: int = 3,
    ) -> str:
        """Asynchronously send a message to the queue"""
        message_id = str(uuid.uuid4())
        scheduled_at = datetime.now(tz=UTC) + timedelta(seconds=delay_seconds)
        logger.debug("Sending message %s to queue %s", message_id, queue_name)

        async with self.get_session() as session:
            message = self.Message(
                id=message_id,
                queue_name=queue_name,
                payload=json.dumps(payload),
                priority=priority,
                scheduled_at=scheduled_at,
                max_retries=max_retries,
            )
            session.add(message)

        return message_id

    async def receive_message(
        self,
        queue_name: str,
    ) -> dict[str, Any] | None:
        """Asynchronously receive a message"""
        async with self.get_session() as session:
            subquery = (
                select(self.Message.id)
                .where(
                    self.Message.queue_name == queue_name,
                    self.Message.status == MessageStatus.PENDING,
                    self.Message.scheduled_at <= datetime.now(tz=UTC),
                )
                .order_by(self.Message.priority.desc(), self.Message.scheduled_at.asc())
                .with_for_update(skip_locked=True)
                .limit(1)
                .scalar_subquery()
            )

            stmt = (
                update(self.Message)
                .where(self.Message.id == subquery)
                .values(
                    status=MessageStatus.PROCESSING,
                    updated_at=datetime.now(tz=UTC),
                )
                .returning(self.Message)
            )

            message = await session.scalar(stmt)

            if not message:
                return None

            logger.debug("Received message %s from queue %s", message.id, queue_name)
            return self._message_to_dict(message)

    async def complete_message(self, message_id: str) -> bool:
        """Asynchronously mark message as completed"""
        logger.debug("Completing message %s", message_id)
        async with self.get_session() as session:
            stmt = (
                update(self.Message)
                .where(self.Message.id == message_id)
                .values(
                    status=MessageStatus.COMPLETED,
                    processed_at=datetime.now(tz=UTC),
                    updated_at=datetime.now(tz=UTC),
                )
            )
            result = await session.execute(stmt)
            return result.rowcount > 0

    async def fail_message(self, message_id: str, error_message: str | None = None) -> bool:
        """Asynchronously mark message as failed"""
        logger.warning("Failing message %s with error: %s", message_id, error_message)
        async with self.get_session() as session:
            message = await session.get(self.Message, message_id)
            if not message:
                return False

            values = self._prepare_fail_message_values(message)
            values["error_message"] = error_message

            stmt = update(self.Message).where(self.Message.id == message_id).values(**values)
            result = await session.execute(stmt)
            await session.commit()
            return result.rowcount > 0

    async def get_queue_stats(self, queue_name: str) -> dict[str, int]:
        """Asynchronously get queue statistics"""
        async with self.get_session() as session:
            stats = {}
            for status in MessageStatus:
                stmt = select(func.count(self.Message.id)).where(
                    self.Message.queue_name == queue_name,
                    self.Message.status == status,
                )
                count = await session.scalar(stmt)
                stats[status.value] = count or 0

            return stats

    async def purge_completed_messages(
        self,
        queue_name: str,
        older_than_days: int = 7,
    ) -> int:
        """
        Asynchronously purge completed messages"""
        cutoff_date = datetime.now(tz=UTC) - timedelta(days=older_than_days)
        logger.debug("Purging completed messages for queue %s older than %s days", queue_name, older_than_days)

        async with self.get_session() as session:
            stmt = delete(self.Message).where(
                self.Message.queue_name == queue_name,
                self.Message.status == MessageStatus.COMPLETED,
                self.Message.processed_at < cutoff_date,
            )
            result = await session.execute(stmt)
            logger.debug("Purged %s completed messages for queue %s", result.rowcount, queue_name)
            return result.rowcount

    async def requeue_dead_letter_messages(
        self,
        queue_name: str,
        max_retries: int = 3,
    ) -> int:
        """
        Asynchronously requeue dead letter messages"""
        logger.debug("Requeuing dead letter messages for queue %s", queue_name)
        async with self.get_session() as session:
            stmt = (
                update(self.Message)
                .where(
                    self.Message.queue_name == queue_name,
                    self.Message.status == MessageStatus.DEAD_LETTER,
                )
                .values(
                    status=MessageStatus.PENDING,
                    retry_count=0,
                    max_retries=max_retries,
                    scheduled_at=datetime.now(tz=UTC),
                    error_message=None,
                    updated_at=datetime.now(tz=UTC),
                )
            )
            result = await session.execute(stmt)
            logger.debug("Requeued %s dead letter messages for queue %s", result.rowcount, queue_name)
            return result.rowcount

    async def close(self):
        """Close the async engine"""
        await self.engine.dispose()

    async def create_table(self):
        """Create the message table in the database."""
        async with self.engine.begin() as conn:
            await conn.run_sync(self.Message.metadata.create_all)
