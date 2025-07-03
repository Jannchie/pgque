import uuid
from datetime import UTC, datetime
from enum import Enum

from sqlalchemy import DateTime, Index, Integer, String, Text
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class BaseMessage(DeclarativeBase):
    __abstract__ = True

    id: Mapped[str] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
    )
    queue_name: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    payload: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(
        String(50),
        default="pending",
        index=True,
    )
    retry_count: Mapped[int] = mapped_column(Integer, default=0)
    max_retries: Mapped[int] = mapped_column(Integer, default=3)
    priority: Mapped[int] = mapped_column(Integer, default=0, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=lambda: datetime.now(tz=UTC))
    updated_at: Mapped[datetime] = mapped_column(
        DateTime,
        default=lambda: datetime.now(tz=UTC),
        onupdate=lambda: datetime.now(tz=UTC),
    )
    scheduled_at: Mapped[datetime] = mapped_column(
        DateTime,
        default=lambda: datetime.now(tz=UTC),
        index=True,
    )
    processed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)


class MessageStatus(str, Enum):
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    DEAD_LETTER = "dead_letter"


_message_class_cache: dict[str, type[BaseMessage]] = {}


def get_message_class(table_name: str) -> type[BaseMessage]:
    """
    Creates a new Message class for a specific table name.
    Caches the created class to avoid re-definition issues.
    """
    if table_name not in _message_class_cache:

        class Message(BaseMessage):
            __tablename__ = table_name

            __table_args__ = (
                Index(f"idx_{table_name}_queue_status_scheduled", "queue_name", "status", "scheduled_at"),
                Index(f"idx_{table_name}_queue_priority_scheduled", "queue_name", "priority", "scheduled_at"),
            )

        _message_class_cache[table_name] = Message
    return _message_class_cache[table_name]
