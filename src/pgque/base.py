import json
from datetime import UTC, datetime, timedelta
from typing import Any

from pgque.models import BaseMessage, MessageStatus


class _BaseMessageQueue:
    def __init__(self, message_class: type[BaseMessage]):
        self.Message = message_class

    def _message_to_dict(self, message: BaseMessage) -> dict[str, Any]:
        """Convert message object to dict"""
        return {
            "id": str(message.id),
            "payload": json.loads(message.payload),
            "retry_count": message.retry_count,
            "max_retries": message.max_retries,
            "created_at": message.created_at.isoformat(),
            "priority": message.priority,
        }

    def _prepare_fail_message_values(self, message: BaseMessage) -> dict[str, Any]:
        """Prepare values for failing a message."""
        retry_count = message.retry_count + 1
        if retry_count > message.max_retries:
            status = MessageStatus.DEAD_LETTER
            scheduled_at = message.scheduled_at
        else:
            status = MessageStatus.PENDING
            delay = 2**retry_count
            scheduled_at = datetime.now(tz=UTC) + timedelta(seconds=delay)
        return {
            "retry_count": retry_count,
            "status": status,
            "scheduled_at": scheduled_at,
            "updated_at": datetime.now(tz=UTC),
        }
