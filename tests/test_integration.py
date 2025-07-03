import asyncio
import time

import pytest
import pytest_asyncio
from conftest import ASYNC_DATABASE_URL, SYNC_DATABASE_URL, TEST_TABLE_NAME

from pgque import AsyncMessageQueue, MessageQueue


async def async_cleanup_sync_queue(queue: MessageQueue):
    async with queue._async_queue.get_session() as session:
        await session.execute(queue._async_queue.Message.__table__.delete())


@pytest.fixture
def sync_queue() -> MessageQueue:
    """
    Provides a synchronous queue for testing,
    and cleans up the data after the test is completed.
    """
    queue = MessageQueue(SYNC_DATABASE_URL, table_name=TEST_TABLE_NAME)
    yield queue
    asyncio.run(async_cleanup_sync_queue(queue))


@pytest_asyncio.fixture
async def async_queue() -> AsyncMessageQueue:
    """
    Provides an asynchronous queue for testing,
    and cleans up the data after the test is completed.
    """
    queue = AsyncMessageQueue(ASYNC_DATABASE_URL, table_name=TEST_TABLE_NAME)
    await queue.create_table()
    yield queue
    async with queue.get_session() as session:
        await session.execute(queue.Message.__table__.delete())  # type: ignore
    await queue.close()


# Synchronous tests
def test_send_and_receive_message(sync_queue: MessageQueue):
    queue_name = "test_queue_sync"
    payload = {"key": "value"}

    message_id = sync_queue.send_message(queue_name, payload)
    assert message_id is not None

    message = sync_queue.receive_message(queue_name)
    assert message is not None
    assert message["id"] == message_id
    assert message["payload"] == payload


def test_complete_message(sync_queue: MessageQueue):
    queue_name = "test_complete_sync"
    payload = {"task": "process_video"}

    sync_queue.send_message(queue_name, payload)
    message = sync_queue.receive_message(queue_name)
    assert message is not None

    completed = sync_queue.complete_message(message["id"])
    assert completed is True

    stats = sync_queue.get_queue_stats(queue_name)
    assert stats["completed"] == 1


def test_fail_message_and_retry(sync_queue: MessageQueue):
    queue_name = "test_fail_sync"
    payload = {"data": "some_data"}

    sync_queue.send_message(queue_name, payload, max_retries=1)
    message = sync_queue.receive_message(queue_name)
    assert message is not None

    failed = sync_queue.fail_message(message["id"], "Simulated failure")
    assert failed is True

    # The message should be in pending status for retry
    stats = sync_queue.get_queue_stats(queue_name)
    assert stats["pending"] == 1

    time.sleep(2)

    # Receive the message again
    message = sync_queue.receive_message(queue_name)
    assert message is not None
    assert message["retry_count"] == 1

    # Fail the message again, it should go to dead-letter queue
    failed = sync_queue.fail_message(message["id"], "Simulated failure again")
    assert failed is True

    stats = sync_queue.get_queue_stats(queue_name)
    assert stats["dead_letter"] == 1


def test_delay_message(sync_queue: MessageQueue):
    queue_name = "test_delay_sync"
    payload = {"scheduled": "task"}

    sync_queue.send_message(queue_name, payload, delay_seconds=2)

    message = sync_queue.receive_message(queue_name)
    assert message is None

    time.sleep(2)

    message = sync_queue.receive_message(queue_name)
    assert message is not None
    assert message["payload"] == payload


# Asynchronous tests
@pytest.mark.asyncio
async def test_async_send_and_receive_message(async_queue: AsyncMessageQueue):
    queue_name = "test_queue_async"
    payload = {"key": "value"}

    message_id = await async_queue.send_message(queue_name, payload)
    assert message_id is not None

    message = await async_queue.receive_message(queue_name)
    assert message is not None
    assert message["id"] == message_id
    assert message["payload"] == payload


@pytest.mark.asyncio
async def test_async_complete_message(async_queue: AsyncMessageQueue):
    queue_name = "test_complete_async"
    payload = {"task": "process_image"}

    await async_queue.send_message(queue_name, payload)
    message = await async_queue.receive_message(queue_name)
    assert message is not None

    completed = await async_queue.complete_message(message["id"])
    assert completed is True

    stats = await async_queue.get_queue_stats(queue_name)
    assert stats["completed"] == 1


@pytest.mark.asyncio
async def test_async_fail_message_and_retry(async_queue: AsyncMessageQueue):
    queue_name = "test_fail_async"
    payload = {"data": "some_async_data"}

    await async_queue.send_message(queue_name, payload, max_retries=1)
    message = await async_queue.receive_message(queue_name)
    assert message is not None

    failed = await async_queue.fail_message(message["id"], "Simulated async failure")
    assert failed is True

    stats = await async_queue.get_queue_stats(queue_name)
    assert stats["pending"] == 1

    await asyncio.sleep(2)

    message = await async_queue.receive_message(queue_name)
    assert message is not None
    assert message["retry_count"] == 1

    failed = await async_queue.fail_message(message["id"], "Simulated async failure again")
    assert failed is True

    stats = await async_queue.get_queue_stats(queue_name)
    assert stats["dead_letter"] == 1


@pytest.mark.asyncio
async def test_async_delay_message(async_queue: AsyncMessageQueue):
    queue_name = "test_delay_async"
    payload = {"scheduled": "async_task"}

    await async_queue.send_message(queue_name, payload, delay_seconds=2)

    message = await async_queue.receive_message(queue_name)
    assert message is None

    await asyncio.sleep(2)

    message = await async_queue.receive_message(queue_name)
    assert message is not None
    assert message["payload"] == payload
