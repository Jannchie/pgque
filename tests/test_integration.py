import asyncio
import contextlib
import time
from collections.abc import AsyncGenerator, Generator

import pytest
import pytest_asyncio
from conftest import ASYNC_DATABASE_URL, SYNC_DATABASE_URL, TEST_TABLE_NAME
from sqlalchemy import delete

from pgque import AsyncMessageQueue, MessageQueue
from pgque.workers import AsyncMessageWorker, MessageWorker


async def async_cleanup_sync_queue(queue: MessageQueue):
    async with queue._async_queue.get_session() as session:
        await session.execute(delete(queue._async_queue.Message))


@pytest.fixture
def sync_queue() -> Generator[MessageQueue, None, None]:
    """
    Provides a synchronous queue for testing,
    and cleans up the data after the test is completed.
    """
    queue = MessageQueue(SYNC_DATABASE_URL, table_name=TEST_TABLE_NAME)
    yield queue
    asyncio.run(async_cleanup_sync_queue(queue))


@pytest_asyncio.fixture
async def async_queue() -> AsyncGenerator[AsyncMessageQueue, None]:
    """
    Provides an asynchronous queue for testing,
    and cleans up the data after the test is completed.
    """
    queue = AsyncMessageQueue(ASYNC_DATABASE_URL, table_name=TEST_TABLE_NAME)
    await queue.create_table()
    yield queue
    async with queue.get_session() as session:
        await session.execute(delete(queue.Message))
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


def test_worker_process_message(sync_queue: MessageQueue):
    queue_name = "test_worker_process"
    payload = {"data": "worker_task"}
    sync_queue.send_message(queue_name, payload)

    processed_payloads = []

    def handler(current_payload) -> bool:
        processed_payloads.append(current_payload)
        return True  # Indicate successful processing

    worker = MessageWorker(sync_queue, queue_name, handler, poll_interval=0.1)  # type: ignore # poll_interval in constructor

    # Run the worker for a short period to allow it to process the message
    worker.start()  # No args
    time.sleep(0.5)
    worker.stop()

    assert len(processed_payloads) == 1
    assert processed_payloads[0] == payload

    stats = sync_queue.get_queue_stats(queue_name)
    assert stats["completed"] == 1
    assert stats["pending"] == 0
    assert stats["dead_letter"] == 0


def test_worker_fail_message_and_retry(sync_queue: MessageQueue):
    queue_name = "test_worker_fail_retry"
    payload = {"data": "worker_fail_task"}
    sync_queue.send_message(queue_name, payload, max_retries=1)

    processed_count = 0

    def handler(current_payload):  # noqa: ANN202, ARG001
        nonlocal processed_count
        processed_count += 1
        return processed_count != 1

    worker = MessageWorker(sync_queue, queue_name, handler, poll_interval=0.1)  # type: ignore # poll_interval in constructor

    worker.start()  # No args
    time.sleep(5)  # Allow for initial failure and retry
    worker.stop()

    assert processed_count == 2  # Initial attempt + 1 retry

    stats = sync_queue.get_queue_stats(queue_name)
    assert stats["completed"] == 1
    assert stats["pending"] == 0
    assert stats["dead_letter"] == 0


def test_worker_dead_letter(sync_queue: MessageQueue):
    queue_name = "test_worker_dead_letter"
    payload = {"data": "worker_dlq_task"}
    sync_queue.send_message(queue_name, payload, max_retries=0)  # No retries

    processed_count = 0

    def handler(current_payload) -> bool:  # noqa: ARG001
        nonlocal processed_count
        processed_count += 1
        return False  # Always fail

    worker = MessageWorker(sync_queue, queue_name, handler, poll_interval=0.1)  # type: ignore # poll_interval in constructor

    worker.start()  # No args
    time.sleep(0.5)  # Allow for processing and immediate DLQ
    worker.stop()

    assert processed_count == 1

    stats = sync_queue.get_queue_stats(queue_name)
    assert stats["completed"] == 0
    assert stats["pending"] == 0
    assert stats["dead_letter"] == 1


@pytest.mark.asyncio
async def test_async_worker_process_message(async_queue: AsyncMessageQueue):
    queue_name = "test_async_worker_process"
    payload = {"data": "async_worker_task"}
    await async_queue.send_message(queue_name, payload)

    processed_payloads = []

    async def handler(current_payload) -> bool:
        processed_payloads.append(current_payload)
        return True  # Indicate successful processing

    worker = AsyncMessageWorker(async_queue, queue_name, handler, poll_interval=0.1)  # poll_interval in constructor

    # Run the worker for a short period to allow it to process the message
    task = worker.start()  # No args
    await asyncio.sleep(0.5)
    worker.stop()

    with contextlib.suppress(asyncio.CancelledError):
        await task

    assert len(processed_payloads) == 1
    assert processed_payloads[0] == payload

    stats = await async_queue.get_queue_stats(queue_name)
    assert stats["completed"] == 1
    assert stats["pending"] == 0
    assert stats["dead_letter"] == 0


@pytest.mark.asyncio
async def test_async_worker_fail_message_and_retry(async_queue: AsyncMessageQueue):
    queue_name = "test_async_worker_fail_retry"
    payload = {"data": "async_worker_fail_task"}
    await async_queue.send_message(queue_name, payload, max_retries=1)

    processed_count = 0

    async def handler(current_payload):  # noqa: ANN202, ARG001
        nonlocal processed_count
        processed_count += 1
        return processed_count != 1

    worker = AsyncMessageWorker(async_queue, queue_name, handler, poll_interval=0.1)  # poll_interval in constructor

    task = worker.start()  # No args
    await asyncio.sleep(5)  # Allow for initial failure and retry
    worker.stop()

    with contextlib.suppress(asyncio.CancelledError):
        await task

    assert processed_count == 2  # Initial attempt + 1 retry

    stats = await async_queue.get_queue_stats(queue_name)
    assert stats["completed"] == 1
    assert stats["pending"] == 0
    assert stats["dead_letter"] == 0


@pytest.mark.asyncio
async def test_async_worker_dead_letter(async_queue: AsyncMessageQueue):
    queue_name = "test_async_worker_dead_letter"
    payload = {"data": "async_worker_dlq_task"}
    await async_queue.send_message(queue_name, payload, max_retries=0)  # No retries

    processed_count = 0

    async def handler(current_payload) -> bool:  # noqa: ARG001
        nonlocal processed_count
        processed_count += 1
        return False  # Always fail

    worker = AsyncMessageWorker(async_queue, queue_name, handler, poll_interval=0.1)  # poll_interval in constructor

    task = worker.start()  # No args
    await asyncio.sleep(0.5)  # Allow for processing and immediate DLQ
    worker.stop()

    with contextlib.suppress(asyncio.CancelledError):
        await task

    assert processed_count == 1

    stats = await async_queue.get_queue_stats(queue_name)
    assert stats["completed"] == 0
    assert stats["pending"] == 0
    assert stats["dead_letter"] == 1
