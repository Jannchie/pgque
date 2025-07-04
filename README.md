# pgque

A simple message queue based on PostgreSQL's `FOR UPDATE SKIP LOCKED`.

## Installation

Install the basic synchronous version:

```bash
pip install pgque
```

To use the asynchronous version, you need to install the `async` extra, which includes the `asyncpg` driver:

```bash
pip install pgque[async]
```

If you prefer to use `psycopg2` instead of `psycopg` (v3) for the synchronous version, you can install the `psycopg2` extra:

```bash
pip install pgque[psycopg2]
```

## Usage

### Creating Tables

To create the message table in your database, instantiate `MessageQueue` or `AsyncMessageQueue` and call `create_table()`.
You can optionally specify a custom table name.

```python
import asyncio
from pgque import MessageQueue, AsyncMessageQueue

database_url = "postgresql://user:password@host:port/dbname"

# Synchronous: Create a table with the default name "messages"
# Pass create_table=True to the constructor
queue = MessageQueue(database_url, create_table=True)

# Asynchronous: Create a table with a custom name
async def create_async_table():
    async_queue = AsyncMessageQueue(database_url, table_name="my_messages")
    await async_queue.create_table()
    await async_queue.close()

asyncio.run(create_async_table())
```

### Synchronous Usage

```python
from pgque import MessageQueue

# Connect to a queue with the default table name
queue = MessageQueue("postgresql+psycopg://user:password@host:port/dbname")

# Connect to a queue with a custom table name
custom_queue = MessageQueue(
    "postgresql+psycopg://user:password@host:port/dbname",
    table_name="my_messages",
)

# Send a message
custom_queue.send_message("my_queue", {"hello": "world"})

# Receive a message
message = custom_queue.receive_message("my_queue")
if message:
    print(message["payload"])
    custom_queue.complete_message(message["id"])
```

### Asynchronous Usage

```python
import asyncio
from pgque import AsyncMessageQueue

async def main():
    # Connect to a queue with a custom table name
    queue = AsyncMessageQueue(
        "postgresql+asyncpg://user:password@host:port/dbname",
        table_name="my_messages",
    )

    # Send a message
    await queue.send_message("my_queue", {"hello": "world"})

    # Receive a message
    message = await queue.receive_message("my_queue")
    if message:
        print(message["payload"])
        await queue.complete_message(message["id"])

    await queue.close()

asyncio.run(main())
```
