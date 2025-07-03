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

First, you need to create the message table in your database. You can optionally specify a custom table name.

```python
from pgque import create_tables

database_url = "postgresql://user:password@host:port/dbname"

# Create a table with the default name "messages"
create_tables(database_url)

# Or, create a table with a custom name
create_tables(database_url, table_name="my_messages")
```

### Synchronous

```python
from pgque import get_sync_queue

# Connect to a queue with the default table name
queue = get_sync_queue("postgresql+psycopg://user:password@host:port/dbname")

# Connect to a queue with a custom table name
custom_queue = get_sync_queue(
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

### Asynchronous

```python
import asyncio
from pgque import get_async_queue

async def main():
    # Connect to a queue with a custom table name
    queue = get_async_queue(
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