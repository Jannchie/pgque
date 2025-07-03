import os

from pgque import AsyncMessageQueue

DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql+psycopg://postgres:postgres@localhost:5432/postgres",
)


async def main():
    q = AsyncMessageQueue(
        DATABASE_URL,
        table_name="messages2",
    )
    stats = await q.get_queue_stats("test")
    print(stats)


if __name__ == "__main__":
    import asyncio

    asyncio.run(main())
