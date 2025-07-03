import os

import pytest
from dotenv import load_dotenv
from sqlalchemy import create_engine

from pgque.models import get_message_class

load_dotenv()

# It is recommended to use a test-specific database
# to avoid interfering with the development database.
# You can set the database URL through environment variables.
# For example:
# export DATABASE_URL="postgresql+psycopg://user:password@host:port/test_db"
# export ASYNC_DATABASE_URL="postgresql+asyncpg://user:password@host:port/test_db"
# If the environment variable is not set,
# the default local database connection string will be used.
SYNC_DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql+psycopg://postgres:postgres@localhost:5432/postgres",
)
ASYNC_DATABASE_URL = os.environ.get(
    "ASYNC_DATABASE_URL",
    "postgresql+asyncpg://postgres:postgres@localhost:5432/postgres",
)
TEST_TABLE_NAME = "test_messages"


@pytest.fixture(scope="session", autouse=True)
def setup_database():
    """
    Create the test table before running tests and drop it after tests are complete.
    """
    message_class = get_message_class(TEST_TABLE_NAME)
    engine = create_engine(SYNC_DATABASE_URL)
    message_class.metadata.create_all(engine)
    yield
    message_class.metadata.drop_all(engine)
