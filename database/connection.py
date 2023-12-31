import os

from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Now you can access your API keys (and other environment variables)
POSTGRESQL_DB_NAME = os.getenv('POSTGRESQL_DB_NAME')
POSTGRESQL_DB_USER = os.getenv('POSTGRESQL_DB_USER')
POSTGRESQL_DB_PASS = os.getenv('POSTGRESQL_DB_PASS')
POSTGRESQL_DB_HOST = os.getenv('POSTGRESQL_DB_HOST')

import asyncpg

pool = None

async def init_db_pool():
    global pool
    pool = await asyncpg.create_pool(
        user=POSTGRESQL_DB_USER,
        password=POSTGRESQL_DB_PASS,
        database=POSTGRESQL_DB_NAME,
        host=POSTGRESQL_DB_HOST,
        port= 32346
    )

async def get_db_pool():
    global pool
    if pool is None:
        await init_db_pool()
    return pool


async def close_db_pool():
    global pool
    if pool:
        await pool.close()