
from database.connection import get_db_pool


async def get_keywords(container_id):
    pool = await get_db_pool()  # Get the shared connection pool
    keyword_data = []  # This will hold tuples of (id, keyword)
    async with pool.acquire() as conn:
        # Execute the query directly using the connection object
        query = "SELECT id, keyword FROM keywords WHERE container_id = $1"
        rows = await conn.fetch(query, container_id)
        keyword_data = [(row['id'], row['keyword']) for row in rows]

    return keyword_data




