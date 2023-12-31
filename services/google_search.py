import aiohttp
from database.connection import get_db_pool
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Now you can access your API keys (and other environment variables)
value_serp_api_key = os.getenv('VALUE_SERP_API_KEY')


async def save_search_results(results, keyword_id):
    pool = await get_db_pool()  # Get the shared connection pool
    generated_id = None  # To store the generated ID

    # Set default values to None if the keys don't exist or if the values are None
    position = results.get("position", None)
    snippet = results.get("snippet", None)
    title = results.get("title", None)
    link = results.get("link", None)
    domain = results.get("domain", None)

    try:
        async with pool.acquire() as conn:  # Ensure the connection is released back to the pool
            insert_query = """INSERT INTO search_results (keyword_id, search_engine, rank, snippet, title, url, domain)
                              VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING id;"""
            # Execute the insert query and fetch the generated ID in one go
            generated_id = await conn.fetchval(insert_query, keyword_id, "google", position, snippet, title, link, domain)
    except Exception as e:  # Catch any exceptions (e.g., database errors)
        print(f"An error occurred: {e}")  # Log or handle the error as needed
        # Optionally, handle specific exceptions like connection errors, timeouts, etc.

    return generated_id  # Return the generated ID or None if an error occurred

async def get_serp_results(keyword,num_results):
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get("https://api.valueserp.com/search", params={"q": keyword, "num": num_results, "api_key": value_serp_api_key}) as response:
                response.raise_for_status()
                results = await response.json()
                return results.get('organic_results', [])[:num_results]
        except aiohttp.ClientError as e:
            print(f"Error fetching SERP for {keyword}: {e}")
            return []

