import aiohttp
import asyncio
import psycopg2
import requests
from database.connection import get_db_pool


async def save_search_results_to_db(results, keyword_id):
    pool = await get_db_pool()  # Get the shared connection pool
    generated_id = None  # To store the generated ID
    async with pool.acquire() as conn:
        insert_query = """INSERT INTO search_results (keyword_id, search_engine, rank, snippet, title, url, domain)
                          VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING id;"""
        # Execute the insert query and fetch the generated ID in one go
        generated_id = await conn.fetchval(insert_query, keyword_id, "google", results["position"], results["snippet"],
                                           results["title"], results["link"], results["domain"])

    return generated_id  # Return the generated ID

async def save_search_results_to_db_old(results, keyword_id):
    pool = await get_db_pool()  # Get the shared connection pool
    generated_id = None  # To store the generated ID
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            insert_query = """INSERT INTO search_results (keyword_id, search_engine, rank,snippet, title, url, domain)
                              VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING id;"""
            await cur.execute(insert_query, (keyword_id, "google", results["position"], results["snippet"],
                                             results["title"], results["link"], results["domain"]))
            await conn.commit()
            generated_id = (await cur.fetchone())[0]  # Fetch the generated ID

    return generated_id  # Return the generated ID






async def get_serp_results(keyword,num_results):
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get("https://api.valueserp.com/search", params={"q": keyword, "num": num_results, "api_key": "CB548B488B0740B08E3567D227B1C31F"}) as response:
                response.raise_for_status()
                results = await response.json()
                return results.get('organic_results', [])[:num_results]
        except aiohttp.ClientError as e:
            print(f"Error fetching SERP for {keyword}: {e}")
            return []

