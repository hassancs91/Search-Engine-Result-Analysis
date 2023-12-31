from services import keywords as kw
from services import google_search as google
from services import metrics as met
import asyncio
from database.connection import init_db_pool,close_db_pool
from database.connection import get_db_pool
import psycopg2
from psycopg2 import sql
import time

import os

from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

CONNECTION_STRING = os.getenv('CONNECTION_STRING')


#optimize code
#UI to create containers and add keywords
#UI for data and analysis (streamlit)


def get_db_connection():
    return psycopg2.connect(CONNECTION_STRING)



def create_container(name, description):
    """Insert a new container into the containers table."""
    conn = get_db_connection()
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO public.containers (name, description) VALUES (%s, %s)",
            (name, description)
        )
        conn.commit()

def add_keywords_to_container(keywords, container_id):
    """Insert multiple new keywords into the keywords table."""
    conn = get_db_connection()
    with conn.cursor() as cur:
        # Prepare a list of tuples (keyword, container_id) for each keyword
        keyword_values = [(keyword.strip(), container_id) for keyword in keywords if keyword.strip()]
        # Construct the SQL query with multiple insert values
        insert_query = sql.SQL("INSERT INTO public.keywords (keyword, container_id) VALUES {}").format(
            sql.SQL(',').join(map(sql.Literal, keyword_values))
        )
        cur.execute(insert_query)
        conn.commit()

def get_containers():
    conn = get_db_connection()
    with conn.cursor() as cur:
        cur.execute("SELECT id, name FROM public.containers")
        return cur.fetchall()

def fetch_latest_progress():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT message, progress_percentage 
        FROM progress_log 
        ORDER BY timestamp DESC 
        LIMIT 1
    """)
    result = cur.fetchone()
    cur.close()
    conn.close()
    return result if result else ("No progress yet.", 0.0)

def is_operation_running():
    conn = get_db_connection()  # Ensure you have this function defined to connect to your database
    cur = conn.cursor()
    cur.execute("""
        SELECT progress_percentage 
        FROM progress_log 
        WHERE progress_percentage < 100
    """)
    result = cur.fetchone()
    cur.close()
    conn.close()
    return result is not None


async def analyze_data(container_id):
    # Get the shared connection pool
    pool = await get_db_pool()

    # Placeholder for getting container keywords
    container_keywords = await kw.get_keywords(container_id)

    # Record the start time
    start_time = time.time()

    # Process each keyword
    total_keywords = len(container_keywords)
    async with pool.acquire() as conn:  # Acquire a connection from the pool
        # Insert a new progress record and retrieve its ID
        row_id = await conn.fetchval('''
            INSERT INTO progress_log (container_id, message, progress_percentage) 
            VALUES ($1, 'Starting Analysis', 0)
            RETURNING id
        ''', container_id)

        for index, (keyword_id, keyword_value) in enumerate(container_keywords):
            try:
                # Placeholder for getting SERP results and saving them
                serp_results = await google.get_serp_results(keyword_value, 100)
                for result in serp_results:
                    try:
                        result_id = await google.save_search_results(result, keyword_id)
                        if result_id:
                            # Placeholder for fetching and saving metrics
                            metrics = await met.fetch_metrics(result, keyword_value)
                            await met.save_metrics_to_db(metrics, keyword_value, container_id, result_id)
                    except Exception as inner_error:
                        # Handle errors in result processing
                        print(f"Error processing result for keyword {keyword_value}: {inner_error}")

                # Calculate and update progress using the row ID
                progress = (index + 1) / total_keywords * 100
                await conn.execute('''
                    UPDATE progress_log 
                    SET message = $2, progress_percentage = $3 
                    WHERE id = $1
                ''', row_id, f"Processed {index + 1} of {total_keywords} keywords", progress)
            except Exception as outer_error:
                # Handle errors in keyword processing
                print(f"Error processing keyword {keyword_value}: {outer_error}")

        # Calculate the total duration
        total_duration = time.time() - start_time

        # Update the record to indicate completion using the row ID and include the duration
        await conn.execute('''
            UPDATE progress_log 
            SET message = 'Analysis Complete', progress_percentage = 100, duration_seconds = $2 
            WHERE id = $1
        ''', row_id, total_duration)

    return f"Analysis done for Container ID: {container_id}. Duration: {total_duration:.2f} seconds"



async def analyze_data_old(container_id):

    # Get Keywords
    container_keywords = await kw.get_keywords(container_id)


    # Loop through each keyword
    for keyword_id, keyword_value in container_keywords:
        serp_results = await google.get_serp_results(keyword_value,100)
    
        for result in serp_results:
            result_id = await google.save_search_results(result, keyword_id)
            if result_id:
                metrics = await met.fetch_metrics(result,keyword_value)
                await met.save_metrics_to_db(metrics, keyword_value, container_id, result_id)
        

            
            
    return f"Analysis done for Container ID: {container_id}"




async def main():
    await init_db_pool()
    try:
        await analyze_data(1)

    finally:
        await close_db_pool()


if __name__ == '__main__':
    asyncio.run(main()) 