from services import keywords as kw
from services import google_search as google
from services import metrics as met
import asyncio
from database.connection import init_db_pool,close_db_pool


async def analyze_data(container_id):
    # Get Keywords
    container_keywords = await kw.get_keywords(container_id)

    # Loop through each keyword
    for keyword_id, keyword_value in container_keywords:
        serp_results = await google.get_serp_results(keyword_value,2)
    
        for result in serp_results:
            result_id = await google.save_search_results_to_db(result, keyword_id)
            metrics = await met.fetch_metrics(result,keyword_value)
            await met.save_metrics_to_db(metrics, keyword_value, container_id,result_id)
            
    return f"Analysis done for Container ID: {container_id}"


async def main():
    await init_db_pool()
    try:
        await analyze_data(1)

    finally:
        await close_db_pool()


if __name__ == '__main__':
    asyncio.run(main()) 