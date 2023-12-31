import requests
import aiohttp
import asyncio
from aiohttp.client_exceptions import ClientError
from database.connection import get_db_pool


from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Now you can access your API keys (and other environment variables)
RAPID_API_KEY = os.getenv('RAPID_API_KEY')


async def save_metrics_to_db_old(metrics, keyword, container_id, result_id):
    pool = await get_db_pool()  # Get the shared connection pool
    async with pool.acquire() as conn:
        insert_query = """INSERT INTO metrics (container_id, keyword, total_backlinks, edu_backlinks, gov_backlinks, referring_domains, do_follow_backlinks, no_follow_backlinks, domain_authority, page_authority, title_tag, meta_description, keyword_in_title, keyword_in_meta, result_id)
                          VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15);"""
        await conn.execute(insert_query, container_id, keyword, metrics['total_backlinks'], metrics['edu_backlinks'],
                           metrics['gov_backlinks'], metrics['referring_domains'], metrics['dofollow_backlinks'],
                           metrics['nofollow_backlinks'], metrics['domain_authority'], metrics['page_authority'],
                           metrics['title'], metrics['meta_description'], metrics['keyword_in_title'], metrics['keyword_in_meta'], result_id)



async def save_metrics_to_db(metrics, keyword, container_id, result_id):
    pool = await get_db_pool()  # Get the shared connection pool

    # Set default values to None if the keys don't exist or if the values are None
    total_backlinks = metrics.get('total_backlinks', None)
    edu_backlinks = metrics.get('edu_backlinks', None)
    gov_backlinks = metrics.get('gov_backlinks', None)
    referring_domains = metrics.get('referring_domains', None)
    dofollow_backlinks = metrics.get('dofollow_backlinks', None)
    nofollow_backlinks = metrics.get('nofollow_backlinks', None)
    domain_authority = metrics.get('domain_authority', None)
    page_authority = metrics.get('page_authority', None)
    title = metrics.get('title', None)
    meta_description = metrics.get('meta_description', None)
    keyword_in_title = metrics.get('keyword_in_title', None)
    keyword_in_meta = metrics.get('keyword_in_meta', None)

    try:
        async with pool.acquire() as conn:  # Ensure the connection is released back to the pool
            insert_query = """INSERT INTO metrics (container_id, keyword, total_backlinks, edu_backlinks, gov_backlinks, referring_domains, do_follow_backlinks, no_follow_backlinks, domain_authority, page_authority, title_tag, meta_description, keyword_in_title, keyword_in_meta, result_id)
                              VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15);"""
            # Execute the insert query
            await conn.execute(insert_query, container_id, keyword, total_backlinks, edu_backlinks, gov_backlinks,
                               referring_domains, dofollow_backlinks, nofollow_backlinks, domain_authority,
                               page_authority, title, meta_description, keyword_in_title, keyword_in_meta, result_id)
    except Exception as e:  # Catch any exceptions (e.g., database errors)
        print(f"An error occurred: {e}")  # Log or handle the error as needed
        # Optionally, handle specific exceptions like connection errors, timeouts, etc.

    # The function doesn't return anything, but you might want to handle or log success/failure


async def save_metrics_bulk_to_db(metrics_list, keyword, container_id, result_id):
    pool = await get_db_pool()

    # Construct one parameterized query for bulk insert.
    insert_query = """
        INSERT INTO metrics (
            container_id, keyword, total_backlinks, edu_backlinks, gov_backlinks, 
            referring_domains, do_follow_backlinks, no_follow_backlinks, 
            domain_authority, page_authority, title_tag, meta_description, 
            keyword_in_title, keyword_in_meta, result_id
        ) VALUES 
    """

    # Generate a list of records to be inserted.
    records = []
    for metrics in metrics_list:
        record = (
            container_id, keyword, 
            metrics.get('total_backlinks'), metrics.get('edu_backlinks'), 
            metrics.get('gov_backlinks'), metrics.get('referring_domains'), 
            metrics.get('dofollow_backlinks'), metrics.get('nofollow_backlinks'), 
            metrics.get('domain_authority'), metrics.get('page_authority'), 
            metrics.get('title'), metrics.get('meta_description'), 
            metrics.get('keyword_in_title'), metrics.get('keyword_in_meta'), result_id
        )
        records.append(record)

    # Convert the records into a format suitable for a bulk insert.
    # This part will vary depending on your database adapter.
    values_str = ','.join(['%s'] * len(records))
    insert_query += f"({values_str})"

    # Flatten the list of records into a single list of parameters for the execute function.
    params = [param for record in records for param in record]

    # Use a connection from the pool to execute your query.
    async with pool.acquire() as conn:
        try:
            # Execute the bulk insert.
            await conn.execute(insert_query, *params)
        except Exception as e:
            # Proper error handling/logging should be here.
            print(f"An error occurred: {e}")





def fetch_metrics_old(result,keyword):
    # Extract domain and URL from the result
    domain = result['domain']
    url = result['link']
    title = result['title']
    snippet = result['snippet']

    # Prepare for API requests
    backlinks_headers = {
        "X-RapidAPI-Key": RAPID_API_KEY,
        "X-RapidAPI-Host": "backlinks-checker1.p.rapidapi.com"
    }
    domain_auth_headers = {
        "X-RapidAPI-Key": RAPID_API_KEY,
        "X-RapidAPI-Host": "domain-authority1.p.rapidapi.com"
    }

    # Backlink info
    backlinks_url = "https://backlinks-checker1.p.rapidapi.com/seo/get-backlinks"
    backlink_query = {"url": url}
    backlink_info = requests.get(backlinks_url, headers=backlinks_headers, params=backlink_query).json()

    # Domain authority info
    domain_authority_url = "https://domain-authority1.p.rapidapi.com/seo/get-domain-info"
    domain_authority_query = {"domain": domain}
    domain_authority_info = requests.get(domain_authority_url, headers=domain_auth_headers, params=domain_authority_query).json()

    # Parse backlink info
    total_backlinks = backlink_info['result']['backlinks_info']['backlinks']
    edu_backlinks = backlink_info['result']['backlinks_info']['edu_backlinks']
    gov_backlinks = backlink_info['result']['backlinks_info']['gov_backlinks']
    ref_domains = backlink_info['result']['backlinks_info']['refdomains']
    dofollow_backlinks = backlink_info['result']['backlinks_info']['dofollow_backlinks']
    nofollow_backlinks = backlink_info['result']['backlinks_info']['nofollow_backlinks']

    # Parse domain authority info
    domain_authority = domain_authority_info['result']['domain_stats']['domain_power']
    page_authority = domain_authority_info['result']['domain_stats']['page_power']

    # Check if the keyword is in the title and meta description
    keyword_in_title = keyword.lower() in title.lower()
    keyword_in_meta = keyword.lower() in snippet.lower()

    # Collect and return metrics
    metrics = {
        'total_backlinks': total_backlinks,
        'edu_backlinks': edu_backlinks,
        'gov_backlinks': gov_backlinks,
        'referring_domains': ref_domains,
        'dofollow_backlinks': dofollow_backlinks,
        'nofollow_backlinks': nofollow_backlinks,
        'domain_authority': domain_authority,
        'page_authority': page_authority,
        'title': title,
        'meta_description': snippet,
        'keyword_in_title': keyword_in_title,
        'keyword_in_meta': keyword_in_meta
    }
    return metrics


# Configuration variables
BACKLINKS_URL = 'https://backlinks-checker1.p.rapidapi.com/seo/get-backlinks'
DOMAIN_AUTHORITY_URL = 'https://domain-authority1.p.rapidapi.com/seo/get-domain-info'

async def fetch_metrics(result, keyword):
    # Set default values for keys that might be missing or null
    domain = result.get('domain', 'Unknown')
    url = result.get('link', 'Unknown')
    title = result.get('title', '')
    snippet = result.get('snippet', '')

    async with aiohttp.ClientSession() as session:
        backlink_info, domain_authority_info = await asyncio.gather(
            get_api_data(session, BACKLINKS_URL, {"url": url}, "backlinks-checker1.p.rapidapi.com"),
            get_api_data(session, DOMAIN_AUTHORITY_URL, {"domain": domain}, "domain-authority1.p.rapidapi.com")
        )

    if not backlink_info or not domain_authority_info:
        return None

    return parse_data(backlink_info, domain_authority_info, title, snippet, keyword)

async def get_api_data(session, url, params, host):
    headers = {
        "X-RapidAPI-Key": RAPID_API_KEY,
        "X-RapidAPI-Host": host
    }
    try:
        async with session.get(url, headers=headers, params=params) as response:
            response.raise_for_status()
            return await response.json()
    except ClientError as e:
        print(f"Client error occurred: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")
    return None

def parse_data(backlink_info, domain_authority_info, title, snippet, keyword):
    # Safely extract data from the API responses with default values for missing keys
    total_backlinks = backlink_info.get('result', {}).get('backlinks_info', {}).get('backlinks', 0)
    edu_backlinks = backlink_info.get('result', {}).get('backlinks_info', {}).get('edu_backlinks', 0)
    gov_backlinks = backlink_info.get('result', {}).get('backlinks_info', {}).get('gov_backlinks', 0)
    ref_domains = backlink_info.get('result', {}).get('backlinks_info', {}).get('refdomains', 0)
    dofollow_backlinks = backlink_info.get('result', {}).get('backlinks_info', {}).get('dofollow_backlinks', 0)
    nofollow_backlinks = backlink_info.get('result', {}).get('backlinks_info', {}).get('nofollow_backlinks', 0)
    domain_authority = domain_authority_info.get('result', {}).get('domain_stats', {}).get('domain_power', 0)
    page_authority = domain_authority_info.get('result', {}).get('domain_stats', {}).get('page_power', 0)

    # Check if the keyword is in the title and meta description (handling cases where they might be None)
    keyword_in_title = keyword.lower() in (title or '').lower()
    keyword_in_meta = keyword.lower() in (snippet or '').lower()

    # Collect and return metrics
    metrics = {
        'total_backlinks': total_backlinks,
        'edu_backlinks': edu_backlinks,
        'gov_backlinks': gov_backlinks,
        'referring_domains': ref_domains,
        'dofollow_backlinks': dofollow_backlinks,
        'nofollow_backlinks': nofollow_backlinks,
        'domain_authority': domain_authority,
        'page_authority': page_authority,
        'title': title or '',
        'meta_description': snippet or '',
        'keyword_in_title': keyword_in_title,
        'keyword_in_meta': keyword_in_meta
    }
    return metrics