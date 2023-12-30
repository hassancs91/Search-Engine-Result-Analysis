import requests

import aiohttp
import asyncio
from aiohttp.client_exceptions import ClientError
from database.connection import get_db_pool


async def save_metrics_to_db(metrics, keyword, container_id, result_id):
    pool = await get_db_pool()  # Get the shared connection pool
    async with pool.acquire() as conn:
        insert_query = """INSERT INTO metrics (container_id, keyword, total_backlinks, edu_backlinks, gov_backlinks, referring_domains, do_follow_backlinks, no_follow_backlinks, domain_authority, page_authority, title_tag, meta_description, keyword_in_title, keyword_in_meta, result_id)
                          VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15);"""
        await conn.execute(insert_query, container_id, keyword, metrics['total_backlinks'], metrics['edu_backlinks'],
                           metrics['gov_backlinks'], metrics['referring_domains'], metrics['dofollow_backlinks'],
                           metrics['nofollow_backlinks'], metrics['domain_authority'], metrics['page_authority'],
                           metrics['title'], metrics['meta_description'], metrics['keyword_in_title'], metrics['keyword_in_meta'], result_id)



async def save_metrics_to_db_old(metrics, keyword, container_id, result_id):
    pool = await get_db_pool()  # Get the shared connection pool
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            insert_query = """INSERT INTO metrics (container_id, keyword, total_backlinks, edu_backlinks, gov_backlinks, referring_domains, do_follow_backlinks, no_follow_backlinks, domain_authority, page_authority, title_tag, meta_description, keyword_in_title, keyword_in_meta,result_id)
                              VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"""
            await cur.execute(insert_query, (container_id, keyword, metrics['total_backlinks'], metrics['edu_backlinks'],
                                           metrics['gov_backlinks'], metrics['referring_domains'], metrics['dofollow_backlinks'],
                                           metrics['nofollow_backlinks'], metrics['domain_authority'], metrics['page_authority'],
                                           metrics['title'], metrics['meta_description'], metrics['keyword_in_title'], metrics['keyword_in_meta'], result_id))
            await conn.commit()



def fetch_metrics_old(result,keyword):
    # Extract domain and URL from the result
    domain = result['domain']
    url = result['link']
    title = result['title']
    snippet = result['snippet']

    # Prepare for API requests
    rapidapi_key = "a617d6467dmshac84323ce581a72p11caa9jsn1adf8bbcbd47"
    backlinks_headers = {
        "X-RapidAPI-Key": rapidapi_key,
        "X-RapidAPI-Host": "backlinks-checker1.p.rapidapi.com"
    }
    domain_auth_headers = {
        "X-RapidAPI-Key": rapidapi_key,
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
RAPIDAPI_KEY = 'a617d6467dmshac84323ce581a72p11caa9jsn1adf8bbcbd47'
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
        "X-RapidAPI-Key": RAPIDAPI_KEY,
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