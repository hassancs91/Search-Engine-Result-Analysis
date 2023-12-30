import psycopg2
import requests

# Database connection parameters
db_params = {
    'dbname': 'studies',
    'user': 'RfA2g1JHO0jNDWak',
    'password': 'vJ6&bG2$hJ1]kN4:',
    'host': 'studies-cl9xf-postgresql.external.kinsta.app',
    'port': 32346
}

def save_metrics_to_db(metrics, keyword, container_id,result_id):
    conn = None
    try:
        conn = psycopg2.connect(**db_params)
        cur = conn.cursor()
        # Assuming your table is named 'metrics' and has the appropriate columns.
        insert_query = """INSERT INTO metrics (container_id, keyword, total_backlinks, edu_backlinks, gov_backlinks, referring_domains, do_follow_backlinks, no_follow_backlinks, domain_authority, page_authority, title_tag, meta_description, keyword_in_title, keyword_in_meta,result_id)
                          VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"""
        cur.execute(insert_query, (container_id, keyword, metrics['total_backlinks'], metrics['edu_backlinks'],
                                   metrics['gov_backlinks'], metrics['referring_domains'], metrics['dofollow_backlinks'],
                                   metrics['nofollow_backlinks'], metrics['domain_authority'], metrics['page_authority'],
                                   metrics['title'], metrics['meta_description'], metrics['keyword_in_title'], metrics['keyword_in_meta'],result_id))
        conn.commit()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Database error: {error}")
    finally:
        if conn is not None:
            conn.close()

def save_search_results_to_db(results, keyword_id):
    conn = None
    generated_id = None  # To store the generated ID
    try:
        conn = psycopg2.connect(**db_params)
        cur = conn.cursor()
        insert_query = """INSERT INTO search_results (keyword_id, search_engine, rank,snippet, title, url, domain)
                          VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING id;"""
        cur.execute(insert_query, (keyword_id,"google", results["position"],  results["snippet"],results["title"], results["link"],
                                   results["domain"]))
        
        conn.commit()
        generated_id = cur.fetchone()[0]  # Fetch the generated ID
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Database error: {error}")
    finally:
        if conn is not None:
            conn.close()
    return generated_id  # Return the generated ID

def get_keywords(container_id):
    conn = None
    keyword_data = []  # This will hold tuples of (id, keyword)
    try:
        conn = psycopg2.connect(**db_params)  # Make sure db_params is defined somewhere
        cur = conn.cursor()
        # Update the query to select both ID and keyword
        cur.execute(f"SELECT id, keyword FROM keywords WHERE container_id = {container_id}")
        # Fetch the results and create a list of (id, keyword) tuples
        keyword_data = [(row[0], row[1]) for row in cur.fetchall()]
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
    return keyword_data

def get_google_serp(keyword):
    try:
        response = requests.get(
            "https://api.valueserp.com/search",
            params={
                "api_key": "CB548B488B0740B08E3567D227B1C31F",
                "q": keyword,
                "num": 5  # Assuming you want top 50 results
            }
        )
        response.raise_for_status()
        results = response.json().get('organic_results', [])
        return results[:5]  # Ensure only top 50 results are returned
    except requests.RequestException as e:
        print(f"Error fetching SERP for {keyword}: {e}")
        return []

def fetch_metrics(result,keyword):
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

def analyze_data(container_id):
    # Get Keywords
    container_keywords = get_keywords(container_id)

    # Loop through each keyword
    for keyword_id, keyword_value in container_keywords:
        serp_results = get_google_serp(keyword_value)
    
        for result in serp_results:
            result_id = save_search_results_to_db(result, keyword_id)
            metrics = fetch_metrics(result,keyword_value)
            save_metrics_to_db(metrics, keyword_value, container_id,result_id)
            
    return f"Analysis done for Container ID: {container_id}"
