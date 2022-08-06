import datetime
import itertools
import json
import logging
import os
import uuid

import boto3
import bs4
import pytz
import requests
from sqlalchemy import create_engine, text


logger = logging.getLogger()
logger.setLevel(logging.INFO)

UPSERT_LIMIT=1000


def chunks(iterable, batch_size=10):
    it = iter(iterable)
    chunk = tuple(itertools.islice(it, batch_size))
    while chunk:
        yield chunk
        chunk = tuple(itertools.islice(it, batch_size))


def get_sources(engine, source_id=None):
    with engine.connect() as connection:
        if source_id:
            sources = connection.execute(
                text(f"select * from source where id = '{source_id}'")
            ).fetchall()
        else:
            sources = connection.execute(
                text(f"select * from source order by updated asc")
            ).fetchall()
    return sources


def update_source(engine, source, extra):
    extra_str = json.dumps(extra)
    with engine.connect() as connection:
        current = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        connection.execute(text(f"update source SET updated = '{current}'::timestamp with TIME ZONE, extra = '{extra_str}' where id = '{str(source['id'])}'"))


def get_zendesk_credentials(source):
    extra = json.loads(source['extra']) if source['extra'] else {}
    subdomain = extra.get("subdomain")
    access_token = extra.get("access_token")
    return subdomain, access_token


def get_hubspot_access_token(source):
    extra = json.loads(source['extra']) if source['extra'] else {}
    refresh_token = extra["refresh_token"]
    parameters = {
        "grant_type": "refresh_token",
        "client_id": os.getenv("HUBSPOT_CLIENT_ID"),
        "client_secret": os.getenv("HUBSPOT_SECRET"),
        "redirect_uri": os.getenv("HUBSPOT_REDIRECT_URI"),
        "refresh_token": refresh_token
    }
    r = requests.post("https://api.hubapi.com/oauth/v1/token", data=parameters)
    data = r.json()
    access_token = data["access_token"]
    return access_token


def get_zendesk_hc_articles(source):
    subdomain, access_token = get_zendesk_credentials(source)
    if not subdomain and not access_token:
        raise Exception("subdomain and access token need to be provided")
    if not source.updated:
        url = f"https://{subdomain}/api/v2/help_center/articles.json?sort_by=updated_at&sort_order=desc"
    else:
        start_time = source.updated.timestamp()
        url = f"https://{subdomain}/api/v2/help_center/incremental/articles.json?start_time={start_time}"
    bearer_token = f"Bearer {access_token}"
    header = {'Authorization': bearer_token}
    articles = requests.get(url, headers=header).json().get("articles", [])
    return [
        {
            "owner": str(source["owner"]),
            "doc_type": "zendesk_help_center_article",
            "doc_id": article["id"],
            "doc_name": article["title"],
            "doc_last_updated": article["updated_at"],
            "source_id": str(source["id"])
        } for article in articles
    ]


def get_zendesk_tickets(source):
    subdomain, access_token = get_zendesk_credentials(source)
    bearer_token = f"Bearer {access_token}"
    header = {'Authorization': bearer_token}
    extra = json.loads(source["extra"]).copy()
    next_link = extra.get("next_link")
    has_more = extra.get("has_more")
    initial_index_completed = extra.get("initial_index_completed", False)
    source_last_updated = source["updated"]
    user_url = f"https://{subdomain}/api/v2/users/me.json"
    user_id = requests.get(user_url, headers=header).json()["user"]["id"]
    if next_link and has_more:
        url = next_link
    else:
        url = f"https://{subdomain}/api/v2/users/{user_id}/tickets/assigned.json?page[size]=50&sort=-updated_at"
    response = requests.get(url, headers=header).json()
    current_next_link = response.get("links", {}).get("next")
    current_has_more = response.get("meta", {}).get("has_more", False)
    tickets = []
    results = response.get("tickets", [])
    current_dt = datetime.datetime.now(datetime.timezone.utc)
    for result in results:
        ticket_last_updated = result["updated_at"]
        ticket_last_updated_dt = pytz.utc.localize(datetime.datetime.strptime(ticket_last_updated, "%Y-%m-%dT%H:%M:%SZ"))
        is_resolved = result["status"] in ["solved", "closed"]
        updated_within_ninety_days = (current_dt - ticket_last_updated_dt).days <= 90
        recently_updated = ticket_last_updated_dt > source_last_updated
        if is_resolved and ((updated_within_ninety_days and not initial_index_completed) or (recently_updated and initial_index_completed)):
            tickets.append({
                "owner": str(source["owner"]),
                "doc_type": "zendesk_ticket",
                "doc_id": result["id"],
                "doc_name": result["subject"],
                "doc_last_updated": ticket_last_updated,
                "source_id": str(source["id"])
            })
        elif not initial_index_completed and not updated_within_ninety_days:
            current_next_link = None
            current_has_more = False
            break
    extra["next_link"] = current_next_link
    extra["has_more"] = current_has_more
    if not current_has_more:
        extra["initial_index_completed"] = True
    return tickets, extra



def hubspot_article_recently_updated(doc_lasted_updated):
    doc_lasted_updated_dt =  pytz.utc.localize(datetime.datetime.strptime(doc_lasted_updated, "%Y-%m-%dT%H:%M:%SZ"))
    recently_updated = (datetime.datetime.now(datetime.timezone.utc) - doc_lasted_updated_dt).days <= 2
    return recently_updated


def get_hubspot_article_documents(engine, owner):
    with engine.connect() as connection:
        document = connection.execute(
            text(f"select doc_id from document where owner = '{owner}' and type = 'hubspot_help_center_article'")
        ).fetchall()
    return document


def get_hubspot_hc_articles(engine, source): 
    extra = json.loads(source['extra']) if source['extra'] else {}
    subdomain = extra.get("subdomain")
    if not subdomain:
        raise Exception("subdomain need to be provided")
    sitemap_url = f"https://{subdomain}/sitemap.xml"
    content = requests.get(sitemap_url).content
    soup = bs4.BeautifulSoup(content, features="xml")
    urls = soup.find_all("url")
    articles = []
    owner = str(source["owner"])
    hs_article_ids = {doc["doc_id"] for doc in get_hubspot_article_documents(engine, owner)}
    for url in urls:
        dt = datetime.datetime.strptime(str(url.find("lastmod").string), "%Y-%m-%d")
        doc_lasted_updated = datetime.datetime.strftime(dt, "%Y-%m-%dT%H:%M:%SZ")
        link = str(url.find("loc").string)
        doc_id = link.split("/")[-1]
        if hubspot_article_recently_updated(doc_lasted_updated) or doc_id not in hs_article_ids:
            doc_name = " ".join([word.capitalize() for word in link.split("/")[-1].split("-")])
            articles.append({
                "owner": owner,
                "doc_type": "hubspot_help_center_article",
                "doc_id": doc_id,
                "doc_url": link,
                "doc_name": doc_name,
                "doc_last_updated": doc_lasted_updated,
                "source_id": str(source["id"])
            })
    return articles



def get_hubspot_tickets(source):
    access_token = get_hubspot_access_token(source)
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {access_token}"
    }
    portal_id = requests.get("https://api.hubapi.com/account-info/v3/details", headers=headers).json()["portalId"]
    url = "https://api.hubapi.com/crm/v3/objects/tickets/search"
    extra = json.loads(source["extra"]).copy()
    after = extra.get("after", 0)
    initial_index_completed = extra.get("initial_index_completed", False)
    source_last_updated = round(source["updated"].timestamp()*1000)
    if not initial_index_completed:
        payload = {
            "sorts": ["-hs_lastmodifieddate"],
            "filterGroups": [
                {
                    "filters": [
                        {"operator": "HAS_PROPERTY", "propertyName": "closed_date"}
                    ]
                }
            ],
            "limit": 100,
            "after": after
        }
    else:
        payload = {
            "sorts": ["-hs_lastmodifieddate"],
            "filterGroups": [
                {
                    "filters": [
                        {"operator": "HAS_PROPERTY", "propertyName": "closed_date"},
                        {"operator": "GTE", "propertyName": "closed_date", "value": source_last_updated}
                    ]
                }
            ],
            "limit": 100
        }
    response = requests.post(url, headers=headers, json=payload).json()
    next_after = response.get("paging", {}).get("next", {}).get("after")
    results = response.get("results", [])
    tickets = []
    current_dt = datetime.datetime.now(datetime.timezone.utc)
    for result in results:
        ticket_last_updated = result["properties"]["hs_lastmodifieddate"]
        try:
            ticket_last_updated_dt = pytz.utc.localize(datetime.datetime.strptime(ticket_last_updated, "%Y-%m-%dT%H:%M:%S.%fZ"))
        except:
            ticket_last_updated_dt = pytz.utc.localize(datetime.datetime.strptime(ticket_last_updated, "%Y-%m-%dT%H:%M:%SZ"))
        if (current_dt - ticket_last_updated_dt).days <= 90:
            tickets.append({
                "owner": str(source["owner"]),
                "doc_type": "hubspot_ticket",
                "portal_id": portal_id,
                "doc_id": result["id"],
                "doc_name": result["properties"]["subject"],
                "doc_last_updated": ticket_last_updated,
                "source_id": str(source["id"])
            })
        else:
            next_after = None
            break
    
    extra["after"] = next_after
    if next_after is None:
        extra["initial_index_completed"] = True

    return tickets, extra


def generate_messages(documents):
    messages = []
    for document in documents:
        messages.append({
            "MessageBody": json.dumps(document),
            "Id": str(uuid.uuid4())
        })
    return messages


def handler(event, context):
    engine = create_engine(os.environ["SQLALCHEMY_DATABASE_URL"])
    sqs = boto3.resource("sqs", region_name="us-east-1")
    queue = sqs.get_queue_by_name(QueueName=os.getenv("SQS_QUEUE_NAME"))
    body = json.loads(event["body"]) if event.get("body") else {}
    documents = []
    source_id = body.get("source_id")
    sources = get_sources(engine, source_id=source_id)
    for source in sources:
        try:
            if source.name == "zendesk_integration":
                articles = get_zendesk_hc_articles(source)
                documents.extend(articles)
                tickets, extra = get_zendesk_tickets(source)
                documents.extend(tickets)
                update_source(engine, source, extra)
            elif source.name == "hubspot_integration":
                articles = get_hubspot_hc_articles(engine, source)
                documents.extend(articles)
                tickets, extra = get_hubspot_tickets(source)
                documents.extend(tickets)
                update_source(engine, source, extra)
        except Exception as e:
            logger.error(str(e))
        if len(documents) > UPSERT_LIMIT:
            break

    messages = generate_messages(documents)
    logger.info(f"Upserting {len(messages)} docs")
    for chunk in chunks(messages, batch_size=10):
        queue.send_messages(Entries=chunk)

    engine.dispose()