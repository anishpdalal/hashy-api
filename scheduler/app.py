import itertools
import json
import logging
import os
import uuid

import boto3
import requests
from sqlalchemy import create_engine, text


logger = logging.getLogger()
logger.setLevel(logging.INFO)


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
                text(f"select * from source")
            ).fetchall()
    return sources


def get_credentials(source):
    extra = json.loads(source['extra']) if source['extra'] else {}
    subdomain = extra.get("subdomain")
    access_token = extra.get("access_token")
    return subdomain, access_token


def get_zendesk_hc_articles(source):
    subdomain, access_token = get_credentials(source)
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


def handler(event, context):
    engine = create_engine(os.environ["SQLALCHEMY_DATABASE_URL"])
    sqs = boto3.resource("sqs", region_name="us-east-1")
    queue = sqs.get_queue_by_name(QueueName=os.getenv("SQS_QUEUE_NAME"))
    body = json.loads(event["body"]) if event.get("body") else {}
    messages = []
    documents = []
    source_id = body.get("source_id")
    sources = get_sources(engine, source_id=source_id)
    for source in sources:
        if source.name == "zendesk_integration":
            try:
                documents.extend(get_zendesk_hc_articles(source))
            except Exception as e:
                logger.error(str(e))
    for document in documents:
        messages.append({
            "MessageBody": json.dumps(document),
            "Id": str(uuid.uuid4())
        })
    logger.info(f"Upserting {len(messages)} docs")
    for chunk in chunks(messages, batch_size=10):
        queue.send_messages(Entries=chunk)

    engine.dispose()