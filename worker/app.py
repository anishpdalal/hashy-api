import datetime
import itertools
import json
import os
import unicodedata
import uuid
import re

from bs4 import BeautifulSoup
import pinecone
import pysbd
import requests
from sentence_transformers import SentenceTransformer
from sqlalchemy import create_engine, text


def chunks(iterable, batch_size=100):
    """A helper function to break an iterable into chunks of size batch_size."""
    it = iter(iterable)
    chunk = tuple(itertools.islice(it, batch_size))
    while chunk:
        yield chunk
        chunk = tuple(itertools.islice(it, batch_size))


def get_record_body(record):
    if isinstance(record["body"], str):
        record_body = json.loads(record["body"])
    else:
        record_body = record["body"]
    return record_body


def get_source(engine, source_id):
    with engine.connect() as connection:
        source = connection.execute(
            text(f"select id, owner, name, extra from source where id = '{source_id}'")
        ).fetchone()
    return source


def get_credentials(source):
    extra = json.loads(source['extra']) if source['extra'] else {}
    subdomain = extra.get("subdomain")
    access_token = extra.get("access_token")
    return subdomain, access_token


def get_zendesk_help_center_article(source, doc_id):
    subdomain, access_token = get_credentials(source)
    if not subdomain and not access_token:
        return
    source_id = source.id
    url = f"https://{subdomain}/api/v2/help_center/articles/{doc_id}.json"
    bearer_token = f"Bearer {access_token}"
    header = {'Authorization': bearer_token}
    data = requests.get(url, headers=header).json()
    article = data["article"]
    article_title = article["title"]
    article_url = article["url"]
    article_last_updated = article["updated_at"]
    article_body = article["body"]
    article_labels = article.get("label_names", [])
    soup = BeautifulSoup(article_body, "html.parser")
    seg = pysbd.Segmenter(language="en", clean=True)
    headers = soup.find_all(re.compile('^h[1-6]$'))
    results = []
    results.append({
        "id": f"{subdomain}-{doc_id}",
        "display_text": article_title,
        "text_to_index": article_title,
        "source_id": str(source_id),
        "doc_type": "zendesk_hc_article_title",
        "doc_last_updated": article_last_updated,
        "doc_url": article_url,
        "doc_name": article_title,
        "doc_labels": article_labels,
    })
    if headers:
        header_locations = [0] + [article_body.find(str(header)) for header in headers]
        for idx, loc in enumerate(header_locations):
            if idx == len(header_locations) - 1:
                article_text = article_body[loc:]
            else:
                article_text = article_body[loc:header_locations[idx+1]]
            display_text = unicodedata.normalize("NFKD", article_text)
            text_to_index = " ".join(seg.segment(display_text))
            results.append({
                "id": f"{subdomain}-{doc_id}-{idx}",
                "display_text": display_text,
                "text_to_index": text_to_index,
                "source_id": str(source_id),
                "doc_type": "zendesk_hc_article_body",
                "doc_last_updated": article_last_updated,
                "doc_url": article_url,
                "doc_name": article_title,
                "doc_labels": article_labels,
            })
    else:
        chunks = seg.segment(article_body)
        chunk_size = 5
        for idx, i in enumerate(range(0, len(chunks), chunk_size)):
            chunk = chunks[i:i+chunk_size]
            text_to_index = unicodedata.normalize("NFKD", " ".join(chunk))
            results.append({
                "id": f"{subdomain}-{doc_id}-{idx}",
                "display_text": text_to_index,
                "text_to_index": text_to_index,
                "source_id": str(source_id),
                "doc_type": "zendesk_hc_article_body",
                "doc_last_updated": article_last_updated,
                "doc_url": article_url,
                "doc_name": article_title,
                "doc_labels": article_labels,
            })
    return results

def get_zendesk_ticket(source, doc_id):
    subdomain, access_token = get_credentials(source)
    if not subdomain and not access_token:
        return
    source_id = source.id
    url = f"https://{subdomain}/api/v2/tickets/{doc_id}.json"
    bearer_token = f"Bearer {access_token}"
    header = {'Authorization': bearer_token}
    data = requests.get(url, headers=header).json()
    ticket = data["ticket"]
    subject = unicodedata.normalize("NFKD", ticket["subject"])
    description = unicodedata.normalize("NFKD", ticket["description"])
    ticket_last_updated = ticket["updated_at"]
    ticket_url = ticket["url"]
    ticket_labels = ticket["tags"]
    return [
        {
            "id": f"{subdomain}-{doc_id}-{0}",
            "display_text": description,
            "text_to_index": f"{subject} | {description}",
            "source_id": str(source_id),
            "doc_type": "zendesk_ticket",
            "doc_last_updated": ticket_last_updated,
            "doc_url": ticket_url,
            "doc_name": subject,
            "doc_labels": ticket_labels,
        }
    ]



def index_documents(index, bi_encoder, results):
    text_embeddings = bi_encoder.encode([result["text_to_index"] for result in results]).tolist()
    upsert_data_generator = map(lambda i: (
        results[i]["id"],
        text_embeddings[i],
        {
            "text": results[i]["display_text"],
            "source_id": results[i]["source_id"],
            "doc_type": results[i]["doc_type"],
            "doc_last_updated": results[i]["doc_last_updated"],
            "doc_url": results[i]["doc_url"],
            "doc_name": results[i]["doc_name"],
            "doc_labels": results[i]["doc_labels"],
        }), range(len(results))
    )
    for ids_vectors_chunk in chunks(upsert_data_generator, batch_size=100):
        index.upsert(vectors=ids_vectors_chunk)


def store_document(engine, doc_id, owner, doc_type, doc_name, doc_last_updated):
    with engine.connect() as connection:
        document = connection.execute(
            text(f"select id from document where doc_id = '{doc_id}' and owner = '{owner}' and type = '{doc_type}'")
        ).fetchone()
        current = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        if not document:
            doc_pk = str(uuid.uuid4())
            connection.execute(text(f"insert into document (id, owner, name, type, doc_id, doc_last_updated, created) values ('{doc_pk}', '{owner}', '{doc_name}', '{doc_type}', '{doc_id}', '{doc_last_updated}'::timestamp with TIME ZONE, '{current}'::timestamp with TIME ZONE)"))
        else:
            connection.execute(text(f"update document SET updated = '{current}'::timestamp with TIME ZONE, doc_last_updated = '{doc_last_updated}'::timestamp with TIME ZONE where id = '{document.id}'"))


def handler(event, context):
    PINECONE_KEY = os.environ["PINECONE_KEY"]
    pinecone.init(api_key=PINECONE_KEY, environment="us-west1-gcp")
    index = pinecone.Index(index_name="semantic-text-search")
    bi_encoder = SentenceTransformer("/mnt/bi_encoder")
    engine = create_engine(os.environ["SQLALCHEMY_DATABASE_URL"])
    for record in event['Records']:
        record_body = get_record_body(record)
        source_id = uuid.UUID(record_body["source_id"])
        source = get_source(engine, source_id)
        if not source:
            continue
        owner = source["owner"]
        doc_type = record_body["doc_type"]
        doc_id = record_body["doc_id"]
        doc_name = record_body["doc_name"]
        doc_last_updated = record_body["doc_last_updated"]
        results = None
        if doc_type == "zendesk_help_center_article":
            results = get_zendesk_help_center_article(source, doc_id)
        elif doc_type == "zendesk_ticket":
            results = get_zendesk_ticket(source, doc_id)
        else:
            continue
        if results:
            index_documents(index, bi_encoder, results)
            store_document(engine, doc_id, owner, doc_type, doc_name, doc_last_updated)
    engine.dispose()