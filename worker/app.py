import itertools
import json
import os
import re
import uuid

import pinecone
import requests
from sentence_transformers import SentenceTransformer
from sqlalchemy import create_engine, text

TAG_RE = re.compile(r'<[^>]+>')
REGEX_EXP = r"(?<!\w\.\w.)(?<![A-Z][a-z]\.)(?<=\.|\?)\s"


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


def get_user_emails(subdomain, access_token):
    bearer_token = f"Bearer {access_token}"
    header = {'Authorization': bearer_token}
    url = f"https://{subdomain}/api/v2/users.json?role[]=agent&role[]=admin"
    data = requests.get(url, headers=header).json()
    user_emails = [r["email"] for r in data.get("users", [])]
    return user_emails


def update_user_emails(engine, user_emails, source_id):
    with engine.connect() as connection:
        user_emails_str = ", ".join(user_emails)
        connection.execute(text(f"update source set shared_with = '{user_emails_str}' where id = '{source_id}'"))
    

def get_zendesk_help_center_articles(subdomain, access_token, source_id):
    url = f"https://{subdomain}/api/v2/help_center/articles.json?sort_by=updated_at&sort_order=desc"
    bearer_token = f"Bearer {access_token}"
    header = {'Authorization': bearer_token}
    data = requests.get(url, headers=header).json()
    articles = []
    for article in data.get("articles", []):
        body_text = article.get("body")
        if not body_text:
            continue
        articles.append({
            "id": f"{subdomain}-{article['id']}",
            "text": article["title"],
            "source_id": str(source_id),
            "doc_type": "zendesk_help_center_article",
            "doc_last_updated": article["updated_at"],
            "doc_url": article["url"],
            "doc_name": article["title"],
            "doc_labels": article.get("label_names", []),
            "vote_sum": article["vote_sum"],
        })
        cleaned_body_text = TAG_RE.sub('', body_text)
        chunks = [chunk for chunk in re.split(REGEX_EXP, cleaned_body_text) if any(c.isalpha() for c in chunk)]
        chunk_size = 4
        for idx in range(0, len(chunks), chunk_size):
            chunk = chunks[idx:idx+chunk_size]
            chunk_str = " ".join(chunk)
            articles.append({
                "id": f"{subdomain}-{article['id']}-{idx}",
                "text": chunk_str,
                "source_id": str(source_id),
                "doc_type": "zendesk_help_center_article",
                "doc_last_updated": article["updated_at"],
                "doc_url": article["url"],
                "doc_name": article["title"],
                "doc_labels": article.get("label_names", []),
                "vote_sum": article["vote_sum"],
            })
    return articles


def index_zendesk_help_center_articles(index, bi_encoder, articles):
    article_text_embeddings = bi_encoder.encode([article["text"] for article in articles]).tolist()
    upsert_data_generator = map(lambda i: (
        articles[i]["id"],
        article_text_embeddings[i],
        {
            "text": articles[i]["text"][0:1500],
            "source_id": articles[i]["source_id"],
            "doc_type": articles[i]["doc_type"],
            "doc_last_updated": articles[i]["doc_last_updated"],
            "doc_url": articles[i]["doc_url"],
            "doc_name": articles[i]["doc_name"],
            "doc_labels": articles[i]["doc_labels"],
            "vote_sum": articles[i]["vote_sum"],
        }), range(len(articles))
    )
    for ids_vectors_chunk in chunks(upsert_data_generator, batch_size=100):
        index.upsert(vectors=ids_vectors_chunk)


# with engine.connect() as connection:
#                     document = connection.execute(
#                         text(f"select id from document where doc_id = '{article['id']}' and owner = '{source['owner']}' and doc_type = 'zendesk_help_center_article'")
#                     ).fetchone()
#                     if not document:
#                         connection.execute(text(f"insert into document (owner, name, type, doc_id, doc_last_updated) values ({source['owner']}, {article['title']}, 'zendesk_help_center_article', {article['id']}, {article['updated_at']}::timestamp with TIME ZONE)"))
        

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
        if source['name'] == "zendesk_integration":
            subdomain, access_token = get_credentials(source)
            if not subdomain and not access_token:
                continue
            user_emails = get_user_emails(subdomain, access_token)
            update_user_emails(engine, user_emails, source_id)
            articles = get_zendesk_help_center_articles(subdomain, access_token, source_id)
            index_zendesk_help_center_articles(index, bi_encoder, articles)

    engine.dispose()