import modal
from stubs import indexer_stub, volume
from images import db_image, integrations_image, web_scraper_image, nlp_image

MODEL_NAME = "msmarco-distilbert-base-v4"
CACHE_DIR = "/cache"

indexer_stub["nlp_image"] = nlp_image

if indexer_stub.is_inside(indexer_stub["nlp_image"]):
    from sentence_transformers import SentenceTransformer

    MODEL = SentenceTransformer(MODEL_NAME, cache_folder=CACHE_DIR)



@indexer_stub.function(image=db_image, secret=indexer_stub["pg_secret"])
def get_hubspot_integration():
    import json
    import psycopg2
    
    conn = None
    row = None
    try:
        conn = psycopg2.connect()
        cur = conn.cursor()
        cur.execute("SELECT id, extra from source where name = 'hubspot_integration'")
        row = cur.fetchone()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
    if not row:
        return
    integration_id = row[0]
    extra = json.loads(row[1])
    return integration_id, extra


@indexer_stub.function(image=integrations_image)
def get_hubspot_sitemap_content(integration):
    import requests

    subdomain = integration["subdomain"]
    sitemap_url = f"https://{subdomain}/sitemap.xml"
    content = requests.get(sitemap_url).content
    return content


@indexer_stub.function(image=web_scraper_image)
def extract_urls(sitemap_content):
    import bs4

    soup = bs4.BeautifulSoup(sitemap_content, features="xml")
    urls = soup.find_all("url")
    return [str(url.find("loc").string) for url in urls]


@indexer_stub.function(image=web_scraper_image)
def break_up_text_with_headers(headers, text):
    import pysbd
    import unicodedata
    
    seg = pysbd.Segmenter(language="en", clean=True)
    header_locations = [0] + [text.find(header) for header in headers]
    results = []
    for idx, loc in enumerate(header_locations):
        if idx == len(header_locations) - 1:
            section_text = text[loc:]
        else:
            section_text = text[loc:header_locations[idx+1]]
        text_to_index = " ".join(seg.segment(section_text))
        text_to_index = unicodedata.normalize("NFKD", text_to_index)
        if text_to_index:
            results.append(text_to_index)
    return results


@indexer_stub.function(image=web_scraper_image)
def break_up_text(text):
    import pysbd
    import unicodedata
    
    seg = pysbd.Segmenter(language="en", clean=True)
    chunks = seg.segment(text)
    chunk_size = 5
    results = []
    for i in range(0, len(chunks), chunk_size):
        chunk = chunks[i:i+chunk_size]
        text_to_index = unicodedata.normalize("NFKD", " ".join(chunk))
        if text_to_index:
            results.append(text_to_index)
    return results


@indexer_stub.function(image=web_scraper_image)
def extract_text(url):
    import re
    import bs4
    import requests

    page_content = requests.get(url).content
    soup = bs4.BeautifulSoup(page_content, "html.parser")
    kb_content = str(soup.find("div", {"class": "kb-article"}))
    link = url.split("/")[-1]
    doc_name = " ".join([word.capitalize() for word in link.split("/")[-1].split("-")])
    headers = [str(header) for header in soup.find_all(re.compile('^h[1-6]$'))]
    result = {"url": url, "text": None, "doc_id": link, "doc_name": doc_name}
    if headers:
        result["text"] = [text for text in break_up_text_with_headers(headers, kb_content) if text != doc_name]
    else:
        result["text"] = break_up_text(kb_content)
    return result


@indexer_stub.function(image=indexer_stub["nlp_image"], gpu=True, shared_volumes={CACHE_DIR: volume}, secret=modal.ref("pinecone-secret"))
def index_help_center_articles(articles):
    import itertools
    import os
    import pinecone

    def chunks(iterable, batch_size=100):
        """A helper function to break an iterable into chunks of size batch_size."""
        it = iter(iterable)
        chunk = tuple(itertools.islice(it, batch_size))
        while chunk:
            yield chunk
            chunk = tuple(itertools.islice(it, batch_size))

    PINECONE_KEY = os.environ["PINECONE_KEY"]
    pinecone.init(api_key=PINECONE_KEY, environment="us-west1-gcp")
    index = pinecone.Index(index_name="semantic-text-search")
    text = [dict(article, **{"snippet": text}) for article in articles for text in article["text"]]
    embeddings = MODEL.encode([t["snippet"] for t in text]).tolist()
    vector_ids = [f"{t['subdomain']}-{t['doc_id']}-{idx}" for idx, t in enumerate(text)]
    upsert_data_generator = map(lambda i: (
        vector_ids[i],
        embeddings[i],
        {
            "text": text[i]["snippet"][0:3000],
            "integration_id": text[i]["integration_id"],
            "type": "hubspot_help_center_article",
            "url": text[i]["url"],
            "name": text[i]["doc_name"],
        }), range(len(text))
    )
    for ids_vectors_chunk in chunks(upsert_data_generator, batch_size=100):
        index.upsert(vectors=ids_vectors_chunk, namespace="note_generation")



if __name__ == "__main__":
    with indexer_stub.run():
        integration_id, integration_metadata = get_hubspot_integration()
        subdomain = integration_metadata["subdomain"]
        sitemap_content = get_hubspot_sitemap_content(integration_metadata)
        urls = extract_urls(sitemap_content)
        help_center_text = list(extract_text.map(urls))
        articles = [dict(text, **{"integration_id": integration_id, "subdomain": subdomain}) for text in help_center_text]
        index_help_center_articles(articles)