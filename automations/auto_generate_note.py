import modal
from stubs import note_generator_stub, volume
from images import db_image, integrations_image, nlp_image

BI_ENCODER_NAME = "msmarco-distilbert-base-v4"
CACHE_DIR = "/cache"

note_generator_stub["nlp_image"] = nlp_image

if note_generator_stub.is_inside(note_generator_stub["nlp_image"]):
    from transformers import AutoTokenizer, AutoModelForSeq2SeqLM
    from sentence_transformers import SentenceTransformer

    BIENCODER_MODEL = SentenceTransformer(BI_ENCODER_NAME, cache_folder=CACHE_DIR)
    QUESTION_GEN_TOKENIZER = AutoTokenizer.from_pretrained("allenai/t5-small-squad2-question-generation", cache_dir=CACHE_DIR)
    QUESTION_GEN_MODEL = AutoModelForSeq2SeqLM.from_pretrained("allenai/t5-small-squad2-question-generation", cache_dir=CACHE_DIR)


@note_generator_stub.function(image=db_image, secret=note_generator_stub["pg_secret"])
def get_hubspot_integration():
    import json
    import psycopg2
    
    conn = None
    rows = None
    try:
        conn = psycopg2.connect()
        cur = conn.cursor()
        cur.execute("SELECT id, extra, owner from source where name = 'hubspot_integration'")
        rows = cur.fetchall()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
    if not rows:
        return
    return [(row[0], json.loads(row[1]), str(row[2])) for row in rows]


@note_generator_stub.function(image=integrations_image, secret=note_generator_stub["hubspot_secret"])
def get_access_token(refresh_token):
    import os
    import requests

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


@note_generator_stub.function(image=integrations_image)
def get_tickets(access_token):
    import datetime
    import requests

    source_last_updated = (datetime.datetime.now() - datetime.timedelta(minutes=10))
    source_last_updated = round(source_last_updated.timestamp()*1000)

    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {access_token}"
    }
    payload = {
        "sorts": ["-hs_lastmodifieddate"],
        "properties": ["hubspot_owner_id", "subject", "content"],
        "filterGroups": [
            {
                "filters": [
                    {"operator": "NOT_HAS_PROPERTY", "propertyName": "closed_date"},
                    {"operator": "HAS_PROPERTY", "propertyName": "subject"},
                    {"operator": "GTE", "propertyName": "createdate", "value": source_last_updated}
                ]
            }
        ]
    }
    ticket_url = "https://api.hubapi.com/crm/v3/objects/tickets/search"
    tickets = requests.post(ticket_url, headers=headers, json=payload).json()
    return tickets["results"]


@note_generator_stub.function(image=integrations_image)
def attach_note_to_ticket(ticket, access_token, generated_response):
    import json
    import datetime
    import requests

    current = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    hubspot_owner_id = ticket["properties"]["hubspot_owner_id"]
    request_body = json.dumps({
        "properties": {
            "hs_timestamp": current,
            "hs_note_body": generated_response,
            "hubspot_owner_id": hubspot_owner_id
        }
    })
    headers = {
        "accept": "application/json",
        "content-type": "application/json",
        "authorization": f"Bearer {access_token}"
    }
    note = requests.post("https://api.hubapi.com/crm/v3/objects/notes", headers=headers, data=request_body).json()
    note_id = note["id"]
    ticket_id = ticket["id"]
    requests.put(f"https://api.hubapi.com/crm/v3/objects/notes/{note_id}/associations/ticket/{ticket_id}/note_to_ticket", headers=headers)
    return note


@note_generator_stub.function(image=note_generator_stub["nlp_image"], shared_volumes={CACHE_DIR: volume})
def generate_question_from_ticket(ticket):
    ticket_properties = ticket["properties"]
    content = ticket_properties.get("content", "")
    subject = ticket_properties["subject"]
    input_string = f"{subject}. {content}"
    input_ids = QUESTION_GEN_TOKENIZER.encode(input_string, return_tensors="pt")
    res = QUESTION_GEN_MODEL.generate(input_ids)
    output = QUESTION_GEN_TOKENIZER.batch_decode(res, skip_special_tokens=True)
    return output[0]


@note_generator_stub.function(image=note_generator_stub["nlp_image"], shared_volumes={CACHE_DIR: volume}, secret=modal.ref("pinecone-secret"))
def search(integration_id, question):
    import os
    import pinecone

    PINECONE_KEY = os.environ["PINECONE_KEY"]
    pinecone.init(api_key=PINECONE_KEY, environment="us-west1-gcp")
    index = pinecone.Index(index_name="semantic-text-search")

    filter = {"integration_id": integration_id}

    query_embedding = BIENCODER_MODEL.encode([question]).tolist()
    query_results = index.query(
        queries=[query_embedding],
        top_k=3,
        filter=filter,
        include_metadata=True,
        include_values=False,
        namespace="note_generation"
    )
    matches = query_results["results"][0]["matches"]
    return [match["metadata"] for match in matches]


@note_generator_stub.function(image=integrations_image, secret=modal.ref("openai-secret"))
def generate_response(question, matches):
    import os
    import openai

    openai.api_key = os.environ["OPENAI_API_KEY"]

    generated_text = "#### AUTO-GENERATED BY NLP LABS ####"
    prompt = "Answer the question based on the context below, and if the question can't be answered based on the context, say \"I don't know\"\n\nContext:\n{0}\n\n---\n\nQuestion: {1}\nAnswer:"
    response = openai.Completion.create(
        engine="text-curie-001",
        prompt=prompt.format(matches[0]["text"], question),
        temperature=0,
        max_tokens=100,
        top_p=1,
        frequency_penalty=0,
        presence_penalty=0
    )
    auto_suggestion = response.choices[0]["text"].strip()
    if not auto_suggestion.startswith("I don't know"):
        generated_text = f"{generated_text}<br><br><b>Suggestion</b>: {auto_suggestion}"
    generated_text += "<br><br><b>Related Articles</b>"
    for match in matches:
        generated_text += f"<br><br><a href='{match['url']}' target='_blank'>{match['name']}</a><br><br>{match['text']}"
    return generated_text


@note_generator_stub.function(image=db_image, secret=note_generator_stub["pg_secret"])
def write_to_db(doc_id, owner, doc_type, doc_name):
    import psycopg2
    import uuid
    import datetime

    doc_pk = str(uuid.uuid4())
    current = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    
    conn = None
    try:
        conn = psycopg2.connect()
        cur = conn.cursor()
        cur.execute("insert into document (id, owner, name, type, doc_id, created) values (%s, %s, %s, %s, %s, %s::timestamp with TIME ZONE)", (doc_pk, owner, doc_name, doc_type, doc_id, current))
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


@note_generator_stub.function(image=db_image, secret=note_generator_stub["pg_secret"])
def ticket_exists(ticket_id, owner):
    import psycopg2
    
    conn = None
    row = None
    try:
        conn = psycopg2.connect()
        cur = conn.cursor()
        cur.execute(f"SELECT * from document where doc_id = '{ticket_id}' and type = 'hubspot_ticket_auto_generate' and owner = '{owner}'")
        row = cur.fetchone()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
    if not row:
        return
    return row


@note_generator_stub.function
def write_note():
    for integration_id, integration_metadata, owner in get_hubspot_integration():
        refresh_token = integration_metadata["refresh_token"]
        access_token = get_access_token(refresh_token)
        tickets = get_tickets(access_token)
        for ticket in tickets:
            ticket_id = ticket["id"]
            if ticket_exists(ticket_id, owner):
                continue
            question = generate_question_from_ticket(ticket)
            matches = search(integration_id, question)
            generated_response = generate_response(question, matches)
            attach_note_to_ticket(ticket, access_token, generated_response)
            doc_id = ticket_id
            doc_name = ticket["properties"]["subject"]
            write_to_db(doc_id, owner, "hubspot_ticket_auto_generate", doc_name)

if __name__ == "__main__":
    with note_generator_stub.run():
        write_note()

