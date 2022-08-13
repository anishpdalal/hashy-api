import modal

stub = modal.Stub("note-generator")
if modal.is_local():
    import os
    from dotenv import load_dotenv
    load_dotenv()
    stub["pg_secret"] = modal.Secret(
        {
            "PGHOST": os.environ["PGHOST"],
            "PGPORT": os.environ["PGPORT"],
            "PGDATABASE": os.environ["PGDATABASE"],
            "PGUSER": os.environ["PGUSER"],
            "PGPASSWORD": os.environ["PGPASSWORD"],
        }
    )
    stub["hubspot_secret"] = modal.Secret(
        {
            "HUBSPOT_CLIENT_ID": os.environ["HUBSPOT_CLIENT_ID"],
            "HUBSPOT_SECRET": os.environ["HUBSPOT_SECRET"],
            "HUBSPOT_REDIRECT_URI": os.environ["HUBSPOT_REDIRECT_URI"],
        }
    )

requests_image = modal.DebianSlim().pip_install(["requests"])
pg_image = modal.DebianSlim().apt_install(["libpq-dev"]).pip_install(["psycopg2"])

@stub.function(image=pg_image, secret=stub["pg_secret"])
def get_hubspot_integration():
    import json
    import psycopg2
    
    conn = None
    row = None
    try:
        conn = psycopg2.connect()
        cur = conn.cursor()
        cur.execute("SELECT extra from source where name = 'hubspot_integration'")
        row = cur.fetchone()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
    if not row:
        return
    extra = json.loads(row[0])
    return extra


@stub.function(image=requests_image, secret=stub["hubspot_secret"])
def get_access_token(integration):
    import os
    import requests

    refresh_token = integration["refresh_token"]
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


@stub.function(image=requests_image)
def get_ticket(access_token, ticket_id=1012677784):
    import requests

    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {access_token}"
    }
    ticket_url = f"https://api.hubapi.com/crm/v3/objects/tickets/{ticket_id}"
    params = {"properties": ["hubspot_owner_id", "subject", "content"]}
    response = requests.get(ticket_url, headers=headers, params=params).json()
    return response


@stub.function(image=requests_image)
def attach_note_to_ticket(ticket, access_token):
    import json
    import datetime
    import requests

    current = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    text = "This is a test"
    hubspot_owner_id = ticket["properties"]["hubspot_owner_id"]
    request_body = json.dumps({
        "properties": {
            "hs_timestamp": current,
            "hs_note_body": text,
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

@stub.function
def write_note():
    integration = get_hubspot_integration()
    access_token = get_access_token(integration)
    ticket = get_ticket(access_token)
    note = attach_note_to_ticket(ticket, access_token)
    return note

if __name__ == "__main__":
    with stub.run():
        note = write_note()
        print(note)


