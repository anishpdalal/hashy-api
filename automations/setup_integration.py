from operator import mod
from fastapi import FastAPI, Request

import modal
from images import integrations_image

web_app = FastAPI()
stub = modal.Stub()


@web_app.get("/hubspot/oauth_redirect")
async def hubspot_oauth_redirect(code, state):
    import json
    import os
    import uuid
    import datetime
    import requests
    import psycopg2

    user_id, subdomain = state.split("|")
    parameters = {
        "grant_type": "authorization_code",
        "code": code,
        "client_id": os.environ["HUBSPOT_CLIENT_ID"],
        "client_secret": os.environ["HUBSPOT_SECRET"],
        "redirect_uri": os.environ["HUBSPOT_REDIRECT_URI"]
    }
    url = "https://api.hubapi.com/oauth/v1/token"
    r = requests.post(url=url, data=parameters)
    data = r.json()
    refresh_token = data["refresh_token"]
    conn = None
    row = None
    doc_pk = str(uuid.uuid4())
    current = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    try:
        conn = psycopg2.connect()
        cur = conn.cursor()
        cur.execute(f"SELECT id, extra from source where name = 'hubspot_integration' and owner = '{user_id}'")
        row = cur.fetchone()
        if row:
            extra = json.loads(row[1]).copy()
            extra.update({"subdomain": subdomain, "refresh_token": refresh_token})
            extra_str = json.dumps(extra)
            cur.execute(f"update source set extra = '{extra_str}' where id = '{str(row[0])}'")
        else:
            extra_str = json.dumps({"subdomain": subdomain, "refresh_token": refresh_token})
            cur.execute(f"insert into source (id, owner, name, created, extra) values (%s, %s, %s, %s::timestamp with TIME ZONE, %s)", (doc_pk, user_id, "hubspot_integration", current, extra_str))
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
    return "Success"


@web_app.get("/auth/hubspot")
async def auth_hubspot(user, subdomain):
    import os
    state = f"{user}|{subdomain}"
    client_id = os.environ["HUBSPOT_CLIENT_ID"]
    redirect_uri = os.environ["HUBSPOT_REDIRECT_URI"]
    scope = "content%20tickets%20crm.objects.contacts.read%20crm.objects.contacts.write%20settings.users.read%20cms.knowledge_base.articles.read%20settings.users.teams.read%20conversations.read"
    url = f"https://app.hubspot.com/oauth/authorize?client_id={client_id}&redirect_uri={redirect_uri}&scope={scope}&state={state}"
    return {"url": url}


@stub.asgi(image=integrations_image, secrets=[modal.ref("hubspot-secret"), modal.ref("postgres-secret")])
def fastapi_app():
    return web_app


if __name__ == "__main__":
    stub.run_forever()