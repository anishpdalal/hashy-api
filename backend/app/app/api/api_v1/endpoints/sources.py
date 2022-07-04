import json
import os

from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession
import requests

from app.api.deps import get_async_session
from app.crud.source import create_source, get_source, update_source
from app.schemas.sources import SourceResponse

api_router = APIRouter()

@api_router.get("/sources/zendesk/oauth_redirect", tags=["sources"], response_model=SourceResponse)
async def zendesk_oauth_redirect(code: str, state: str, db: AsyncSession = Depends(get_async_session)):
    user_id, subdomain = state.split("|")
    parameters = {
        "grant_type": "authorization_code",
        "code": code,
        "client_id": os.environ["ZENDESK_CLIENT_ID"],
        "client_secret": os.environ["ZENDESK_SECRET"],
        "redirect_uri": os.environ["ZENDESK_REDIRECT_URI"],
        "scope": "users:read tickets:read hc:read triggers:read triggers:write automations:read automations:write",
        "state": state
    }
    payload = json.dumps(parameters)
    header = {"Content-Type": "application/json"}
    url = f"https://{subdomain}/oauth/tokens"
    r = requests.post(url=url, data=payload, headers=header)
    data = r.json()
    access_token = data["access_token"]
    extra = json.dumps({"access_token": access_token, "subdomain": subdomain})
    bearer_token = f"Bearer {access_token}"
    header = {'Authorization': bearer_token}
    url = f"https://{subdomain}/api/v2/users.json?role[]=agent&role[]=admin"
    r = requests.get(url, headers=header)
    user_emails = [r["email"] for r in r.json().get("users", [])]
    source = await get_source(db, user_id, "zendesk_integration")
    if source:
        source = await update_source(db, str(source.id), {"shared_with": user_emails, "extra": extra})
    else:
        source = await create_source(db, user_id, "zendesk_integration", user_emails, extra=extra)
    return source