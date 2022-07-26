import json
import os
from urllib.parse import urlencode

import boto3
from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import FileResponse
import gantry.query as gquery
from sqlalchemy.ext.asyncio import AsyncSession
import requests

from app.api.deps import current_active_user, get_async_session
from app.crud.source import create_source, get_source, update_source
from app.models.user import User

api_router = APIRouter()

@api_router.get("/sources/zendesk/oauth_redirect", tags=["sources"])
async def zendesk_oauth_redirect(code: str, state: str, db: AsyncSession = Depends(get_async_session)):
    user_id, subdomain = state.split("|")
    parameters = {
        "grant_type": "authorization_code",
        "code": code,
        "client_id": os.getenv("ZENDESK_CLIENT_ID"),
        "client_secret": os.getenv("ZENDESK_SECRET"),
        "redirect_uri": os.getenv("ZENDESK_REDIRECT_URI"),
        "scope": "read",
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
    url = f"https://{subdomain}/api/v2/users.json?page[size]=100&role[]=agent&role[]=admin"
    r = requests.get(url, headers=header)
    user_emails = [r["email"] for r in r.json().get("users", [])]
    source = await get_source(db, user_id, "zendesk_integration")
    if source:
        source = await update_source(db, str(source.id), {"shared_with": user_emails, "extra": extra})
    else:
        source = await create_source(db, user_id, "zendesk_integration", user_emails, extra=extra)
    lambda_client = boto3.client("lambda", region_name="us-east-1")
    lambda_client.invoke(
        FunctionName=os.getenv("SCHEDULER_FUNCTION"),
        Payload=json.dumps({"source_id": str(source.id)})
    )
    return {"message": "success"}


@api_router.get("/sources/hubspot/oauth_redirect", tags=["sources"])
async def hubspot_oauth_redirect(code: str, state: str, db: AsyncSession = Depends(get_async_session)):
    user_id, subdomain = state.split("|")
    scope = "content%20tickets%20settings.users.read%20cms.knowledge_base.articles.read%20settings.users.teams.read"
    client_id = os.getenv("HUBSPOT_CLIENT_ID")
    redirect_uri=os.getenv("HUBSPOT_REDIRECT_URI")
    url = f"https://app.hubspot.com/oauth/authorize?client_id={client_id}&redirect_uri={redirect_uri}&scope={scope}&state={state}"
    parameters = {
        "grant_type": "authorization_code",
        "code": code,
        "client_id": client_id,
        "client_secret": os.getenv("HUBSPOT_SECRET"),
        "redirect_uri": redirect_uri
    }
    url = "https://api.hubapi.com/oauth/v1/token"
    r = requests.post(url=url, data=parameters)
    data = r.json()
    access_token = data["access_token"]
    refresh_token = data["refresh_token"]
    bearer_token = f"Bearer {access_token}"
    header = {'Authorization': bearer_token}
    url = "https://api.hubapi.com/settings/v3/users/"
    response = requests.get(url, headers=header).json()
    user_emails = [r["email"] for r in response.get("results", [])]
    extra = json.dumps({"subdomain": subdomain, "refresh_token": refresh_token})
    source = await get_source(db, user_id, "hubspot_integration")
    if source:
        source = await update_source(db, str(source.id), {"extra": extra, "shared_with": user_emails})
    else:
        source = await create_source(db, user_id, "hubspot_integration", user_emails, extra=extra)
    lambda_client = boto3.client("lambda", region_name="us-east-1")
    lambda_client.invoke(
        FunctionName=os.getenv("SCHEDULER_FUNCTION"),
        Payload=json.dumps({"source_id": str(source.id)})
    )
    return {"message": "success"}


@api_router.get("/sources/zendesk", tags=["sources"])
async def get_zendesk_user_source(
    user: User = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_session)
):
    source = await get_source(db, str(user.id), "zendesk_integration")
    if source is None:
        raise HTTPException(status_code=404, detail="zendesk source not found")
    return {
        "id": str(source.id),
        "owner": str(source.owner),
        "name": source.name
    }


@api_router.get("/sources/hubspot", tags=["sources"])
async def get_zendesk_user_source(
    user: User = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_session)
):
    source = await get_source(db, str(user.id), "hubspot_integration")
    if source is None:
        raise HTTPException(status_code=404, detail="hubspot source not found")
    return {
        "id": str(source.id),
        "owner": str(source.owner),
        "name": source.name
    }


@api_router.get("/sources/zendesk/analytics", tags=["sources"])
async def get_zendesk_user_source(
    user: User = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_session)
):
    gquery.init(api_key=os.getenv("GANTRY_API_KEY"))
    source = await get_source(db, str(user.id), "zendesk_integration")
    source_id = str(source.id)
    gdf = gquery.query(
        application="search_endpoint",
        version=0,
        environment=os.getenv("ENVIRONMENT")
    )
    df = gdf.fetch().reset_index()
    df = df[df["inputs.source_ids"].notna()]
    columns = [
        "timestamp",
        "inputs.query",
        "inputs.user_id",
        "inputs.log_id",
        "outputs.first_result_doc_name",
        "outputs.first_result_doc_url"
    ]
    filtered_df = df[df["inputs.source_ids"].str.contains(source_id, na=False)][columns]
    filtered_df.rename(
        columns={
            "inputs.log_id": "ticket_id",
            "inputs.query": "query",
            "outputs.first_result_doc_name": "first_result_doc_name",
            "outputs.first_result_doc_url": "first_result_doc_url"
        },
        inplace=True
    )
    filtered_df.to_csv("/tmp/report.csv", index=False)
    return FileResponse(path="/tmp/report.csv", filename="report.csv", media_type="text/csv")