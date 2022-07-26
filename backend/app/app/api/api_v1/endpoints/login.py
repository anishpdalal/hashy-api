import os
from urllib.parse import urlencode

from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import (
    auth_backend,
    current_active_user,
    cookie_backend,
    google_oauth_client,
    get_async_session,
    fastapi_users
)
from app.crud.api_key import create_api_key, delete_api_key
from app.models.user import User
from app.schemas.auth import ZendeskAuth, HubSpotAuth
from app.schemas.user import UserCreate, UserRead


api_router = APIRouter()

api_router.include_router(
    fastapi_users.get_auth_router(auth_backend), prefix="/auth/jwt", tags=["auth"]
)
api_router.include_router(
    fastapi_users.get_auth_router(cookie_backend), prefix="/auth/cookie", tags=["auth"]
)
api_router.include_router(
    fastapi_users.get_register_router(UserRead, UserCreate),
    prefix="/auth",
    tags=["auth"],
)
api_router.include_router(
    fastapi_users.get_reset_password_router(),
    prefix="/auth",
    tags=["auth"],
)
api_router.include_router(
    fastapi_users.get_verify_router(UserRead),
    prefix="/auth",
    tags=["auth"],
)
api_router.include_router(
    fastapi_users.get_oauth_router(google_oauth_client, auth_backend, os.getenv("SECRET")),
    prefix="/auth/jwt/google",
    tags=["auth"],
)

api_router.include_router(
    fastapi_users.get_oauth_router(google_oauth_client, cookie_backend, os.getenv("SECRET")),
    prefix="/auth/cookie/google",
    tags=["auth"],
)

@api_router.post("/auth/api_key", tags=["auth"])
async def generate_api_key(user: User = Depends(current_active_user), db: AsyncSession = Depends(get_async_session)):
    if user.api_key:
        await delete_api_key(db, user)
    await create_api_key(db, user)
    return {"message": "Successfully created an API key!"}


@api_router.post("/auth/zendesk", tags=["auth"])
async def zendesk_auth(auth: ZendeskAuth, user: User = Depends(current_active_user)):
    user_id = user.id
    parameters = {
        "response_type": "code",
        "redirect_uri": os.getenv("ZENDESK_REDIRECT_URI"),
        "client_id": os.getenv("ZENDESK_CLIENT_ID"),
        "scope": "read",
        "state": f"{user_id}|{auth.subdomain}"
    }
    url = f"https://{auth.subdomain}/oauth/authorizations/new?{urlencode(parameters)}"
    return {"authorization_url": url}


@api_router.post("/auth/hubspot", tags=["auth"])
async def hubspot_auth(auth: HubSpotAuth, user: User = Depends(current_active_user)):
    user_id = user.id
    state = f"{user_id}|{auth.subdomain}"
    client_id = os.getenv("HUBSPOT_CLIENT_ID")
    redirect_uri=os.getenv("HUBSPOT_REDIRECT_URI")
    scope = "content%20tickets%20settings.users.read%20cms.knowledge_base.articles.read%20settings.users.teams.read"
    url = f"https://app.hubspot.com/oauth/authorize?client_id={client_id}&redirect_uri={redirect_uri}&scope={scope}&state={state}"
    return {"authorization_url": url}