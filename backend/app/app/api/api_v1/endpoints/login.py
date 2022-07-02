import os

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
from app.crud.api_key import create, delete
from app.models.user import APIKey, User
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
        delete(db, user)
    create(db, user)
    return {"message": "Successfully created an API key!"}

