from fastapi import APIRouter

from app.api.api_v1.endpoints import login, sources, users

api_router = APIRouter()
api_router.include_router(login.api_router)
api_router.include_router(sources.api_router)
api_router.include_router(users.api_router)