from fastapi import APIRouter
import fastapi_users

from app.schemas.user import UserUpdate, UserRead
from app.api.deps import fastapi_users

api_router = APIRouter()

api_router.include_router(
    fastapi_users.get_users_router(UserRead, UserUpdate),
    prefix="/users",
    tags=["users"],
)