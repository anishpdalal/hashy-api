from fastapi import FastAPI

from app.api.api_v1.api import api_router

api = FastAPI()

api.include_router(api_router, prefix="/v0")