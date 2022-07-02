from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import delete

from app.models.user import APIKey, User


async def delete(db: AsyncSession, user: User):
    await db.execute(delete(APIKey).where(APIKey.user_id == str(user.id)))
    await db.commit()
    return


async def create(db: AsyncSession, user: User):
    api_key = APIKey(user_id=str(user.id))
    db.add(api_key)
    await db.commit()
    await db.refresh(api_key)
    return api_key
