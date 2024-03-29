from typing import Any, Dict, List
from sqlalchemy import select, update, or_

from sqlalchemy.ext.asyncio import AsyncSession

from app.models.sources import Source


async def create_source(
    db: AsyncSession,
    user_id: str,
    name: str,
    shared_with: List[str],
    extra: str = None
):
    source = Source(
        owner=user_id,
        name=name,
        shared_with=shared_with,
        extra=extra
    )
    db.add(source)
    await db.commit()
    await db.refresh(source)
    source = await get_source(db, user_id, name)
    return source


async def get_source(
    db: AsyncSession,
    user_id: str,
    name: str,
):
    stmt = select(Source).where(Source.owner == user_id, Source.name == name)
    result = await db.execute(stmt)
    return result.scalars().first()


async def get_sources(
    db: AsyncSession,
    user_id: str,
    user_email: str
):
    stmt = select(Source).where(or_(Source.owner == user_id, Source.shared_with.contains([user_email])))
    result = await db.execute(stmt)
    return result.scalars().all()


async def update_source(
    db: AsyncSession,
    id: str,
    fields: Dict[str, Any],
):
    stmt = update(Source).where(Source.id == id).values(**fields).returning(
        Source.id,
        Source.owner,
        Source.name
    )
    result = await db.execute(stmt)
    await db.commit()
    return result.fetchone()
    