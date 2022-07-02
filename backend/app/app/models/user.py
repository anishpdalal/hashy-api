from typing import List
import uuid

from fastapi_users.db import (
    SQLAlchemyBaseOAuthAccountTableUUID,
    SQLAlchemyBaseUserTableUUID,
)
from sqlalchemy import Column, DateTime, ForeignKey
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship

from app.db.base_class import Base
from app.models.sources import Source


class OAuthAccount(SQLAlchemyBaseOAuthAccountTableUUID, Base):
    pass


class APIKey(Base):
    __tablename__ = "api_key"
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id = Column(UUID, ForeignKey("user.id"), unique=True, nullable=False)
    user = relationship("User", back_populates="api_key", uselist=False, lazy="joined")
    created = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)


class User(SQLAlchemyBaseUserTableUUID, Base):
    oauth_accounts: List[OAuthAccount] = relationship("OAuthAccount", lazy="joined")
    api_key: APIKey = relationship("APIKey", back_populates="user", uselist=False, lazy="joined")
    sources: List[Source] = relationship("Source", lazy="joined")