import uuid

from sqlalchemy import Boolean, Column, String, ForeignKey, DateTime
from sqlalchemy.sql import func
from sqlalchemy.dialects.postgresql import UUID

from app.db.base_class import Base

class Document(Base):
    __tablename__ = "document"
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    owner = Column(UUID, ForeignKey("user.id"), nullable=False)
    name = Column(String, nullable=False)
    type = Column(String, nullable=False)
    is_boosted = Column(Boolean, default=False)
    created = Column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated = Column(DateTime(timezone=True), onupdate=func.now())
    extra = Column(String)