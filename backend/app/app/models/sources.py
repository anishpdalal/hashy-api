import uuid

from sqlalchemy import Column, String, ForeignKey, DateTime
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.sql import func
from sqlalchemy.dialects.postgresql import UUID

from app.db.base_class import Base

class Source(Base):
    __tablename__ = "source"
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    owner = Column(UUID, ForeignKey("user.id"), nullable=False)
    name = Column(String, nullable=False)
    created = Column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated = Column(DateTime(timezone=True), onupdate=func.now())
    shared_with = Column(ARRAY(String))
    extra = Column(String)