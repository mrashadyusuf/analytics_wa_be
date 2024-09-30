# SQLAlchemy model for ms_group
from sqlalchemy import Column, String, DateTime
from datetime import datetime
from database import Base

class Group(Base):
    __tablename__ = 'ms_group'

    ms_group_id = Column(String(50), primary_key=True, index=True)
    isactive = Column(String(1), nullable=False)
    ms_group_name = Column(String(75), nullable=False)
    createdby = Column(String(50), nullable=False)
    created = Column(DateTime, default=datetime.utcnow)
    updatedby = Column(String(50), nullable=True)
    updated = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
