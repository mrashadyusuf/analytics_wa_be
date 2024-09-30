from sqlalchemy import Column, String, Boolean, DateTime
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime

Base = declarative_base()

class UserGroup(Base):
    __tablename__ = 'ms_user_group'

    ms_user_group_id = Column(String(50), primary_key=True, index=True)
    isactive = Column(String(1), nullable=False)  # Assuming '1' or '0' for active/inactive
    ms_user_id = Column(String(50), nullable=False)
    ms_group_id = Column(String(50), nullable=False)
    isdefault = Column(String(1), nullable=False)  # Assuming '1' or '0' for default
    createdby = Column(String(50), nullable=False)
    created = Column(DateTime, default=datetime.utcnow)
    updatedby = Column(String(50), nullable=True)
    updated = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
