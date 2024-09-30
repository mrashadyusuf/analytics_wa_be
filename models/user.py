from sqlalchemy import Column, String, Integer, Boolean, DateTime
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime

Base = declarative_base()

class User(Base):
    __tablename__ = 'ms_user'

    ms_user_id = Column(String(50), primary_key=True, index=True)
    isactive = Column(String(1), nullable=False)
    ms_user_name = Column(String(100), nullable=False)
    ms_user_username = Column(String(50), nullable=False, unique=True)
    ms_user_password = Column(String(255), nullable=False)
    ms_user_email = Column(String(50), nullable=False, unique=True)
    ms_user_token = Column(String(5))
    created_by = Column(String(50))
    created_dt = Column(DateTime, default=datetime.utcnow)
    updated_by = Column(String(50))
    updated_dt = Column(DateTime, default=datetime.utcnow)
