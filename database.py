# database.py
from sqlalchemy import create_engine, MetaData
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from databases import Database
from dotenv import load_dotenv
import os

load_dotenv()

# Get the DATABASE_URL from the environment
DATABASE_URL = os.getenv("DATABASE_URL")

# SQLAlchemy engine
engine = create_engine(DATABASE_URL)

# Database for async usage
database = Database(DATABASE_URL)

# Metadata for schema generation
metadata = MetaData()

# Base class for declarative models
Base = declarative_base()

# SessionLocal is the session maker for SQLAlchemy ORM
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

conn = engine.connect().execution_options(autocommit=True)

# Dependency to get the session
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
