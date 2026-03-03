import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

DATABASE_URL = os.getenv("DATABASE_URL")

# Fallback for local dev if no env var set
if not DATABASE_URL:
    from config import get_settings
    settings = get_settings()
    DATABASE_URL = f"sqlite:///{settings.db_path}"

# Azure SQL needs a different connect_args than SQLite
if DATABASE_URL.startswith("sqlite"):
    engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})
else:
    engine = create_engine(DATABASE_URL)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)