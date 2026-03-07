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
    engine = create_engine(
        DATABASE_URL,
        connect_args={"timeout": 30},
        pool_pre_ping=True,     # Test connection before use (Azure drops idle connections)
        pool_recycle=1800,      # Recycle connections every 30 mins
        pool_size=5,            # Max persistent connections
        max_overflow=10,        # Extra connections allowed under load
        echo=False
    )

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
