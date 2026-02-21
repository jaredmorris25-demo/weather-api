from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from .models import Base
from config import DATABASE_URL

engine = create_engine(DATABASE_URL, echo=True)

Base.metadata.create_all(bind=engine)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def get_db():
    """
    Dependency function to get a database session.
    This is used in our FastAPI routes to interact with the database.
    It ensures that we create a new session for each request and close it after we're done.
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()