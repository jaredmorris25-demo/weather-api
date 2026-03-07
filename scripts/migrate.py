"""
One-off database migration script.
Run this manually to create/update database schema.
Usage: python -m scripts.migrate
"""
from app.database import engine
from app.models import Base

def run_migration():
    print("Running database migration...")
    Base.metadata.create_all(bind=engine)
    print("Migration complete.")

if __name__ == "__main__":
    run_migration()