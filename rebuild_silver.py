"""
Rebuild Silver layer from scratch.

Use this when:
- Bronze data was corrected and you need to resync
- Silver data is suspect and you want a clean slate
- Testing transformations with different logic
"""

from app.database import SessionLocal
from app.models import WeatherRecordSilver, TransformationLog
from transform_bronze_to_silver import transform_bronze_to_silver

def rebuild_silver():
    db = SessionLocal()
    
    print("🗑️  Deleting all Silver records...")
    db.query(WeatherRecordSilver).delete()
    
    print("🗑️  Resetting transformation checkpoints...")
    db.query(TransformationLog)\
        .filter(TransformationLog.transformation_name == "bronze_to_silver")\
        .delete()
    
    db.commit()
    db.close()
    
    print("✅ Silver layer cleared")
    print("\n🔄 Rebuilding from Bronze...\n")
    
    transform_bronze_to_silver()

if __name__ == "__main__":
    rebuild_silver()