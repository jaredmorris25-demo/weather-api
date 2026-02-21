"""
Gold to Analytics Layer Transformation

Filters and enriches data for specific analytical use cases.
Example: Hot, clear days (temp > 30°C and clear sky).
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from datetime import datetime, timedelta
from app.database import SessionLocal
from app.models import WeatherRecordSilver, WeatherAnalyticsLayer


def transform_to_analytics(days_back: int = 30):
    """
    Create analytics layer: hot clear days (temp > 30 AND clear sky).
    
    Args:
        days_back: How many days to process
    """
    db = SessionLocal()
    
    try:
        print("\n" + "="*70)
        print(f"Starting Gold → Analytics transformation")
        print(f"Filter: temperature > 30°C AND description contains 'clear'")
        print("="*70)
        
        # Calculate cutoff date
        cutoff_date = datetime.utcnow() - timedelta(days=days_back)
        
        # Get Silver records matching criteria
        # (In real world, you might join with Gold for additional context)
        silver_records = db.query(WeatherRecordSilver)\
            .filter(WeatherRecordSilver.timestamp >= cutoff_date)\
            .filter(WeatherRecordSilver.temperature > 30)\
            .filter(WeatherRecordSilver.description.like('%clear%'))\
            .all()
        
        if not silver_records:
            print("ℹ️  No records match criteria (temp > 30 and clear sky)")
            return
        
        print(f"📊 Found {len(silver_records)} matching Silver records")
        
        analytics_created = 0
        
        for silver_record in silver_records:
            # Check if already in analytics layer
            existing = db.query(WeatherAnalyticsLayer)\
                .filter(WeatherAnalyticsLayer.silver_record_id == silver_record.id)\
                .first()
            
            if existing:
                continue  # Already processed
            
            # Create analytics record
            analytics_record = WeatherAnalyticsLayer(
                city=silver_record.city,
                country=silver_record.country,
                timestamp=silver_record.timestamp,
                temperature=silver_record.temperature,
                humidity=silver_record.humidity,
                wind_speed=silver_record.wind_speed,
                description=silver_record.description,
                is_hot_clear_day=True,  # By definition (filtered above)
                silver_record_id=silver_record.id
            )
            
            db.add(analytics_record)
            analytics_created += 1
        
        db.commit()
        
        print(f"\n✅ Analytics transformation complete:")
        print(f"   - Created: {analytics_created} records")
        print("="*70 + "\n")
        
    except Exception as e:
        print(f"❌ Error during transformation: {e}")
        db.rollback()
        raise
    
    finally:
        db.close()


if __name__ == "__main__":
    transform_to_analytics()