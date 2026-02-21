"""
Gold to Reporting Mart Transformation

Pre-aggregates data for fast dashboard queries.
Grouped by city and day with max/min temp and avg wind speed.
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from datetime import datetime, timedelta
from app.database import SessionLocal
from app.models import WeatherDailyGold, WeatherReportingMart


def transform_to_reporting_mart(days_back: int = 30):
    """
    Create reporting mart from Gold layer.
    
    Simple aggregations optimized for PowerBI dashboards.
    
    Args:
        days_back: How many days to include
    """
    db = SessionLocal()
    
    try:
        print("\n" + "="*70)
        print(f"Starting Gold → Reporting Mart transformation")
        print(f"Processing last {days_back} days")
        print("="*70)
        
        # Calculate cutoff date
        cutoff_date = (datetime.utcnow() - timedelta(days=days_back)).date()
        
        # Get Gold records
        gold_records = db.query(WeatherDailyGold)\
            .filter(WeatherDailyGold.date >= cutoff_date)\
            .all()
        
        if not gold_records:
            print("ℹ️  No Gold records to process")
            return
        
        print(f"📊 Found {len(gold_records)} Gold records")
        
        mart_created = 0
        mart_updated = 0
        
        for gold_record in gold_records:
            # Check if already exists
            existing = db.query(WeatherReportingMart)\
                .filter(WeatherReportingMart.city == gold_record.city)\
                .filter(WeatherReportingMart.date == gold_record.date)\
                .first()
            
            if existing:
                # Update existing
                existing.max_temperature = round(gold_record.max_temperature, 2)      # ROUND HERE
                existing.min_temperature = round(gold_record.min_temperature, 2)      # ROUND HERE
                existing.avg_wind_speed = round(gold_record.avg_wind_speed, 2)        # ROUND HERE
                mart_updated += 1
            else:
                # Create new
                mart_record = WeatherReportingMart(
                    city=gold_record.city,
                    date=gold_record.date,
                    max_temperature=round(gold_record.max_temperature, 2),            # ROUND HERE
                    min_temperature=round(gold_record.min_temperature, 2),            # ROUND HERE
                    avg_wind_speed=round(gold_record.avg_wind_speed, 2)               # ROUND HERE
                )
                db.add(mart_record)
                mart_created += 1
        
        db.commit()
        
        print(f"\n✅ Reporting Mart transformation complete:")
        print(f"   - Created: {mart_created} records")
        print(f"   - Updated: {mart_updated} records")
        print("="*70 + "\n")
        
    except Exception as e:
        print(f"❌ Error during transformation: {e}")
        db.rollback()
        raise
    
    finally:
        db.close()


if __name__ == "__main__":
    transform_to_reporting_mart()