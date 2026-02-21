"""
Silver to Gold Transformation

Aggregates hourly Silver data into daily Gold summaries.
"""

from sqlalchemy import func
from datetime import datetime, timedelta, date
from collections import Counter
from app.database import SessionLocal
from app.models import WeatherRecordSilver, WeatherDailyGold


def transform_silver_to_gold(days_back: int = 7):
    """
    Aggregate Silver hourly data into Gold daily summaries.
    
    Args:
        days_back: How many days to process (default: last 7 days)
    """
    db = SessionLocal()
    
    try:
        print("\n" + "="*70)
        print(f"Starting Silver → Gold transformation")
        print(f"Processing last {days_back} days")
        print("="*70)
        
        # Calculate date range
        end_date = datetime.utcnow().date()
        start_date = end_date - timedelta(days=days_back)
        
        # Get Silver records in date range
        silver_records = db.query(WeatherRecordSilver)\
            .filter(func.date(WeatherRecordSilver.timestamp) >= start_date)\
            .filter(func.date(WeatherRecordSilver.timestamp) <= end_date)\
            .all()
        
        if not silver_records:
            print("ℹ️  No Silver records to process")
            return
        
        print(f"📊 Found {len(silver_records)} Silver records")
        
        # Group by city and date
        grouped_data = {}
        for record in silver_records:
            record_date = record.timestamp.date()
            key = (record.city, record_date)
            
            if key not in grouped_data:
                grouped_data[key] = {
                    'city': record.city,
                    'country': record.country,
                    'date': record_date,
                    'temperatures': [],
                    'humidities': [],
                    'wind_speeds': [],
                    'descriptions': [],
                    'total_readings': 0,
                    'valid_readings': 0
                }
            
            grouped_data[key]['temperatures'].append(record.temperature)
            grouped_data[key]['humidities'].append(record.humidity)
            grouped_data[key]['wind_speeds'].append(record.wind_speed)
            grouped_data[key]['descriptions'].append(record.description)
            grouped_data[key]['total_readings'] += 1
            
            if record.data_quality_flag == 'valid':
                grouped_data[key]['valid_readings'] += 1
        
        # Create Gold records
        gold_created = 0
        gold_updated = 0
        
        for key, data in grouped_data.items():
            city, record_date = key
            
            # Calculate aggregations
            temps = data['temperatures']
            humids = data['humidities']
            winds = data['wind_speeds']
            
            # Most common weather description
            description_counts = Counter(data['descriptions'])
            most_common_desc = description_counts.most_common(1)[0][0]
            
            # Check if Gold record already exists for this city/date
            existing = db.query(WeatherDailyGold)\
                .filter(WeatherDailyGold.city == city)\
                .filter(WeatherDailyGold.date == record_date)\
                .first()
            
            if existing:
                # Update existing record
                existing.avg_temperature = round(sum(temps) / len(temps), 2)           # ROUND HERE
                existing.max_temperature = round(max(temps), 2)                        # ROUND HERE
                existing.min_temperature = round(min(temps), 2)                        # ROUND HERE
                existing.avg_humidity = sum(humids) // len(humids)
                existing.max_humidity = max(humids)
                existing.min_humidity = min(humids)
                existing.avg_wind_speed = round(sum(winds) / len(winds), 2)          # ROUND HERE
                existing.most_common_description = most_common_desc
                existing.total_readings = data['total_readings']
                existing.valid_readings = data['valid_readings']
                gold_updated += 1
            else:
                # Create new Gold record
                gold_record = WeatherDailyGold(
                    city=data['city'],
                    country=data['country'],
                    date=record_date,
                    avg_temperature=round(sum(temps) / len(temps), 2),           # ROUND HERE
                    max_temperature=round(max(temps), 2),                        # ROUND HERE
                    min_temperature=round(min(temps), 2),                        # ROUND HERE
                    avg_humidity=sum(humids) // len(humids),                     # Integer division, no need to round
                    max_humidity=max(humids),
                    min_humidity=min(humids),
                    avg_wind_speed=round(sum(winds) / len(winds), 2),           # ROUND HERE
                    most_common_description=most_common_desc,
                    total_readings=data['total_readings'],
                    valid_readings=data['valid_readings']
                )
                db.add(gold_record)
                gold_created += 1
        
        db.commit()
        
        print(f"\n✅ Gold transformation complete:")
        print(f"   - Created: {gold_created} records")
        print(f"   - Updated: {gold_updated} records")
        print("="*70 + "\n")
        
    except Exception as e:
        print(f"❌ Error during transformation: {e}")
        db.rollback()
        raise
    
    finally:
        db.close()


if __name__ == "__main__":
    transform_silver_to_gold()