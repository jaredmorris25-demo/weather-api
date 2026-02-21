"""
Bronze to Silver Transformation

This script processes raw weather data (Bronze) into cleaned, validated data (Silver).

In production, this would be:
- A Databricks notebook running on a schedule
- An Azure Data Factory data flow
- A dbt model

The logic is the same: read from Bronze, transform, write to Silver.
"""

from sqlalchemy.orm import Session
from sqlalchemy import func
from datetime import datetime, timedelta
from app.database import SessionLocal
from app.models import WeatherRecord, WeatherRecordSilver, TransformationLog

def validate_temperature(temp: float) -> tuple[str, str]:
    """
    Check if temperature is reasonable.
    
    Returns: (quality_flag, quality_notes)
    """
    if temp < -50 or temp > 60:
        return ("invalid", f"Temperature {temp}°C is outside reasonable range (-50 to 60)")
    elif temp < -30 or temp > 50:
        return ("suspect", f"Temperature {temp}°C is extreme but possible")
    else:
        return ("valid", None)


def validate_humidity(humidity: int) -> tuple[str, str]:
    """
    Check if humidity is reasonable.
    
    Returns: (quality_flag, quality_notes)
    """
    if humidity < 0 or humidity > 100:
        return ("invalid", f"Humidity {humidity}% is outside valid range (0-100)")
    else:
        return ("valid", None)


def validate_record(record: WeatherRecord) -> tuple[str, list[str]]:
    """
    Apply all validation rules to a record.
    
    Returns: (overall_quality_flag, list_of_issues)
    """
    issues = []
    worst_flag = "valid"
    
    # Check temperature
    temp_flag, temp_note = validate_temperature(record.temperature)
    if temp_flag != "valid":
        issues.append(temp_note)
        if temp_flag == "invalid" or worst_flag == "valid":
            worst_flag = temp_flag
    
    # Check humidity
    humidity_flag, humidity_note = validate_humidity(record.humidity)
    if humidity_flag != "valid":
        issues.append(humidity_note)
        if humidity_flag == "invalid":
            worst_flag = "invalid"
    
    notes = "; ".join(issues) if issues else None
    return (worst_flag, notes)


def deduplicate_records(db: Session, records: list[WeatherRecord]) -> list[WeatherRecord]:
    """
    Remove duplicates: keep the record CLOSEST to the top of the hour.
    
    Example: For 1pm hour, prefer 1:03pm over 1:58pm
    """
    unique_records = {}
    
    for record in records:
        # Round timestamp to the hour
        hour_key = record.timestamp.replace(minute=0, second=0, microsecond=0)
        key = (record.city, hour_key)
        
        # Calculate distance from top of hour
        distance = abs((record.timestamp - hour_key).total_seconds())
        
        if key not in unique_records:
            unique_records[key] = (record, distance)
        else:
            # Keep whichever is closer to the top of hour
            existing_record, existing_distance = unique_records[key]
            if distance < existing_distance:
                unique_records[key] = (record, distance)
    
    # Extract just the records (drop the distance metadata)
    return [record for record, distance in unique_records.values()]


def transform_bronze_to_silver():
    """
    Main transformation function using checkpoint pattern.
    
    Instead of looking back X hours, we process everything since
    the last successful run. This ensures no data is ever missed,
    even if the job is down for days.
    """
    db = SessionLocal()
    
    try:
        print("\n" + "="*70)
        print(f"Starting Bronze → Silver transformation")
        print("="*70)
        
        # Find the last successful run (HIGH WATER MARK)
        last_run = db.query(TransformationLog)\
            .filter(TransformationLog.transformation_name == "bronze_to_silver")\
            .filter(TransformationLog.status == "success")\
            .order_by(TransformationLog.run_timestamp.desc())\
            .first()
        
        if last_run:
            cutoff_time = last_run.last_processed_timestamp
            print(f"📌 Resuming from checkpoint: {cutoff_time}")
        else:
            # First run ever - process all historical data
            cutoff_time = datetime(2020, 1, 1)
            print(f"🆕 First run - processing all historical data since {cutoff_time}")
        
        # Get Bronze records that haven't been processed yet
        bronze_records = db.query(WeatherRecord)\
            .filter(WeatherRecord.timestamp > cutoff_time)\
            .order_by(WeatherRecord.timestamp.asc())\
            .all()
        
        if not bronze_records:
            print("ℹ️  No new Bronze records to process")
            return
        
        print(f"📊 Found {len(bronze_records)} Bronze records")
        
        # Deduplicate
        unique_records = deduplicate_records(db, bronze_records)
        print(f"🔍 After deduplication: {len(unique_records)} unique records")
        
        # Transform and validate each record
        silver_records_created = 0
        valid_count = 0
        suspect_count = 0
        invalid_count = 0
        
        for bronze_record in unique_records:
            # Check if we've already processed this bronze record
            existing = db.query(WeatherRecordSilver)\
                .filter(WeatherRecordSilver.bronze_record_id == bronze_record.id)\
                .first()
            
            if existing:
                continue  # Skip already processed records
            
            # Validate the record
            quality_flag, quality_notes = validate_record(bronze_record)
            
            # Create Silver record
            silver_record = WeatherRecordSilver(
                city=bronze_record.city,
                country=bronze_record.country,
                temperature=bronze_record.temperature,
                feels_like=bronze_record.feels_like,
                humidity=bronze_record.humidity,
                description=bronze_record.description,
                wind_speed=bronze_record.wind_speed,
                wind_direction=bronze_record.wind_direction,
                pressure=bronze_record.pressure,
                visibility=bronze_record.visibility,
                timestamp=bronze_record.timestamp,
                bronze_record_id=bronze_record.id,
                data_quality_flag=quality_flag,
                data_quality_notes=quality_notes
            )
            
            db.add(silver_record)
            silver_records_created += 1
            
            # Count by quality
            if quality_flag == "valid":
                valid_count += 1
            elif quality_flag == "suspect":
                suspect_count += 1
            else:
                invalid_count += 1
        
        # Commit the Silver records
        db.commit()
        
        # Log this transformation run (CHECKPOINT)
        max_timestamp = max([r.timestamp for r in bronze_records]) if bronze_records else cutoff_time
        
        transform_log = TransformationLog(
            transformation_name="bronze_to_silver",
            last_processed_timestamp=max_timestamp,
            records_processed=silver_records_created,
            status="success"
        )
        db.add(transform_log)
        db.commit()
        
        print(f"\n✅ Transformation complete:")
        print(f"   - Created {silver_records_created} Silver records")
        print(f"   - Valid: {valid_count}")
        print(f"   - Suspect: {suspect_count}")
        print(f"   - Invalid: {invalid_count}")
        print(f"   - Checkpoint saved: {max_timestamp}")
        print("="*70 + "\n")
        
    except Exception as e:
        print(f"❌ Error during transformation: {e}")
        
        # Log the failed run
        transform_log = TransformationLog(
            transformation_name="bronze_to_silver",
            last_processed_timestamp=cutoff_time if 'cutoff_time' in locals() else None,
            records_processed=0,
            status="failed"
        )
        db.add(transform_log)
        db.commit()
        
        db.rollback()
        raise
    
    finally:
        db.close()

if __name__ == "__main__":
    # Run the transformation
    transform_bronze_to_silver()