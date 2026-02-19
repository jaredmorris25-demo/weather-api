from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import List
from datetime import datetime

from .database import get_db
from .models import WeatherRecord
from .weather_client import WeatherClient

from .models import BatchLog

app = FastAPI(
    title="Weather API", 
    description="A simple API to fetch and store weather data for cities.",
    version="1.0.0"
)
weather_client = WeatherClient()

@app.get("/")
def read_root():
    return {"message": "Weather API is running",
            "version": "1.0.0",
            "endpoints": {
                "/weather/{city}": "Get current weather for a city",
                "/weather/records": "Get all stored weather records"
            }
    }

@app.post("/weather/fetch/{city}")
def fetch_weather(city: str, country_code: str = "AU", db: Session = Depends(get_db)):
    """
        Fetch weather data from OpenWeatherMap and store it in database.
        
        This is your 'ingest' endpoint - it pulls data from external source
        and writes to your Bronze layer (raw storage).
        
        Args:
            city: City name
            country_code: Country code (defaults to AU)
            db: Database session (automatically injected by FastAPI)
        
        Returns:
            The stored weather record
    """
    
# Call OpenWeather API to get current weather data
    weather_data = weather_client.get_weather(city, country_code)

    if not weather_data:
        raise HTTPException(status_code=404, detail="Weather data not found for the specified city")

    # Extract relevant fields from API response
    # This is basic transformation - in real pipeplines you'd do much more here
    record = WeatherRecord(
        city=weather_data['name'],
        country=weather_data['sys']['country'],
        temperature=weather_data['main']['temp'],
        feels_like=weather_data['main']['feels_like'],
        humidity=weather_data['main']['humidity'],
        description=weather_data['weather'][0]['description'],
        wind_speed=weather_data['wind']['speed'],
        wind_direction=weather_data['wind']['deg']
    )

    # Store the record in the database
    db.add(record)
    db.commit()
    db.refresh(record)
    
    return {
    "message": f"Weather data for {record.city} stored successfully",
    "record_id": record.id,
    "temperature": record.temperature,
    "description": record.description,
}

@app.get("/weather/history/{city}")
def get_weather_history(city: str, db: Session = Depends(get_db)):
    """
    Retrieve all stored weather records for a city.
    
    This is your 'query' endpoint - reading from your Bronze layer.
    Later, we'll create Silver/Gold layers with aggregations.
    
    Args:
        city: City name
        db: Database session
    
    Returns:
        List of all weather records for that city
    """
    records = db.query(WeatherRecord).filter(WeatherRecord.city == city).all()
    
    if not records:
        raise HTTPException(status_code=404, detail="No weather records found for the specified city")
    
    return {
        "city": city,
        "record_count": len(records),
        "records": [
            {
                "id": r.id,
                "temperature": r.temperature,
                "feels_like": r.feels_like,
                "humidity": r.humidity,
                "description": r.description,
                "wind_speed": r.wind_speed,
                "timestamp": r.timestamp
            }
            for r in records
        ]
    }

@app.get("/weather/latest/{city}")
def get_latest_weather(city: str, db: Session = Depends(get_db)):
    """
    Get the most recent weather record for a city.
    
    Args:
        city: City name
        db: Database session
    
    Returns:
        Most recent weather record
    """
    record = db.query(WeatherRecord)\
        .filter(WeatherRecord.city == city)\
        .order_by(WeatherRecord.timestamp.desc())\
        .first()
    
    if not record:
        raise HTTPException(status_code=404, detail=f"No weather data found for {city}")
    
    return {
        "city": record.city,
        "country": record.country,
        "temperature": record.temperature,
        "feels_like": record.feels_like,
        "humidity": record.humidity,
        "description": record.description,
        "wind_speed": record.wind_speed,
        "timestamp": record.timestamp
    }

@app.delete("/weather/record/{record_id}")
def delete_weather_record(record_id: int, db: Session = Depends(get_db)):
    """
    Delete a specific weather record by ID.
    
    In production, you'd authenticate this endpoint and log the deletion.
    For My Health Record scale, you'd use SQL for bulk deletions, not APIs.
    
    Args:
        record_id: The ID of the record to delete
        db: Database session
    
    Returns:
        Confirmation of deletion
    """
    # Find the record
    record = db.query(WeatherRecord).filter(WeatherRecord.id == record_id).first()
    
    if not record:
        raise HTTPException(status_code=404, detail=f"Record {record_id} not found")
    
    # Store info before deletion (for the response)
    deleted_info = {
        "id": record.id,
        "city": record.city,
        "temperature": record.temperature,
        "timestamp": record.timestamp
    }
    
    # Delete it
    db.delete(record)
    db.commit()
    
    return {
        "message": "Record deleted successfully",
        "deleted_record": deleted_info
    }

@app.post("/batch/log")
def log_batch_run(
    batch_start: str,
    batch_end: str,
    cities_attempted: int,
    cities_successful: int,
    cities_failed: int,
    duration_seconds: float,
    error_message: str = None,
    db: Session = Depends(get_db)
):
    """
    Log a batch run to the database.
    
    In production, every ETL job logs its execution:
    - When did it start/end?
    - Did it succeed?
    - How much data was processed?
    - Any errors?
    
    This is how you debug production issues at 2am.
    """
    log_entry = BatchLog(
        batch_start_time=datetime.fromisoformat(batch_start),
        batch_end_time=datetime.fromisoformat(batch_end),
        cities_attempted=cities_attempted,
        cities_successful=cities_successful,
        cities_failed=cities_failed,
        duration_seconds=duration_seconds,
        error_message=error_message
    )
    
    db.add(log_entry)
    db.commit()
    db.refresh(log_entry)
    
    return {
        "message": "Batch run logged successfully",
        "log_id": log_entry.id
    }


@app.get("/batch/history")
def get_batch_history(limit: int = 10, db: Session = Depends(get_db)):
    """
    View recent batch run history.
    
    In production, this powers your monitoring dashboards:
    "Show me the last 24 hours of job runs"
    """
    logs = db.query(BatchLog)\
        .order_by(BatchLog.batch_start_time.desc())\
        .limit(limit)\
        .all()
    
    return {
        "total_batches": len(logs),
        "batches": [
            {
                "id": log.id,
                "start_time": log.batch_start_time,
                "end_time": log.batch_end_time,
                "duration_seconds": log.duration_seconds,
                "attempted": log.cities_attempted,
                "successful": log.cities_successful,
                "failed": log.cities_failed,
                "error": log.error_message
            }
            for log in logs
        ]
    }