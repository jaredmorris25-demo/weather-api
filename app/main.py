from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import text, desc, func
from typing import List, Optional
from datetime import datetime

from .database import get_db, engine
from .models import Base, WeatherRecord, WeatherRecordSilver, WeatherDailyGold, BatchLog
from .weather_client import WeatherClient
from pydantic import BaseModel

from contextlib import asynccontextmanager
import os

@asynccontextmanager
async def lifespan(app: FastAPI):
    yield  # Tables already created via migration

app = FastAPI(
    title="Weather API",
    description="A simple API to fetch and store weather data for cities.",
    version="1.0.0",
    lifespan=lifespan
)
weather_client = WeatherClient()

# ── Pydantic Response Models ─────────────────────────────────────────────────
# These define what the API *returns* — separate from DB models.
# Benefit: DB schema can evolve without breaking API consumers.

class WeatherResponse(BaseModel):
    id: int
    city: str
    country: Optional[str]
    temperature: float
    feels_like: Optional[float]
    humidity: Optional[int]
    wind_speed: Optional[float]
    description: Optional[str]
    timestamp: str

    class Config:
        from_attributes = True  # Allows SQLAlchemy model → Pydantic conversion


class SilverResponse(BaseModel):
    id: int
    city: str
    country: Optional[str]
    temperature: Optional[float]
    humidity: Optional[int]
    wind_speed: Optional[float]
    description: Optional[str]
    data_quality_flag: Optional[str]
    data_quality_notes: Optional[str]
    timestamp: str
    processed_at: Optional[str]

    class Config:
        from_attributes = True


class GoldResponse(BaseModel):
    id: int
    city: str
    country: Optional[str]
    date: str
    avg_temperature: Optional[float]
    max_temperature: Optional[float]
    min_temperature: Optional[float]
    avg_humidity: Optional[int]
    avg_wind_speed: Optional[float]
    most_common_description: Optional[str]
    total_readings: Optional[int]
    valid_readings: Optional[int]

    class Config:
        from_attributes = True

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
        wind_direction=weather_data['wind']['deg'],
        pressure=weather_data["main"]["pressure"],
        visibility=weather_data.get("visibility", 0)
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

# ── Phase 14: GET Endpoints ──────────────────────────────────────────────────

@app.get("/weather/records", response_model=List[WeatherResponse])
def get_weather_records(
    city: Optional[str] = None,
    limit: int = 100,
    offset: int = 0,
    db: Session = Depends(get_db)
):
    """
    Query Bronze layer with optional city filter and pagination.
    
    - city: Filter by city name. Omit for all cities.
    - limit/offset: Pagination. e.g. limit=100&offset=100 for page 2.
    """
    query = db.query(WeatherRecord).order_by(desc(WeatherRecord.timestamp))
    if city:
        query = query.filter(WeatherRecord.city == city)

    return [
        WeatherResponse(
            id=r.id, city=r.city, country=r.country,
            temperature=r.temperature, feels_like=r.feels_like,
            humidity=r.humidity, wind_speed=r.wind_speed,
            description=r.description, timestamp=str(r.timestamp)
        )
        for r in query.offset(offset).limit(limit).all()
    ]


@app.get("/weather/records/latest", response_model=List[WeatherResponse])
def get_all_latest(db: Session = Depends(get_db)):
    """
    Most recent Bronze record for each city.
    
    Uses a subquery to find max timestamp per city, then joins back
    to get the full row — avoids pulling all rows into Python.
    """
    subquery = (
        db.query(
            WeatherRecord.city,
            func.max(WeatherRecord.timestamp).label("max_ts")
        )
        .group_by(WeatherRecord.city)
        .subquery()
    )
    records = (
        db.query(WeatherRecord)
        .join(
            subquery,
            (WeatherRecord.city == subquery.c.city) &
            (WeatherRecord.timestamp == subquery.c.max_ts)
        )
        .order_by(WeatherRecord.city)
        .all()
    )
    return [
        WeatherResponse(
            id=r.id, city=r.city, country=r.country,
            temperature=r.temperature, feels_like=r.feels_like,
            humidity=r.humidity, wind_speed=r.wind_speed,
            description=r.description, timestamp=str(r.timestamp)
        )
        for r in records
    ]


@app.get("/weather/silver", response_model=List[SilverResponse])
def get_silver_records(
    city: Optional[str] = None,
    quality: Optional[str] = None,
    limit: int = 100,
    offset: int = 0,
    db: Session = Depends(get_db)
):
    """
    Query Silver (cleaned) layer.
    
    - quality: Filter by data_quality_flag — valid | suspect | invalid
    """
    query = db.query(WeatherRecordSilver).order_by(desc(WeatherRecordSilver.timestamp))
    if city:
        query = query.filter(WeatherRecordSilver.city == city)
    if quality:
        query = query.filter(WeatherRecordSilver.data_quality_flag == quality)

    return [
        SilverResponse(
            id=r.id, city=r.city, country=r.country,
            temperature=r.temperature, humidity=r.humidity,
            wind_speed=r.wind_speed, description=r.description,
            data_quality_flag=r.data_quality_flag,
            data_quality_notes=r.data_quality_notes,
            timestamp=str(r.timestamp),
            processed_at=str(r.processed_at) if r.processed_at else None
        )
        for r in query.offset(offset).limit(limit).all()
    ]


@app.get("/weather/gold", response_model=List[GoldResponse])
def get_gold_records(
    city: Optional[str] = None,
    limit: int = 30,
    offset: int = 0,
    db: Session = Depends(get_db)
):
    """
    Query Gold (daily aggregated) layer.
    
    Note: DECIMAL columns from SQLAlchemy are cast to float for JSON serialisation.
    Default limit 30 = roughly one month per city.
    """
    query = db.query(WeatherDailyGold).order_by(desc(WeatherDailyGold.date))
    if city:
        query = query.filter(WeatherDailyGold.city == city)

    return [
        GoldResponse(
            id=r.id, city=r.city, country=r.country,
            date=str(r.date),
            # DECIMAL → float cast needed for JSON serialisation
            avg_temperature=float(r.avg_temperature) if r.avg_temperature is not None else None,
            max_temperature=float(r.max_temperature) if r.max_temperature is not None else None,
            min_temperature=float(r.min_temperature) if r.min_temperature is not None else None,
            avg_humidity=r.avg_humidity,
            avg_wind_speed=float(r.avg_wind_speed) if r.avg_wind_speed is not None else None,
            most_common_description=r.most_common_description,
            total_readings=r.total_readings,
            valid_readings=r.valid_readings
        )
        for r in query.offset(offset).limit(limit).all()
    ]


@app.get("/weather/summary")
def get_summary(db: Session = Depends(get_db)):
    """
    Cross-layer record counts and data freshness per city.
    Useful as a pipeline health / dashboard endpoint.
    """
    bronze_count = db.query(func.count(WeatherRecord.id)).scalar()
    silver_count = db.query(func.count(WeatherRecordSilver.id)).scalar()
    gold_count = db.query(func.count(WeatherDailyGold.id)).scalar()

    latest_per_city = (
        db.query(WeatherRecord.city, func.max(WeatherRecord.timestamp).label("latest"))
        .group_by(WeatherRecord.city)
        .all()
    )

    return {
        "record_counts": {
            "bronze": bronze_count,
            "silver": silver_count,
            "gold": gold_count
        },
        "latest_ingestion_per_city": {
            row.city: str(row.latest) for row in latest_per_city
        }
    }

@app.get("/health")
def health_check(db: Session = Depends(get_db)):
    # Check database connectivity
    try:
        db.execute(text("SELECT 1"))
        db_status = "healthy"
    except Exception as e:
        db_status = f"unhealthy: {str(e)}"

    # Check OpenWeatherMap API key
    api_key = os.environ.get("OPENWEATHER_API_KEY")
    api_status = "configured" if api_key else "missing"

    overall = "healthy" if db_status == "healthy" and api_status == "configured" else "unhealthy"

    return {
        "status": overall,
        "version": "1.0.0",
        "checks": {
            "database": db_status,
            "openweathermap_api_key": api_status,
        }
    }