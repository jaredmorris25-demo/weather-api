from sqlalchemy import Column, Integer, String, Float, DateTime
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime

Base = declarative_base()

class WeatherRecord(Base):
    """
    Represents a weather data record in the database.
    Think of this as the 'schema' or 'table definition' - it tells SQLAlchemy
    what columns we want in our 'weather_records' table and how to map them to Python objects.
    """
    __tablename__ = 'weather_records'

    id = Column(Integer, primary_key=True, index=True)      # City Name
    city = Column(String(100), nullable=False)              # Country Code
    country = Column(String(2), nullable=True)              # Country Code (optional)
    temperature = Column(Float, nullable=False)             # Temperature in Celsius
    feels_like = Column(Float, nullable=False)              # Feels Like Temperature in Celsius
    humidity = Column(Integer, nullable=False)              # humidity percentage
    description = Column(String(255), nullable=False)       # Weather Description
    wind_speed = Column(Float, nullable=False)              # Wind Speed in m/s
    wind_direction = Column(Integer, nullable=True)         # Wind Direction in degrees
    timestamp = Column(DateTime, default=datetime.utcnow)   # Timestamp of when the data was recorded

    def __repr__(self):
        return f"<WeatherRecord(city_name='{self.city}', temperature={self.temperature}, humidity={self.humidity}, description='{self.description}', wind_speed={self.wind_speed}, timestamp='{self.timestamp}')>"
    
class BatchLog(Base):
    """
    Tracks scheduler batch runs - successes, failures, timing.

    In production, this is your "job run history" or "pipeline execution log".
    Critical for debugging: "Why did last night's data load fail?"
    """
    __tablename__ = "batch_logs"

    id = Column(Integer, primary_key=True, index=True)
    batch_start_time = Column(DateTime)
    batch_end_time = Column(DateTime)
    cities_attempted = Column(Integer)       # How many cities we tried
    cities_successful = Column(Integer)      # How many succeeded
    cities_failed = Column(Integer)          # How many failed
    error_message = Column(String, nullable=True)  # Any errors encountered
    duration_seconds = Column(Float)         # How long the batch took

    def __repr__(self):
        return f"<BatchLog(start={self.batch_start_time}, success={self.cities_successful}/{self.cities_attempted})>"