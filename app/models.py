from sqlalchemy import Column, Integer, String, Float, DateTime, Date, Boolean
from sqlalchemy import DECIMAL
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
    pressure = Column(Integer)        # NEW FIELD
    visibility = Column(Integer)      # NEW FIELD
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
    
class WeatherRecordSilver(Base):
    """
    Silver layer: Cleaned and validated weather data.
    
    Transformations from Bronze:
    - Deduplicated (one record per city per hour)
    - Data quality checks applied
    - Suspicious values flagged
    - Ready for analytics
    
    In production, this would be a Delta table in Databricks.
    """
    __tablename__ = "weather_records_silver"
    
    id = Column(Integer, primary_key=True, index=True)
    city = Column(String, index=True)
    country = Column(String)
    temperature = Column(Float)
    feels_like = Column(Float)
    humidity = Column(Integer)
    description = Column(String)
    wind_speed = Column(Float)
    wind_direction = Column(Integer)
    pressure = Column(Integer)
    visibility = Column(Integer)
    
    # Silver layer additions
    timestamp = Column(DateTime, index=True)           # When this reading is for
    processed_at = Column(DateTime, default=datetime.utcnow)  # When we created this record
    bronze_record_id = Column(Integer)                 # Link back to bronze source
    data_quality_flag = Column(String)                 # "valid", "suspect", "invalid"
    data_quality_notes = Column(String, nullable=True) # Why it was flagged
    
    def __repr__(self):
        return f"<WeatherRecordSilver(city={self.city}, temp={self.temperature}°C, quality={self.data_quality_flag})>"
    
class TransformationLog(Base):
    """
    Tracks transformation job runs - when they ran, what they processed.
    
    This is your "checkpoint" or "high water mark" pattern.
    Ensures no data is missed even if jobs fail or are down for days.
    
    In production, this is how you:
    - Resume from where you left off
    - Monitor job health
    - Debug data pipeline issues
    """
    __tablename__ = "transformation_logs"
    
    id = Column(Integer, primary_key=True, index=True)
    transformation_name = Column(String, index=True)  # e.g., "bronze_to_silver"
    last_processed_timestamp = Column(DateTime)       # Latest Bronze timestamp we processed
    run_timestamp = Column(DateTime, default=datetime.utcnow)  # When this transform ran
    records_processed = Column(Integer)               # How many records created
    status = Column(String)                           # "success" or "failed"
    
    def __repr__(self):
        return f"<TransformationLog({self.transformation_name} @ {self.run_timestamp}: {self.status})>"
    
class WeatherDailyGold(Base):
    """
    Gold layer: Daily weather summaries per city.
    
    Business logic applied, aggregated from Silver.
    Source of truth for daily weather patterns.
    """
    __tablename__ = "weather_daily_gold"
    
    id = Column(Integer, primary_key=True, index=True)
    city = Column(String, index=True)
    country = Column(String)
    date = Column(Date, index=True)
    
    # Temperature aggregations
    avg_temperature = Column(DECIMAL(5,2))
    max_temperature = Column(DECIMAL(5,2))
    min_temperature = Column(DECIMAL(5,2))
    
    # Other weather metrics
    avg_humidity = Column(Integer)
    max_humidity = Column(Integer)
    min_humidity = Column(Integer)
    avg_wind_speed = Column(DECIMAL(5,2))
    avg_pressure = Column(DECIMAL(6,2))
    avg_visibility = Column(DECIMAL(7,2))
    
    # Most common weather condition for the day
    most_common_description = Column(String)
    
    # Data quality
    total_readings = Column(Integer)
    valid_readings = Column(Integer)
    
    # Metadata
    created_at = Column(DateTime, default=datetime.utcnow)
    
    def __repr__(self):
        return f"<WeatherDailyGold({self.city}, {self.date}: {self.avg_temperature}°C)>"
    
class WeatherAnalyticsLayer(Base):
    """
    Analytics Layer: Enriched hourly data for analysis.
    
    Filtered and enhanced from Silver for specific analytical use cases.
    Example: Clear sky days with high temperatures.
    """
    __tablename__ = "weather_analytics_layer"
    
    id = Column(Integer, primary_key=True, index=True)
    city = Column(String, index=True)
    country = Column(String)
    timestamp = Column(DateTime, index=True)
    
    # Core measurements
    temperature = Column(Float)
    humidity = Column(Integer)
    wind_speed = Column(Float)
    description = Column(String)
    
    # Analytical flags
    is_hot_clear_day = Column(Boolean)  # temp > 30 AND clear sky
    
    # Link back to Silver
    silver_record_id = Column(Integer)
    
    # Metadata
    created_at = Column(DateTime, default=datetime.utcnow)
    
    def __repr__(self):
        return f"<WeatherAnalytics({self.city}, {self.timestamp}: {self.temperature}°C, hot_clear={self.is_hot_clear_day})>"
    
class WeatherReportingMart(Base):
    """
    Reporting Mart: Pre-aggregated dashboard data.
    
    Optimized for PowerBI queries.
    Updated daily from Gold layer.
    """
    __tablename__ = "weather_reporting"
    
    id = Column(Integer, primary_key=True, index=True)
    city = Column(String, index=True)
    date = Column(Date, index=True)
    
    # Dashboard metrics (simplified from Gold)
    max_temperature = Column(DECIMAL(5,2))
    min_temperature = Column(DECIMAL(5,2))
    avg_wind_speed = Column(DECIMAL(5,2))
    
    # Metadata
    created_at = Column(DateTime, default=datetime.utcnow)
    
    def __repr__(self):
        return f"<WeatherReportingMart({self.city}, {self.date}: {self.min_temperature}-{self.max_temperature}°C)>"