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