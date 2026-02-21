"""
Weather Data Scheduler

Fetches weather data for a specified city at regular intervals and stores it in the database.

In Production, this would be replaced by:
- Azure Data Factory scheduled pipeline
- Databricks job scheduler
- Kubernetes CronJob
- Azure Functions with Timer Trigger

But for learning, we're using Python's schedule library to simulate the scheduling.
"""

import schedule
import time
import requests
from datetime import datetime
import logging
from config import get_config

# Get environment-specific settings
config = get_config()
API_BASE_URL = f"http://127.0.0.1:{config['api_port']}"
LOG_FILE = config['log_file']

# Set up file logging with environment-specific log file
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# API_BASE_URL = "http://localhost:8000"  # URL of your FastAPI app
CITIES_TO_FETCH = [
    {"city": "Brisbane", "country_code": "AU"},
    {"city": "Sydney", "country_code": "AU"},
    {"city": "Melbourne", "country_code": "AU"},
    {"city": "Perth", "country_code": "AU"},
    {"city": "Adelaide", "country_code": "AU"}
]

FETCH_INTERVAL_MINUTES = 20  # Fetch every 20 minutes


def fetch_weather_for_cities(city: str, country_code: str   ):
    """
    Fetch weather data for a single city.

    This is the orchestration logic calling the API endpoint and handling success/failure.
    """
    try:
        url = f"{API_BASE_URL}/weather/fetch/{city}"
        params = {"country_code": country_code}

        print(f"Fetching weather for {city}, {country_code} at {datetime.now()}")

        response = requests.post(url, params=params)
        response.raise_for_status()  # Raise an error for bad responses

        data = response.json()
        print(f"  ✅ Success: {data['temperature']}°C - {data['description']}")

        return True
    
    except requests.exceptions.ConnectionError:
        print(f"  ❌ Connection error while fetching weather for {city}, {country_code}")
        return False
    
    except requests.exceptions.HTTPError as e:
        print(f"  ❌ HTTP error while fetching weather for {city}, {country_code}: {e}")
        return False
    
    except Exception as e:
        print(f"  ❌ Unexpected error while fetching weather for {city}, {country_code}: {e}")
        return False
    

def fetch_all_cities():
    """
    Fetch weather for all configured cities and log the batch run.
    """
    batch_start = datetime.now()

    logger.info(f"Starting batch: {len(CITIES_TO_FETCH)} cities")
    print("\n" + "="*60)
    print(f"Starting scheduled weather fetch at {datetime.now()}")
    print("="*60)

    success_count = 0
    fail_count = 0
    error_messages = []

    for city_config in CITIES_TO_FETCH:
        if fetch_weather_for_cities(city_config['city'], city_config['country_code']):
            success_count += 1
            logger.info(f"Successfully fetched weather for {city_config['city']}, {city_config['country_code']}")
        else:
            fail_count += 1
            error_messages.append(f"Failed to fetch {city_config['city']}, {city_config['country_code']}")
            logger.error(f"Failed to fetch weather for {city_config['city']}, {city_config['country_code']}")
            
        # Small delay between requests to avoid overwhelming the API
        time.sleep(2)    

    batch_end = datetime.now()
    duration = (batch_end - batch_start).total_seconds()

    logger.info(f"Batch complete: {success_count} successful, {fail_count} failed, duration: {duration:.2f} seconds")

    print(f"\nBatch complete: {success_count} successful, {fail_count} failed")
    print(f"Duration: {duration:.2f} seconds")

# Try to log to database (might fail if API is down)
    try:
        log_url = f"{API_BASE_URL}/batch/log"
        log_data = {
            "batch_start": batch_start.isoformat(),
            "batch_end": batch_end.isoformat(),
            "cities_attempted": len(CITIES_TO_FETCH),
            "cities_successful": success_count,
            "cities_failed": fail_count,
            "duration_seconds": duration,
            "error_message": "; ".join(error_messages) if error_messages else None
        }
        
        response = requests.post(log_url, params=log_data)
        response.raise_for_status()
        print(f"✅ Batch run logged to database")
        logger.info("Batch logged to database successfully")
        
    except Exception as e:
        print(f"⚠️  Warning: Could not log batch run to database: {e}")
        logger.warning(f"Could not log to database: {e}")
        
    print("="*60 + "\n")


def main():
    """
    Main scheduler loop.
    
    In production, you'd use:
    - Databricks Jobs (scheduled notebooks)
    - Azure Data Factory (time-based triggers)
    - Airflow DAGs
    - Kubernetes CronJobs
    
    But the logic is the same: run a task on a schedule.
    """
    print("🚀 Weather Data Scheduler Started")
    print(f"📅 Fetching weather every {FETCH_INTERVAL_MINUTES} minutes")
    print(f"🌍 Monitoring cities: {', '.join([c['city'] for c in CITIES_TO_FETCH])}")
    print(f"🔗 API endpoint: {API_BASE_URL}")
    print("\nPress Ctrl+C to stop\n")
    
    # Run once immediately (so you don't wait 10 minutes to see it work)
    fetch_all_cities()
    
    # Schedule regular runs
    schedule.every(FETCH_INTERVAL_MINUTES).minutes.do(fetch_all_cities)
    
    # Keep running forever
    try:
        while True:
            schedule.run_pending()
            time.sleep(1)  # Check every second if it's time to run
            
    except KeyboardInterrupt:
        print("\n\n👋 Scheduler stopped by user")

if __name__ == "__main__":
        main()

