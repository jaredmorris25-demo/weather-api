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

API_BASE_URL = "http://localhost:8000"  # URL of your FastAPI app
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
    Fetch weather for all configured cities.

    This is your "job" or "task" that runs on a schedule.
    In Databricks, this would be a notebook.
    In Azure Data Factory, this would be a pipeline.

    """
    print("\n" + "="*60)
    print(f"Starting scheduled weather fetch at {datetime.now()}")
    print("="*60)

    success_count = 0
    fail_count = 0

    for city_config in CITIES_TO_FETCH:
        if fetch_weather_for_cities(city_config['city'], city_config['country_code']):
            success_count += 1
        else:
            fail_count += 1
            
        # Small delay between requests to avoid overwhelming the API
        time.sleep(2)

    print(f"\nBatch complete: {success_count} successes, {fail_count} failures at {datetime.now()}")
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

