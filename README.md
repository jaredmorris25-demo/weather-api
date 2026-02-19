# Weather Data API

A FastAPI-based weather data aggregation service that demonstrates:
    - REST API development
    - External API integration
    - Database storage (Bronze layer)
    - Version control with Git

## SETUP

1. Install dependencies:
'''bash
    pip install -r requirements.txt
'''

2. Create '.env' file with your OpenWeatherMap API Key

3. Run the API:
'''bash
    uvicorn app.main:app -- reload
'''

4. Visit http://127.0.0.1:8000/docs for interactive API documentation

## Endpoints

    - 'POST /weather/fetch/{city}' - Fetch and store weather data
    - 'GET /weather/history/{city} - Get all recrords for a city
    - 'GET /weather/latest/{city} - Get most recent record for a city

## Current Status

   - ✅ Basic CRUD API working
   - ✅ Connected to OpenWeatherMap
   - 🚧 Next: Add scheduling and Silver layer transformations