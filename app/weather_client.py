import os
import requests
from dotenv import load_dotenv
import json

load_dotenv()

class WeatherClient:
    def __init__(self):
        self.api_key = os.getenv('OPENWEATHER_API_KEY')
        self.base_url = os.getenv('OPENWEATHER_BASE_URL')

        if not self.api_key or not self.base_url:
            raise ValueError("API key and base URL must be set in the environment variables.")

    def get_weather(self, city: str, country_code: str = None):
        """
        Fetch current weather for a city.
        
        Args:
            city: City name (e.g., 'Brisbane')
            country_code: Optional 2-letter country code (e.g., 'AU')
        
        Returns:
            dict: Weather data from API, or None if request fails
        """
        query = f"{city},{country_code}" if country_code else city

        params = {
            'q': query,
            'appid': self.api_key,
            'units': 'metric' # Use Celsius
        }

        try:
            response = requests.get(self.base_url, params=params)
            response.raise_for_status()  # Raise an HTTPError for bad responses (4xx and 5xx)
            data = response.json()

            print('Debug - full api response:', json.dumps(data, indent=2))  # Debug: print full API response

            return data
        
        except requests.exceptions.RequestException as e:
            print(f"Error fetching weather data: {e}")
            return None 

