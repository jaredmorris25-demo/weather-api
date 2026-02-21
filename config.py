"""
Environment configuration.
"""

import os

ENV = os.getenv("APP_ENV", "dev")

ENVIRONMENTS = {
    "dev": {
        "database_url": "sqlite:///./weather_data_dev.db",
        "api_port": 8001,
        "log_file": "scheduler_dev.log"
    },
    "uat": {
        "database_url": "sqlite:///./weather_data_uat.db",
        "api_port": 8002,
        "log_file": "scheduler_uat.log"
    },
    "prod": {
        "database_url": "sqlite:///./weather_data.db",  # Keep original name!
        "api_port": 8000,
        "log_file": "scheduler.log"
    }
}

def get_config():
    if ENV not in ENVIRONMENTS:
        raise ValueError(f"Invalid environment: {ENV}")
    return ENVIRONMENTS[ENV]

DATABASE_URL = get_config()["database_url"]
API_PORT = get_config()["api_port"]
LOG_FILE = get_config()["log_file"]