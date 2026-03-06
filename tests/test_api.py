from fastapi.testclient import TestClient
from app.main import app

client = TestClient(app)

def test_health_check():
    response = client.get("/")
    assert response.status_code == 200

def test_weather_history_empty():
    response = client.get("/weather/history/TestCityThatDoesNotExist")
    assert response.status_code == 200

def test_fetch_weather_invalid_city():
    response = client.post("/weather/fetch/zzznonsensecity999")
    assert response.status_code in [400, 404, 422]
