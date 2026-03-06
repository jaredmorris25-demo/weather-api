def test_basic_math():
    """Sanity check - pipeline is running"""
    assert 1 + 1 == 2

def test_config_loads():
    """Config module loads correctly"""
    from config import get_config
    config = get_config()
    assert "database_url" in config
    assert "api_port" in config

def test_models_importable():
    """Database models load correctly"""
    from app.models import WeatherRecord
    assert WeatherRecord is not None
