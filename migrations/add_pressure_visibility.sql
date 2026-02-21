-- Migration: Add pressure and visibility fields
-- Run on: weather_data.db (Production)
-- Date: 2025-02-20

-- Bronze layer
ALTER TABLE weather_records ADD COLUMN pressure INTEGER;
ALTER TABLE weather_records ADD COLUMN visibility INTEGER;

-- Silver layer
ALTER TABLE weather_records_silver ADD COLUMN pressure INTEGER;
ALTER TABLE weather_records_silver ADD COLUMN visibility INTEGER;

-- Gold layer
ALTER TABLE weather_daily_gold ADD COLUMN avg_pressure DECIMAL(6,2);
ALTER TABLE weather_daily_gold ADD COLUMN avg_visibility DECIMAL(7,2);

-- Verify
PRAGMA table_info(weather_records);
PRAGMA table_info(weather_records_silver);
PRAGMA table_info(weather_daily_gold);