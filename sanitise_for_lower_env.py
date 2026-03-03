import sqlite3
import shutil
import argparse
from datetime import datetime

def refresh_environment(source_db, target_db, environment):
    """
    Copy Prod snapshot to lower environment.
    
    Key principles:
    - Never modify source (Prod snapshot)
    - Sanitise any sensitive data
    - Preserve referential integrity
    - Log what was done and when
    """
    
    print(f"[{datetime.now()}] Starting {environment} refresh from {source_db}")
    
    # Work on a copy of the snapshot, never the original
    shutil.copy2(source_db, target_db)
    
    conn = sqlite3.connect(target_db)
    cursor = conn.cursor()
    
    # --------------------------------------------------------
    # SANITISATION RULES
    # In weather data there's nothing sensitive.
    # In MHR-equivalent data you would do things like:
    #   UPDATE patients SET name = 'Test User ' || id
    #   UPDATE patients SET dob = '1900-01-01'
    #   UPDATE records SET medicare_number = '0000000000'
    # --------------------------------------------------------
    
    # For learning: at minimum, mark the data as non-production
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS _env_metadata (
            key TEXT PRIMARY KEY,
            value TEXT,
            updated_at TIMESTAMP
        )
    """)
    
    cursor.execute("""
        INSERT OR REPLACE INTO _env_metadata (key, value, updated_at)
        VALUES 
            ('source_environment', 'prod', ?),
            ('refresh_date', ?, ?),
            ('target_environment', ?, ?)
    """, (datetime.now(), datetime.now(), datetime.now(), 
          environment, datetime.now()))
    
    # If DEV: optionally subset the data (don't need 2 years of prod data in Dev)
    if environment == 'dev':
        print("  Dev environment: subsetting to last 30 days only")
        cursor.execute("""
            DELETE FROM weather_records 
            WHERE timestamp < datetime('now', '-30 days')
        """)
        # Cascade to other layers
        cursor.execute("""
            DELETE FROM weather_records_silver 
            WHERE processed_at < datetime('now', '-30 days')
        """)
        
        row_count = cursor.execute(
            "SELECT COUNT(*) FROM weather_records"
        ).fetchone()[0]
        print(f"  Dev subset: {row_count} Bronze records retained")
    
    conn.commit()
    conn.close()
    
    print(f"[{datetime.now()}] Refresh complete → {target_db}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", required=True)
    parser.add_argument("--target", required=True)
    parser.add_argument("--environment", required=True, choices=["uat", "dev"])
    args = parser.parse_args()
    
    refresh_environment(args.source, args.target, args.environment)