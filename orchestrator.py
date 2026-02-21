"""
Transformation Orchestrator

Runs scheduled transformations across the medallion architecture:
- Bronze → Silver: Every hour
- Silver → Gold: Daily at 2am
- Gold → Analytics/Reporting: Daily at 2:05am

In production, this would be:
- Databricks Jobs (scheduled notebooks)
- Azure Data Factory (time-based triggers)
- Airflow DAGs
- Kubernetes CronJobs

But the orchestration logic is the same.
"""

import schedule
import time
import logging
from datetime import datetime
from config import get_config, ENV

# Import transformation functions
from transform_bronze_to_silver import transform_bronze_to_silver
from transform_silver_to_gold import transform_silver_to_gold
from transform_gold_to_analytics import transform_to_analytics
from transform_gold_to_reporting import transform_to_reporting_mart

# Get environment-specific settings
config = get_config()
LOG_FILE = f"orchestrator_{ENV}.log"

# Set up logging
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def run_bronze_to_silver():
    """
    Bronze → Silver transformation.
    
    Runs hourly to keep Silver layer fresh.
    """
    logger.info("="*70)
    logger.info("Starting Bronze → Silver transformation (scheduled)")
    logger.info("="*70)
    
    try:
        transform_bronze_to_silver()
        logger.info("✅ Bronze → Silver completed successfully")
    except Exception as e:
        logger.error(f"❌ Bronze → Silver failed: {e}", exc_info=True)
        # In production, you'd send an alert here


def run_silver_to_gold():
    """
    Silver → Gold transformation.
    
    Runs daily to create aggregated business metrics.
    """
    logger.info("="*70)
    logger.info("Starting Silver → Gold transformation (scheduled)")
    logger.info("="*70)
    
    try:
        transform_silver_to_gold()
        logger.info("✅ Silver → Gold completed successfully")
    except Exception as e:
        logger.error(f"❌ Silver → Gold failed: {e}", exc_info=True)


def run_gold_to_analytics_and_reporting():
    """
    Gold → Analytics/Reporting transformations.
    
    Runs daily after Gold layer is updated.
    """
    logger.info("="*70)
    logger.info("Starting Gold → Analytics/Reporting transformations (scheduled)")
    logger.info("="*70)
    
    # Analytics layer
    try:
        transform_to_analytics()
        logger.info("✅ Gold → Analytics completed successfully")
    except Exception as e:
        logger.error(f"❌ Gold → Analytics failed: {e}", exc_info=True)
    
    # Reporting mart
    try:
        transform_to_reporting_mart()
        logger.info("✅ Gold → Reporting completed successfully")
    except Exception as e:
        logger.error(f"❌ Gold → Reporting failed: {e}", exc_info=True)


def main():
    """
    Main orchestrator loop.
    
    Schedules all transformations and runs forever.
    """
    logger.info("🚀 Transformation Orchestrator Started")
    logger.info(f"📅 Environment: {ENV}")
    logger.info(f"📅 Bronze → Silver: Every hour")
    logger.info(f"📅 Silver → Gold: Daily at 2:00 AM")
    logger.info(f"📅 Gold → Analytics/Reporting: Daily at 2:05 AM")
    logger.info("\nPress Ctrl+C to stop\n")
    
    print("🚀 Transformation Orchestrator Started")
    print(f"📅 Environment: {ENV}")
    print(f"📅 Bronze → Silver: Every hour")
    print(f"📅 Silver → Gold: Daily at 2:00 AM")
    print(f"📅 Gold → Analytics/Reporting: Daily at 2:05 AM")
    print("\nPress Ctrl+C to stop\n")
    
    # Run Bronze → Silver immediately on startup
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Running initial Bronze → Silver transformation...")
    run_bronze_to_silver()
    
    # Schedule recurring jobs
    
    # Bronze → Silver: Every hour
    schedule.every().hour.do(run_bronze_to_silver) # PRODUCTION SCHEDULE
    # For testing: Uncomment to run every 5 minutes instead
    # schedule.every(5).minutes.do(run_bronze_to_silver)
    
    # Silver → Gold: Daily at 2:00 AM
    schedule.every().day.at("02:00").do(run_silver_to_gold) # PRODUCTION SCHEDULE
    # For testing: Uncomment to run every 5 minutes instead
    # schedule.every(6).minutes.do(run_silver_to_gold)
    
    # Gold → Analytics/Reporting: Daily at 2:10 AM (after Gold completes) # PRODUCTION SCHEDULE
    schedule.every().day.at("02:10").do(run_gold_to_analytics_and_reporting)
    # For testing: Uncomment to run every 5 minutes instead
    # schedule.every(7).minutes.do(run_gold_to_analytics_and_reporting)
    
    # Keep running forever
    try:
        while True:
            schedule.run_pending()
            time.sleep(1)
            
    except KeyboardInterrupt:
        logger.info("\n\n👋 Orchestrator stopped by user")
        print("\n\n👋 Orchestrator stopped by user")


if __name__ == "__main__":
    main()