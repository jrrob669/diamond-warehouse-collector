"""
Script principal avec scheduler.
Collecte quotidienne automatique à 22h15.
"""
import schedule
import time
import logging
from datetime import datetime

from utils.logger import setup_logging, log_section_header, log_metrics_summary
from utils.alerts import AlertSystem
from core.data_collector_v3 import OptionsDataCollectorV3
from storage.manager import StorageManager
from config import settings

logger = setup_logging()

collector = OptionsDataCollectorV3()
storage = StorageManager()
alerts = AlertSystem()

def daily_collection_job():
    """Job principal de collecte quotidienne."""
    log_section_header("STARTING DAILY COLLECTION JOB")
    
    today = datetime.now().strftime('%Y%m%d')
    start_time = time.time()
    
    try:
        for symbol in settings.SYMBOLS:
            logger.info(f"Processing {symbol}...")
            
            metrics, datasets = collector.calculate_daily_metrics(symbol, today)
            storage.save_daily_metrics(symbol, metrics)
            
            if 'gex_distribution' in datasets and not datasets['gex_distribution'].empty:
                storage.save_gex_distribution(symbol, today, datasets['gex_distribution'])
            
            log_metrics_summary(metrics)
            alerts.check_and_alert(metrics)
            storage.update_state(symbol, today, 'success')
        
        storage_stats = storage.get_storage_stats()
        logger.info(f"Storage: {storage_stats['total']['size_gb']:.2f} GB, "
                   f"{storage_stats['total']['file_count']} files")
        
        elapsed = time.time() - start_time
        logger.info(f"Collection completed in {elapsed:.1f}s")
        log_section_header("DAILY COLLECTION COMPLETED SUCCESSFULLY")
    
    except Exception as e:
        logger.error(f"Daily collection failed: {e}", exc_info=True)
        log_section_header("DAILY COLLECTION FAILED")

def main():
    """Boucle principale avec scheduler."""
    log_section_header("THETADATA WAREHOUSE - STARTING")
    
    logger.info(f"Symbols tracked: {', '.join(settings.SYMBOLS)}")
    logger.info("Scheduled job: 22:15 daily")
    logger.info("Press Ctrl+C to stop")
    
    schedule.every().day.at("22:15").do(daily_collection_job)
    
    try:
        while True:
            schedule.run_pending()
            time.sleep(60)
    
    except KeyboardInterrupt:
        logger.info("Shutdown requested by user")
        log_section_header("WAREHOUSE STOPPED")

if __name__ == "__main__":
    main()
