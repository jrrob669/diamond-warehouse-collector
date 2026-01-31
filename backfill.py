"""
Script de backfill pour remplissage historique.
Usage: python backfill.py --symbol SPY --start 2020-01-01 --end 2025-01-28
"""
import argparse
import logging
from datetime import datetime
import time
import pandas as pd

from utils.logger import setup_logging, log_section_header
from core.data_collector import OptionsDataCollector
from storage.manager import StorageManager
from config import settings

logger = setup_logging()

def backfill_date_range(symbol: str, start_date: str, end_date: str):
    collector = OptionsDataCollector()
    storage = StorageManager()
    
    start = pd.to_datetime(start_date)
    end = pd.to_datetime(end_date)
    date_range = pd.bdate_range(start, end)
    
    log_section_header(f"BACKFILL: {symbol} from {start_date} to {end_date}")
    logger.info(f"Total business days to process: {len(date_range)}")
    
    success_count = 0
    
    for i, date in enumerate(date_range, 1):
        date_str = date.strftime('%Y%m%d')
        logger.info(f"[{i}/{len(date_range)}] Processing {date_str}...")
        
        try:
            metrics, datasets = collector.calculate_daily_metrics(symbol, date_str)
            storage.save_daily_metrics(symbol, metrics)
            
            if 'gex_distribution' in datasets and not datasets['gex_distribution'].empty:
                storage.save_gex_distribution(symbol, date_str, datasets['gex_distribution'])
            
            storage.update_state(symbol, date_str, 'success')
            success_count += 1
            logger.info(f"✓ {date_str} completed")
            time.sleep(1)
        
        except Exception as e:
            logger.error(f"✗ {date_str} failed: {e}")
    
    log_section_header("BACKFILL COMPLETED")
    logger.info(f"Success: {success_count}/{len(date_range)}")

def main():
    parser = argparse.ArgumentParser(description='Backfill historical options data')
    parser.add_argument('--symbol', type=str, default='SPY')
    parser.add_argument('--start', type=str, required=True)
    parser.add_argument('--end', type=str, required=True)
    args = parser.parse_args()
    
    backfill_date_range(args.symbol, args.start, args.end)

if __name__ == "__main__":
    main()
