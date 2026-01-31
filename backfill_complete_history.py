"""
Backfill historique complet : Prix stock + RR25 par DTE.
Usage: python backfill_complete_history.py --symbol SPY --start 2020-01-01 --end 2025-01-28
"""
import argparse
import pandas as pd
from datetime import datetime
import time
import logging

from utils.logger import setup_logging, log_section_header
from core.data_collector_v3 import OptionsDataCollectorV3
from storage.manager import StorageManager
from config import settings

logger = setup_logging()

def backfill_complete_history(symbol: str, start_date: str, end_date: str):
    """
    Collecte historique COMPLET :
    1. Prix stock EOD
    2. Métriques quotidiennes (Net Gamma, etc.)
    3. RR25 par DTE (0, 7, 30, 60)
    4. Surface IV
    """
    collector = OptionsDataCollectorV3()
    storage = StorageManager()
    
    start = pd.to_datetime(start_date)
    end = pd.to_datetime(end_date)
    date_range = pd.bdate_range(start, end)
    
    log_section_header(f"BACKFILL COMPLET: {symbol} from {start_date} to {end_date}")
    logger.info(f"Total business days: {len(date_range)}")
    logger.info("Collecting:")
    logger.info("  ✓ Stock prices (OHLCV)")
    logger.info("  ✓ Net Delta/Gamma")
    logger.info("  ✓ RR25 par DTE (0, 7, 30, 60)")
    logger.info("  ✓ IV ATM par DTE")
    logger.info("  ✓ GEX, P/C ratios, Liquidity")
    
    # PHASE 1 : Historique prix stock complet (rapide)
    logger.info("\n" + "="*80)
    logger.info("PHASE 1: Collecting complete stock price history")
    logger.info("="*80)
    
    collector.collect_stock_price_history(symbol, start_date.replace('-', ''), end_date.replace('-', ''))
    
    # PHASE 2 : Métriques quotidiennes + Term Structure (lent)
    logger.info("\n" + "="*80)
    logger.info("PHASE 2: Collecting daily metrics + term structure")
    logger.info("="*80)
    
    success_count = 0
    failed_dates = []
    
    for i, date in enumerate(date_range, 1):
        date_str = date.strftime('%Y%m%d')
        logger.info(f"\n[{i}/{len(date_range)}] Processing {date_str}...")
        
        try:
            start_time = time.time()
            
            # Collecte complète avec term structure
            metrics, datasets = collector.calculate_daily_metrics(symbol, date_str)
            
            # Sauvegarder
            storage.save_daily_metrics(symbol, metrics)
            
            if 'gex_distribution' in datasets and not datasets['gex_distribution'].empty:
                storage.save_gex_distribution(symbol, date_str, datasets['gex_distribution'])
            
            storage.update_state(symbol, date_str, 'success')
            
            elapsed = time.time() - start_time
            
            # Afficher métriques clés
            logger.info(f"  ✓ Completed in {elapsed:.1f}s")
            logger.info(f"    Spot: ${metrics.get('spot_price', 'N/A'):.2f}")
            logger.info(f"    Net Gamma: ${metrics.get('net_gamma_billions', 'N/A'):.2f}B")
            logger.info(f"    RR25 7DTE: {metrics.get('ts_7dte_rr25', 'N/A')}")
            
            success_count += 1
            time.sleep(1)  # Rate limiting
        
        except Exception as e:
            logger.error(f"  ✗ Failed: {e}")
            failed_dates.append(date_str)
    
    # RÉSUMÉ
    log_section_header("BACKFILL COMPLETED")
    logger.info(f"Success: {success_count}/{len(date_range)} days")
    
    if failed_dates:
        logger.warning(f"\nFailed dates ({len(failed_dates)}):")
        for d in failed_dates[:10]:
            logger.warning(f"  - {d}")
        if len(failed_dates) > 10:
            logger.warning(f"  ... and {len(failed_dates) - 10} more")
    
    # Statistiques finales
    logger.info("\n" + "="*80)
    logger.info("FINAL STATISTICS")
    logger.info("="*80)
    
    # Charger et afficher stats
    df_master = storage.load_master_daily(symbol)
    
    if not df_master.empty:
        logger.info(f"\nTotal days collected: {len(df_master)}")
        logger.info(f"Date range: {df_master['date'].min().date()} to {df_master['date'].max().date()}")
        
        # Complétude term structure
        ts_cols = [c for c in df_master.columns if 'ts_' in c and 'dte_rr25' in c]
        logger.info("\nTerm Structure Completeness:")
        for col in ts_cols:
            pct = (1 - df_master[col].isna().sum() / len(df_master)) * 100
            logger.info(f"  {col:.<30} {pct:>5.1f}%")
    
    # Stats historique prix
    history = collector.price_history.get_complete_history(symbol)
    logger.info(f"\nStock Price History:")
    logger.info(f"  Days: {history['summary']['stock_days']}")
    logger.info(f"  Range: {history['summary']['stock_start']} to {history['summary']['stock_end']}")
    
    logger.info(f"\nTerm Structure History:")
    logger.info(f"  Days: {history['summary']['ts_days']}")
    logger.info(f"  Range: {history['summary']['ts_start']} to {history['summary']['ts_end']}")
    
    # Storage
    storage_stats = storage.get_storage_stats()
    logger.info(f"\nStorage Usage:")
    logger.info(f"  Total: {storage_stats['total']['size_gb']:.2f} GB")
    logger.info(f"  Files: {storage_stats['total']['file_count']}")

def main():
    parser = argparse.ArgumentParser(description='Backfill complete history: prices + term structure')
    parser.add_argument('--symbol', type=str, default='SPY', help='Ticker symbol')
    parser.add_argument('--start', type=str, required=True, help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end', type=str, required=True, help='End date (YYYY-MM-DD)')
    args = parser.parse_args()
    
    backfill_complete_history(args.symbol, args.start, args.end)

if __name__ == "__main__":
    main()
