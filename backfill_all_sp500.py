#!/usr/bin/env python3
"""
Backfill massif adapte a la souscription FREE ThetaData:
- Stock EOD: ~1 an via ThetaData + 5 ans via yfinance
- Options greeks/metrics: periode disponible via ThetaData
- Reprend automatiquement (state tracking)
"""
import json
import time
import logging
import sys
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, '/home/ubuntu/warehouse_collector')

from utils.logger import setup_logging, log_section_header
from core.api_wrapper import ThetaDataAPI
from core.calculations import (
    calculate_net_exposures, calculate_realized_volatility,
    calculate_skew_metrics, calculate_pc_ratios,
    calculate_gex_distribution, analyze_liquidity
)
from core.term_structure import calculate_term_structure_metrics
from core.price_history import PriceHistoryManager
from storage.manager import StorageManager
from config import settings

logger = setup_logging()

STATE_FILE = Path('/home/ubuntu/warehouse_data/state/backfill_progress.json')

def load_progress():
    if STATE_FILE.exists():
        with open(STATE_FILE) as f:
            return json.load(f)
    return {'completed_symbols': [], 'current_symbol': None, 'failed': {}, 'phase': 'stock_history'}

def save_progress(progress):
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(STATE_FILE, 'w') as f:
        json.dump(progress, f, indent=2)


def collect_stock_history_yfinance(symbol, start_date, end_date, price_history_mgr):
    """Collect 5 years of stock OHLCV via yfinance (free, no subscription needed)."""
    try:
        import yfinance as yf
        ticker = yf.Ticker(symbol)
        df = ticker.history(start=start_date, end=end_date, auto_adjust=False)

        if df.empty:
            logger.warning(f"{symbol}: No yfinance data")
            return 0

        # Normalize columns
        df = df.reset_index()
        df.columns = [c.lower().replace(' ', '_') for c in df.columns]
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date']).dt.tz_localize(None)

        # Keep OHLCV
        keep_cols = [c for c in ['date', 'open', 'high', 'low', 'close', 'volume'] if c in df.columns]
        df = df[keep_cols]

        price_history_mgr.save_stock_history(symbol, df)
        logger.info(f"{symbol}: {len(df)} days stock history via yfinance")
        return len(df)
    except Exception as e:
        logger.warning(f"{symbol}: yfinance failed: {e}")
        return 0


def collect_options_metrics_day(symbol, date_str, api, storage):
    """Collect options metrics for one symbol/date. Returns metrics dict or None."""
    # Spot price (recent only via ThetaData, fallback to None)
    df_spot = api.fetch('/stock/history/eod', {
        'symbol': symbol, 'start_date': date_str, 'end_date': date_str
    })

    if df_spot.empty:
        return None

    spot_price = float(df_spot['close'].iloc[0])

    metrics = {
        'symbol': symbol,
        'date': date_str,
        'timestamp': datetime.now().isoformat(),
        'spot_price': spot_price,
    }

    # Get OHLCV if available
    for col in ['open', 'high', 'low', 'volume']:
        if col in df_spot.columns:
            metrics[f'spot_{col}'] = float(df_spot[col].iloc[0]) if col != 'volume' else int(df_spot[col].iloc[0])

    # Get active expirations
    df_exp = api.fetch('/option/list/expirations', {'symbol': symbol})
    if df_exp.empty:
        return metrics

    expirations = pd.to_datetime(df_exp['expiration'])
    active = expirations[expirations >= pd.to_datetime(date_str)]
    exp_list = active.dt.strftime('%Y%m%d').tolist()

    if not exp_list:
        return metrics

    # Collect greeks + OI for all expirations
    all_greeks = []
    all_oi = []
    all_prices = []

    for exp in exp_list[:settings.MAX_EXPIRATIONS_FOR_GEX]:
        df_greeks = api.fetch('/option/history/greeks/eod', {
            'symbol': symbol, 'expiration': exp,
            'start_date': date_str, 'end_date': date_str
        })
        time.sleep(settings.RATE_LIMIT_DELAY)

        df_oi = api.fetch('/option/history/open_interest', {
            'symbol': symbol, 'expiration': exp, 'date': date_str
        })
        time.sleep(settings.RATE_LIMIT_DELAY)

        df_prices = api.fetch('/option/history/eod', {
            'symbol': symbol, 'expiration': exp,
            'start_date': date_str, 'end_date': date_str
        })
        time.sleep(settings.RATE_LIMIT_DELAY)

        for df in [df_greeks, df_oi, df_prices]:
            if not df.empty:
                df['expiration'] = pd.to_datetime(exp)
                df['date'] = pd.to_datetime(date_str)
                df['dte'] = (pd.to_datetime(exp) - pd.to_datetime(date_str)).days

        all_greeks.append(df_greeks)
        all_oi.append(df_oi)
        all_prices.append(df_prices)

    df_greeks = pd.concat(all_greeks, ignore_index=True) if any(not d.empty for d in all_greeks) else pd.DataFrame()
    df_oi = pd.concat(all_oi, ignore_index=True) if any(not d.empty for d in all_oi) else pd.DataFrame()
    df_prices = pd.concat(all_prices, ignore_index=True) if any(not d.empty for d in all_prices) else pd.DataFrame()

    metrics['expiration_count'] = len(exp_list)

    if not df_greeks.empty and not df_oi.empty:
        try:
            net_exp = calculate_net_exposures(df_greeks, df_oi, spot_price)
            metrics.update(net_exp)
        except Exception:
            pass

        try:
            skew = calculate_skew_metrics(df_greeks)
            metrics.update(skew)
        except Exception:
            pass

        try:
            ts = calculate_term_structure_metrics(df_greeks)
            metrics.update(ts)
        except Exception:
            pass

        try:
            pc = calculate_pc_ratios(df_prices, df_greeks, df_oi)
            metrics.update(pc)
        except Exception:
            pass

        try:
            df_gex = calculate_gex_distribution(df_greeks, df_oi, spot_price)
            if not df_gex.empty:
                call_walls = df_gex[df_gex['is_call_wall']]
                put_walls = df_gex[df_gex['is_put_wall']]
                if not call_walls.empty:
                    metrics['top_call_wall'] = float(call_walls.iloc[0]['strike'])
                if not put_walls.empty:
                    metrics['top_put_wall'] = float(put_walls.iloc[0]['strike'])
        except Exception:
            pass

        try:
            liq = analyze_liquidity(df_prices)
            for k, v in liq.items():
                metrics[f'liquidity_{k}'] = v
        except Exception:
            pass

    return metrics


def backfill_symbol(symbol, api, storage, price_history_mgr):
    """Full backfill for one symbol."""

    # Phase 1: Stock price history via yfinance (5 years, free)
    collect_stock_history_yfinance(symbol, '2021-01-01', '2026-01-31', price_history_mgr)

    # Phase 2: Options metrics - only recent dates where ThetaData has stock EOD
    # With FREE sub, stock EOD is ~1 year. Collect from 2025-01-01 to now.
    start = pd.to_datetime('2025-01-01')
    end = pd.to_datetime('2026-01-31')
    date_range = pd.bdate_range(start, end)

    success = 0
    failed = 0

    for i, date in enumerate(date_range, 1):
        date_str = date.strftime('%Y%m%d')

        try:
            metrics = collect_options_metrics_day(symbol, date_str, api, storage)
            if metrics and metrics.get('spot_price'):
                storage.save_daily_metrics(symbol, metrics)
                storage.update_state(symbol, date_str, 'success')
                success += 1
            else:
                failed += 1
            time.sleep(0.1)
        except Exception as e:
            failed += 1
            logger.debug(f"{symbol} {date_str}: {e}")
            time.sleep(0.3)

        if i % 50 == 0:
            logger.info(f"  {symbol}: {i}/{len(date_range)} ({success} OK)")

    logger.info(f"{symbol}: Done - {success} days collected, {failed} failed")
    return success


def main():
    log_section_header("MASSIVE BACKFILL S&P 500 - ADAPTED FOR FREE SUBSCRIPTION")
    logger.info("Phase 1: Stock OHLCV 5yr via yfinance (free)")
    logger.info("Phase 2: Options metrics ~1yr via ThetaData")

    api = ThetaDataAPI(settings.THETADATA_BASE_URL, settings.THETADATA_TIMEOUT)
    storage = StorageManager()
    history_path = settings.PATHS['derived_hv'] / '..' / 'price_history'
    price_history_mgr = PriceHistoryManager(history_path)

    progress = load_progress()
    completed = set(progress.get('completed_symbols', []))
    symbols = settings.SYMBOLS
    remaining = [s for s in symbols if s not in completed]

    logger.info(f"Total symbols: {len(symbols)}, Completed: {len(completed)}, Remaining: {len(remaining)}")

    for idx, symbol in enumerate(remaining, 1):
        logger.info(f"\n{'='*60}")
        logger.info(f"[{idx}/{len(remaining)}] {symbol}")
        logger.info(f"{'='*60}")

        progress['current_symbol'] = symbol
        save_progress(progress)

        try:
            success = backfill_symbol(symbol, api, storage, price_history_mgr)
            progress['completed_symbols'].append(symbol)
            progress['current_symbol'] = None
            save_progress(progress)
        except Exception as e:
            logger.error(f"CRITICAL {symbol}: {e}", exc_info=True)
            progress['failed'][symbol] = str(e)
            progress['completed_symbols'].append(symbol)  # Skip and continue
            save_progress(progress)
            time.sleep(2)

    log_section_header("BACKFILL COMPLETE")
    logger.info(f"Completed: {len(progress['completed_symbols'])}/{len(symbols)}")
    logger.info(f"Failed symbols: {len(progress.get('failed', {}))}")

    stats = storage.get_storage_stats()
    logger.info(f"Storage: {stats['total']['size_gb']:.2f} GB, {stats['total']['file_count']} files")


if __name__ == '__main__':
    main()
