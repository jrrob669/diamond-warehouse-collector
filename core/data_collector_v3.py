"""
Collecteur de données VERSION 3.
Avec historique complet prix + term structure RR25 par DTE.
"""
import pandas as pd
import logging
from typing import List, Dict
import time
from datetime import datetime, timedelta

from core.api_wrapper import ThetaDataAPI
from core.calculations import (
    calculate_net_exposures, calculate_realized_volatility,
    calculate_skew_metrics, calculate_pc_ratios,
    calculate_gex_distribution, analyze_liquidity
)
from core.term_structure import calculate_term_structure_metrics as calculate_full_term_structure
from core.price_history import PriceHistoryManager

logger = logging.getLogger(__name__)

class OptionsDataCollectorV3:
    """
    Collecteur VERSION 3 avec :
    - Historique prix stock complet
    - RR25 par DTE (0, 7, 30, 60)
    - Surface IV complète
    """
    
    def __init__(self):
        from config import settings
        self.api = ThetaDataAPI(settings.THETADATA_BASE_URL, settings.THETADATA_TIMEOUT)
        self.settings = settings
        
        # Gestionnaire historique
        history_path = settings.PATHS['derived_hv'] / '..' / 'price_history'
        self.price_history = PriceHistoryManager(history_path)
    
    def collect_stock_price_history(self, symbol: str, start_date: str, end_date: str):
        """
        Collecte historique complet des prix stock.
        
        Args:
            symbol: Ticker
            start_date: Date début (YYYYMMDD)
            end_date: Date fin (YYYYMMDD)
        """
        logger.info(f"Collecting stock history: {symbol} from {start_date} to {end_date}")
        
        df_stock = self.api.fetch('/stock/history/eod', {
            'symbol': symbol,
            'start_date': start_date,
            'end_date': end_date
        })
        
        if df_stock.empty:
            logger.warning(f"No stock history retrieved for {symbol}")
            return pd.DataFrame()
        
        # Sauvegarder
        self.price_history.save_stock_history(symbol, df_stock)
        
        logger.info(f"Collected {len(df_stock)} days of stock history")
        return df_stock
    
    def get_active_expirations(self, symbol: str, current_date: str) -> List[str]:
        """Récupère les expirations actives."""
        df_exp = self.api.fetch('/option/list/expirations', {'symbol': symbol})
        
        if df_exp.empty:
            logger.warning(f"No expirations found for {symbol}")
            return []
        
        expirations = pd.to_datetime(df_exp['expiration'])
        active = expirations[expirations >= pd.to_datetime(current_date)]
        exp_list = active.dt.strftime('%Y%m%d').tolist()
        logger.info(f"Found {len(exp_list)} active expirations for {symbol}")
        return exp_list
    
    def collect_all_expirations_data(self, symbol: str, date: str) -> Dict:
        """Collecte données pour TOUTES les expirations actives."""
        expirations = self.get_active_expirations(symbol, date)
        
        if not expirations:
            return {
                'all_prices': pd.DataFrame(), 
                'all_greeks': pd.DataFrame(),
                'all_oi': pd.DataFrame(), 
                'expiration_count': 0
            }
        
        all_prices = []
        all_greeks = []
        all_oi = []
        
        logger.info(f"Collecting data for {len(expirations)} expirations...")
        
        for i, exp in enumerate(expirations, 1):
            logger.debug(f"Processing expiration {i}/{len(expirations)}: {exp}")
            
            # Prices
            df_prices = self.api.fetch('/option/history/eod', {
                'symbol': symbol, 'expiration': exp,
                'start_date': date, 'end_date': date
            })
            time.sleep(self.settings.RATE_LIMIT_DELAY)
            
            # Greeks
            df_greeks = self.api.fetch('/option/history/greeks/eod', {
                'symbol': symbol, 'expiration': exp,
                'start_date': date, 'end_date': date
            })
            time.sleep(self.settings.RATE_LIMIT_DELAY)
            
            # OI
            df_oi = self.api.fetch('/option/history/open_interest', {
                'symbol': symbol, 'expiration': exp, 'date': date
            })
            time.sleep(self.settings.RATE_LIMIT_DELAY)
            
            # Ajouter DTE
            for df in [df_prices, df_greeks, df_oi]:
                if not df.empty:
                    df['expiration'] = pd.to_datetime(exp)
                    df['date'] = pd.to_datetime(date)
                    df['dte'] = (pd.to_datetime(exp) - pd.to_datetime(date)).days
            
            all_prices.append(df_prices)
            all_greeks.append(df_greeks)
            all_oi.append(df_oi)
        
        df_prices = pd.concat(all_prices, ignore_index=True) if all_prices else pd.DataFrame()
        df_greeks = pd.concat(all_greeks, ignore_index=True) if all_greeks else pd.DataFrame()
        df_oi = pd.concat(all_oi, ignore_index=True) if all_oi else pd.DataFrame()
        
        logger.info(f"Collected {len(df_prices)} price rows, {len(df_greeks)} greeks rows, {len(df_oi)} OI rows")
        
        return {
            'all_prices': df_prices, 
            'all_greeks': df_greeks,
            'all_oi': df_oi, 
            'expiration_count': len(expirations)
        }
    
    def calculate_daily_metrics(self, symbol: str, date: str) -> tuple:
        """
        Calcule métriques complètes pour une date.
        VERSION 3 avec term structure RR25 par DTE.
        """
        logger.info(f"=== CALCULATING DAILY METRICS V3: {symbol} {date} ===")
        
        metrics = {
            'symbol': symbol, 
            'date': date, 
            'timestamp': datetime.now().isoformat()
        }
        
        # 1. Spot price
        df_spot = self.api.fetch('/stock/history/eod', {
            'symbol': symbol, 'start_date': date, 'end_date': date
        })
        
        if df_spot.empty:
            logger.error(f"No spot price for {symbol} on {date}")
            return metrics, {}
        
        spot_price = float(df_spot['close'].iloc[0])
        metrics['spot_price'] = spot_price
        metrics['spot_open'] = float(df_spot['open'].iloc[0])
        metrics['spot_high'] = float(df_spot['high'].iloc[0])
        metrics['spot_low'] = float(df_spot['low'].iloc[0])
        metrics['spot_volume'] = int(df_spot['volume'].iloc[0])
        
        logger.info(f"Spot price: ${spot_price:.2f}")
        
        # Sauvegarder prix dans historique
        self.price_history.save_stock_history(symbol, df_spot)
        
        # 2. Core data (toutes expirations)
        core_data = self.collect_all_expirations_data(symbol, date)
        df_prices = core_data['all_prices']
        df_greeks = core_data['all_greeks']
        df_oi = core_data['all_oi']
        metrics['expiration_count'] = core_data['expiration_count']
        
        if df_greeks.empty or df_oi.empty:
            logger.warning("Insufficient data for calculations")
            return metrics, {}
        
        # 3. Net Delta & Gamma
        net_exposures = calculate_net_exposures(df_greeks, df_oi, spot_price)
        metrics.update(net_exposures)
        logger.info(f"Net Gamma: ${net_exposures['net_gamma']/1e9:.2f}B")
        
        # 4. Skew global (toutes expirations mélangées)
        skew_metrics = calculate_skew_metrics(df_greeks)
        metrics.update(skew_metrics)
        
        # 5. ⭐ NOUVEAU : Term Structure (RR25 par DTE)
        logger.info("Calculating term structure (RR25 by DTE)...")
        term_structure = calculate_full_term_structure(df_greeks)
        metrics.update(term_structure)
        
        # Log term structure
        for dte in [0, 7, 30, 60]:
            rr25_key = f'ts_{dte}dte_rr25'
            if rr25_key in term_structure:
                logger.info(f"RR25 {dte}DTE: {term_structure.get(rr25_key, 'N/A')}")
        
        # Sauvegarder term structure dans historique
        ts_row = {'date': pd.to_datetime(date)}
        ts_row.update(term_structure)
        df_ts = pd.DataFrame([ts_row])
        self.price_history.save_term_structure_history(symbol, df_ts)
        
        # 6. Put/Call Ratios
        pc_ratios = calculate_pc_ratios(df_prices, df_greeks, df_oi)
        metrics.update(pc_ratios)
        
        # 7. GEX Distribution
        df_gex = calculate_gex_distribution(df_greeks, df_oi, spot_price)
        if not df_gex.empty:
            call_walls = df_gex[df_gex['is_call_wall']]
            put_walls = df_gex[df_gex['is_put_wall']]
            if not call_walls.empty:
                metrics['top_call_wall'] = float(call_walls.iloc[0]['strike'])
            if not put_walls.empty:
                metrics['top_put_wall'] = float(put_walls.iloc[0]['strike'])
        
        # 8. Liquidity
        liquidity = analyze_liquidity(df_prices)
        for k, v in liquidity.items():
            metrics[f'liquidity_{k}'] = v
        
        # 9. Realized Volatility
        start_date_hv = (pd.to_datetime(date) - timedelta(days=300)).strftime('%Y%m%d')
        df_stock_hist = self.api.fetch('/stock/history/eod', {
            'symbol': symbol, 'start_date': start_date_hv, 'end_date': date
        })
        
        if not df_stock_hist.empty:
            df_hv = calculate_realized_volatility(df_stock_hist, self.settings.HV_WINDOWS)
            if not df_hv.empty:
                last_hv = df_hv.iloc[-1]
                for window in self.settings.HV_WINDOWS:
                    col = f'hv_{window}d'
                    if col in last_hv:
                        metrics[col] = float(last_hv[col])
        
        # 10. IV-HV Spread
        if 'iv_atm' in metrics and 'hv_20d' in metrics:
            if pd.notna(metrics['iv_atm']) and pd.notna(metrics['hv_20d']):
                metrics['iv_hv_spread'] = metrics['iv_atm'] - metrics['hv_20d']
        
        logger.info("=== DAILY METRICS V3 COMPLETED ===")
        
        return metrics, {
            'gex_distribution': df_gex, 
            'core_prices': df_prices,
            'core_greeks': df_greeks, 
            'core_oi': df_oi
        }
