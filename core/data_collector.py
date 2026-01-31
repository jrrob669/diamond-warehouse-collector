"""
Collecteurs de données spécialisés.
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

logger = logging.getLogger(__name__)

class OptionsDataCollector:
    """Collecteur principal pour options data."""
    
    def __init__(self):
        from config import settings
        self.api = ThetaDataAPI(settings.THETADATA_BASE_URL, settings.THETADATA_TIMEOUT)
        self.settings = settings
    
    def get_active_expirations(self, symbol: str, current_date: str) -> List[str]:
        """Récupère les expirations actives (>= date courante)."""
        df_exp = self.api.fetch('/option/list/expirations', {'symbol': symbol})
        
        if df_exp.empty:
            logger.warning(f"No expirations found for {symbol}")
            return []
        
        expirations = pd.to_datetime(df_exp['expiration'])
        active = expirations[expirations >= pd.to_datetime(current_date)]
        exp_list = active.dt.strftime('%Y%m%d').tolist()
        logger.info(f"Found {len(exp_list)} active expirations for {symbol}")
        return exp_list
    
    def collect_core_data(self, symbol: str, date: str, expiration: str) -> Dict[str, pd.DataFrame]:
        """Collecte les 3 core datasets pour une expiration."""
        logger.info(f"Collecting core data: {symbol} exp={expiration} date={date}")
        
        df_prices = self.api.fetch('/option/history/eod', {
            'symbol': symbol, 'expiration': expiration,
            'start_date': date, 'end_date': date
        })
        time.sleep(self.settings.RATE_LIMIT_DELAY)
        
        df_greeks = self.api.fetch('/option/history/greeks/eod', {
            'symbol': symbol, 'expiration': expiration,
            'start_date': date, 'end_date': date
        })
        time.sleep(self.settings.RATE_LIMIT_DELAY)
        
        df_oi = self.api.fetch('/option/history/open_interest', {
            'symbol': symbol, 'expiration': expiration, 'date': date
        })
        time.sleep(self.settings.RATE_LIMIT_DELAY)
        
        return {'prices': df_prices, 'greeks': df_greeks, 'oi': df_oi}
    
    def collect_all_expirations_data(self, symbol: str, date: str) -> Dict:
        """Collecte données pour TOUTES les expirations actives."""
        expirations = self.get_active_expirations(symbol, date)
        
        if not expirations:
            return {
                'all_prices': pd.DataFrame(), 'all_greeks': pd.DataFrame(),
                'all_oi': pd.DataFrame(), 'expiration_count': 0
            }
        
        all_prices = []
        all_greeks = []
        all_oi = []
        
        logger.info(f"Collecting data for {len(expirations)} expirations...")
        
        for i, exp in enumerate(expirations, 1):
            logger.info(f"Processing expiration {i}/{len(expirations)}: {exp}")
            data = self.collect_core_data(symbol, date, exp)
            
            for key in ['prices', 'greeks', 'oi']:
                if not data[key].empty:
                    data[key]['expiration'] = pd.to_datetime(exp)
                    data[key]['dte'] = (pd.to_datetime(exp) - pd.to_datetime(date)).days
            
            all_prices.append(data['prices'])
            all_greeks.append(data['greeks'])
            all_oi.append(data['oi'])
        
        df_prices = pd.concat(all_prices, ignore_index=True) if all_prices else pd.DataFrame()
        df_greeks = pd.concat(all_greeks, ignore_index=True) if all_greeks else pd.DataFrame()
        df_oi = pd.concat(all_oi, ignore_index=True) if all_oi else pd.DataFrame()
        
        logger.info(f"Collected {len(df_prices)} price rows, {len(df_greeks)} greeks rows, {len(df_oi)} OI rows")
        
        return {
            'all_prices': df_prices, 'all_greeks': df_greeks,
            'all_oi': df_oi, 'expiration_count': len(expirations)
        }
    
    def calculate_daily_metrics(self, symbol: str, date: str) -> tuple:
        """Calcule TOUTES les métriques pour une date."""
        logger.info(f"=== CALCULATING DAILY METRICS: {symbol} {date} ===")
        
        metrics = {'symbol': symbol, 'date': date, 'timestamp': datetime.now().isoformat()}
        
        df_spot = self.api.fetch('/stock/history/eod', {
            'symbol': symbol, 'start_date': date, 'end_date': date
        })
        
        if df_spot.empty:
            logger.error(f"No spot price for {symbol} on {date}")
            return metrics, {}
        
        spot_price = float(df_spot['close'].iloc[0])
        metrics['spot_price'] = spot_price
        logger.info(f"Spot price: ${spot_price:.2f}")
        
        core_data = self.collect_all_expirations_data(symbol, date)
        df_prices = core_data['all_prices']
        df_greeks = core_data['all_greeks']
        df_oi = core_data['all_oi']
        metrics['expiration_count'] = core_data['expiration_count']
        
        if df_greeks.empty or df_oi.empty:
            logger.warning("Insufficient data for calculations")
            return metrics, {}
        
        net_exposures = calculate_net_exposures(df_greeks, df_oi, spot_price)
        metrics.update(net_exposures)
        logger.info(f"Net Gamma: ${net_exposures['net_gamma']/1e9:.2f}B")
        
        skew_metrics = calculate_skew_metrics(df_greeks)
        metrics.update(skew_metrics)
        
        pc_ratios = calculate_pc_ratios(df_prices, df_greeks, df_oi)
        metrics.update(pc_ratios)
        
        df_gex = calculate_gex_distribution(df_greeks, df_oi, spot_price)
        if not df_gex.empty:
            call_walls = df_gex[df_gex['is_call_wall']]
            put_walls = df_gex[df_gex['is_put_wall']]
            if not call_walls.empty:
                metrics['top_call_wall'] = float(call_walls.iloc[0]['strike'])
            if not put_walls.empty:
                metrics['top_put_wall'] = float(put_walls.iloc[0]['strike'])
        
        liquidity = analyze_liquidity(df_prices)
        for k, v in liquidity.items():
            metrics[f'liquidity_{k}'] = v
        
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
        
        if 'iv_atm' in metrics and 'hv_20d' in metrics:
            if np.isfinite(metrics['iv_atm']) and np.isfinite(metrics['hv_20d']):
                metrics['iv_hv_spread'] = metrics['iv_atm'] - metrics['hv_20d']
        
        logger.info("=== DAILY METRICS COMPLETED ===")
        return metrics, {
            'gex_distribution': df_gex, 'core_prices': df_prices,
            'core_greeks': df_greeks, 'core_oi': df_oi
        }
