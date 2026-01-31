"""
Gestionnaire de stockage Parquet optimisé.
"""
import pandas as pd
import logging
from pathlib import Path
from typing import Dict, Optional
import json
from datetime import datetime

logger = logging.getLogger(__name__)

class StorageManager:
    """Gestionnaire centralisé du stockage warehouse."""
    
    def __init__(self):
        from config import settings
        self.paths = settings.PATHS
        self.state_file = settings.STATE_FILE
    
    def save_core_data(self, symbol: str, date: str, expiration: str, 
                       df_prices: pd.DataFrame, df_greeks: pd.DataFrame, df_oi: pd.DataFrame):
        """Sauvegarde les données core avec partitionnement par date."""
        date_obj = pd.to_datetime(date)
        year = date_obj.strftime('%Y')
        month = date_obj.strftime('%m')
        exp_clean = expiration.replace('-', '')
        
        if not df_prices.empty:
            path = self.paths['core_prices'] / symbol / year / month
            path.mkdir(parents=True, exist_ok=True)
            filepath = path / f"exp_{exp_clean}.parquet"
            df_prices.to_parquet(filepath, engine='pyarrow', compression='snappy', index=False)
            logger.debug(f"Saved prices: {filepath}")
        
        if not df_greeks.empty:
            path = self.paths['core_greeks'] / symbol / year / month
            path.mkdir(parents=True, exist_ok=True)
            filepath = path / f"exp_{exp_clean}.parquet"
            df_greeks.to_parquet(filepath, engine='pyarrow', compression='snappy', index=False)
            logger.debug(f"Saved greeks: {filepath}")
        
        if not df_oi.empty:
            path = self.paths['core_oi'] / symbol / year / month
            path.mkdir(parents=True, exist_ok=True)
            filepath = path / f"exp_{exp_clean}.parquet"
            df_oi.to_parquet(filepath, engine='pyarrow', compression='snappy', index=False)
            logger.debug(f"Saved OI: {filepath}")
    
    def save_daily_metrics(self, symbol: str, metrics: Dict):
        """Sauvegarde les métriques agrégées quotidiennes."""
        df_metrics = pd.DataFrame([metrics])
        filepath = self.paths['aggregated'] / f"{symbol}_master_daily.parquet"
        
        if filepath.exists():
            df_existing = pd.read_parquet(filepath)
            df_existing = df_existing[df_existing['date'] != metrics['date']]
            df_combined = pd.concat([df_existing, df_metrics], ignore_index=True)
            df_combined = df_combined.sort_values('date')
            df_combined.to_parquet(filepath, engine='pyarrow', compression='snappy', index=False)
        else:
            df_metrics.to_parquet(filepath, engine='pyarrow', compression='snappy', index=False)
        
        logger.info(f"Saved daily metrics: {filepath}")
    
    def save_gex_distribution(self, symbol: str, date: str, df_gex: pd.DataFrame):
        """Sauvegarde la distribution GEX par strike."""
        if df_gex.empty:
            return
        
        df_gex['date'] = pd.to_datetime(date)
        date_obj = pd.to_datetime(date)
        year = date_obj.strftime('%Y')
        path = self.paths['derived_gex'] / symbol / year
        path.mkdir(parents=True, exist_ok=True)
        filepath = path / f"gex_{date.replace('-', '')}.parquet"
        df_gex.to_parquet(filepath, engine='pyarrow', compression='snappy', index=False)
        logger.debug(f"Saved GEX distribution: {filepath}")
    
    def load_master_daily(self, symbol: str, start_date: Optional[str] = None, 
                          end_date: Optional[str] = None) -> pd.DataFrame:
        """Charge les métriques quotidiennes avec filtrage optionnel."""
        filepath = self.paths['aggregated'] / f"{symbol}_master_daily.parquet"
        
        if not filepath.exists():
            logger.warning(f"Master daily file not found: {filepath}")
            return pd.DataFrame()
        
        df = pd.read_parquet(filepath)
        if start_date:
            df = df[df['date'] >= pd.to_datetime(start_date)]
        if end_date:
            df = df[df['date'] <= pd.to_datetime(end_date)]
        return df
    
    def get_last_collected_date(self, symbol: str) -> Optional[str]:
        """Récupère la dernière date collectée depuis le state file."""
        if not self.state_file.exists():
            return None
        try:
            with open(self.state_file, 'r') as f:
                state = json.load(f)
            return state.get(symbol, {}).get('last_date')
        except:
            return None
    
    def update_state(self, symbol: str, date: str, status: str = 'success'):
        """Met à jour le state file avec la dernière collecte."""
        state = {}
        if self.state_file.exists():
            try:
                with open(self.state_file, 'r') as f:
                    state = json.load(f)
            except:
                pass
        
        if symbol not in state:
            state[symbol] = {}
        
        state[symbol]['last_date'] = date
        state[symbol]['last_update'] = datetime.now().isoformat()
        state[symbol]['status'] = status
        
        with open(self.state_file, 'w') as f:
            json.dump(state, f, indent=2)
        
        logger.debug(f"Updated state: {symbol} -> {date} ({status})")
    
    def get_storage_stats(self) -> Dict:
        """Calcule statistiques d'utilisation du storage."""
        stats = {}
        for name, path in self.paths.items():
            if path.exists():
                total_size = sum(f.stat().st_size for f in path.rglob('*') if f.is_file())
                stats[name] = {
                    'size_mb': total_size / (1024 * 1024),
                    'file_count': len(list(path.rglob('*.parquet')))
                }
        
        total_mb = sum(s['size_mb'] for s in stats.values())
        stats['total'] = {
            'size_mb': total_mb,
            'size_gb': total_mb / 1024,
            'file_count': sum(s['file_count'] for s in stats.values())
        }
        return stats
