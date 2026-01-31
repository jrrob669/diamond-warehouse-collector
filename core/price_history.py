"""
Module de collecte et stockage d'historique des prix.
Gère prix stock et historique complet RR25 par DTE.
"""
import pandas as pd
import logging
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

class PriceHistoryManager:
    """Gestionnaire d'historique des prix."""
    
    def __init__(self, storage_path: Path):
        self.storage_path = storage_path
        self.storage_path.mkdir(parents=True, exist_ok=True)
    
    def save_stock_history(self, symbol: str, df_stock: pd.DataFrame):
        """
        Sauvegarde l'historique complet des prix stock.
        
        Args:
            symbol: Ticker (ex: SPY)
            df_stock: DataFrame avec colonnes [date, open, high, low, close, volume]
        """
        if df_stock.empty:
            return
        
        filepath = self.storage_path / f"{symbol}_stock_history.parquet"
        
        # Si existe, merger avec ancien
        if filepath.exists():
            df_existing = pd.read_parquet(filepath)
            df_combined = pd.concat([df_existing, df_stock], ignore_index=True)
            df_combined = df_combined.drop_duplicates(subset=['date'])
            df_combined = df_combined.sort_values('date')
            df_combined.to_parquet(filepath, engine='pyarrow', compression='snappy', index=False)
        else:
            df_stock.to_parquet(filepath, engine='pyarrow', compression='snappy', index=False)
        
        logger.info(f"Saved stock history: {len(df_stock)} rows to {filepath}")
    
    def load_stock_history(self, symbol: str, start_date: Optional[str] = None, 
                          end_date: Optional[str] = None) -> pd.DataFrame:
        """
        Charge l'historique des prix stock.
        
        Args:
            symbol: Ticker
            start_date: Date début (optionnel)
            end_date: Date fin (optionnel)
        
        Returns:
            DataFrame avec historique
        """
        filepath = self.storage_path / f"{symbol}_stock_history.parquet"
        
        if not filepath.exists():
            logger.warning(f"Stock history not found: {filepath}")
            return pd.DataFrame()
        
        df = pd.read_parquet(filepath)
        
        if start_date:
            df = df[df['date'] >= pd.to_datetime(start_date)]
        if end_date:
            df = df[df['date'] <= pd.to_datetime(end_date)]
        
        return df
    
    def save_term_structure_history(self, symbol: str, df_ts: pd.DataFrame):
        """
        Sauvegarde l'historique complet de la term structure.
        
        Args:
            symbol: Ticker
            df_ts: DataFrame avec colonnes term structure
                   [date, ts_0dte_rr25, ts_7dte_rr25, ts_30dte_rr25, ts_60dte_rr25, ...]
        """
        if df_ts.empty:
            return
        
        filepath = self.storage_path / f"{symbol}_term_structure_history.parquet"
        
        # Si existe, merger
        if filepath.exists():
            df_existing = pd.read_parquet(filepath)
            df_combined = pd.concat([df_existing, df_ts], ignore_index=True)
            df_combined = df_combined.drop_duplicates(subset=['date'])
            df_combined = df_combined.sort_values('date')
            df_combined.to_parquet(filepath, engine='pyarrow', compression='snappy', index=False)
        else:
            df_ts.to_parquet(filepath, engine='pyarrow', compression='snappy', index=False)
        
        logger.info(f"Saved term structure history: {len(df_ts)} rows to {filepath}")
    
    def load_term_structure_history(self, symbol: str, start_date: Optional[str] = None,
                                    end_date: Optional[str] = None) -> pd.DataFrame:
        """Charge l'historique term structure."""
        filepath = self.storage_path / f"{symbol}_term_structure_history.parquet"
        
        if not filepath.exists():
            logger.warning(f"Term structure history not found: {filepath}")
            return pd.DataFrame()
        
        df = pd.read_parquet(filepath)
        
        if start_date:
            df = df[df['date'] >= pd.to_datetime(start_date)]
        if end_date:
            df = df[df['date'] <= pd.to_datetime(end_date)]
        
        return df
    
    def get_complete_history(self, symbol: str) -> Dict[str, pd.DataFrame]:
        """
        Charge TOUT l'historique disponible.
        
        Returns:
            {
                'stock_prices': DataFrame,
                'term_structure': DataFrame,
                'summary': dict avec stats
            }
        """
        df_stock = self.load_stock_history(symbol)
        df_ts = self.load_term_structure_history(symbol)
        
        summary = {
            'stock_days': len(df_stock),
            'stock_start': df_stock['date'].min() if not df_stock.empty else None,
            'stock_end': df_stock['date'].max() if not df_stock.empty else None,
            'ts_days': len(df_ts),
            'ts_start': df_ts['date'].min() if not df_ts.empty else None,
            'ts_end': df_ts['date'].max() if not df_ts.empty else None
        }
        
        return {
            'stock_prices': df_stock,
            'term_structure': df_ts,
            'summary': summary
        }
    
    def export_to_csv(self, symbol: str, output_dir: Path):
        """Exporte tout en CSV pour analyse externe."""
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Stock prices
        df_stock = self.load_stock_history(symbol)
        if not df_stock.empty:
            csv_path = output_dir / f"{symbol}_stock_history.csv"
            df_stock.to_csv(csv_path, index=False)
            logger.info(f"Exported stock history to {csv_path}")
        
        # Term structure
        df_ts = self.load_term_structure_history(symbol)
        if not df_ts.empty:
            csv_path = output_dir / f"{symbol}_term_structure_history.csv"
            df_ts.to_csv(csv_path, index=False)
            logger.info(f"Exported term structure to {csv_path}")


def create_historical_summary(df_stock: pd.DataFrame, df_ts: pd.DataFrame) -> Dict:
    """
    Crée un résumé statistique de l'historique.
    
    Returns:
        Statistiques descriptives
    """
    summary = {}
    
    if not df_stock.empty:
        summary['stock'] = {
            'days': len(df_stock),
            'start': df_stock['date'].min(),
            'end': df_stock['date'].max(),
            'price_min': df_stock['close'].min(),
            'price_max': df_stock['close'].max(),
            'price_mean': df_stock['close'].mean(),
            'volume_avg': df_stock['volume'].mean()
        }
    
    if not df_ts.empty:
        # Stats RR25 par DTE
        for dte in [0, 7, 30, 60]:
            col = f'ts_{dte}dte_rr25'
            if col in df_ts.columns:
                summary[f'rr25_{dte}dte'] = {
                    'mean': df_ts[col].mean(),
                    'std': df_ts[col].std(),
                    'min': df_ts[col].min(),
                    'max': df_ts[col].max(),
                    'completeness': (1 - df_ts[col].isna().sum() / len(df_ts)) * 100
                }
    
    return summary
