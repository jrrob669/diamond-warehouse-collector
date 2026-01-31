"""
Analyse de la structure par terme (Term Structure).
Calcule RR25, IV ATM, et autres métriques par maturité (DTE).

VERSION COMPLÈTE avec :
- RR25 par DTE (0, 7, 30, 60)
- IV ATM par DTE
- Buckets DTE (0DTE, 1-7DTE, etc.)
- Surface IV complète
"""
import pandas as pd
import numpy as np
import logging
from typing import Dict, List
from scipy.interpolate import interp1d

logger = logging.getLogger(__name__)

def find_nearest_expiration(df_greeks: pd.DataFrame, target_dte: int, tolerance: int = 3) -> pd.DataFrame:
    """
    Trouve l'expiration la plus proche d'un DTE cible.
    
    Args:
        df_greeks: DataFrame avec colonnes 'dte', 'expiration'
        target_dte: DTE cible (0, 7, 30, 60)
        tolerance: Tolérance en jours (±3 jours par défaut)
    
    Returns:
        DataFrame filtré sur l'expiration la plus proche
    """
    if df_greeks.empty or 'dte' not in df_greeks.columns:
        return pd.DataFrame()
    
    # Trouver expirations dans la fenêtre de tolérance
    df_filtered = df_greeks[
        (df_greeks['dte'] >= target_dte - tolerance) &
        (df_greeks['dte'] <= target_dte + tolerance)
    ]
    
    if df_filtered.empty:
        return pd.DataFrame()
    
    # Prendre l'expiration la plus proche du target
    df_filtered = df_filtered.copy()
    df_filtered['dte_diff'] = (df_filtered['dte'] - target_dte).abs()
    
    # Grouper par expiration et prendre la plus proche
    closest_exp = df_filtered.loc[df_filtered['dte_diff'].idxmin(), 'expiration']
    
    return df_greeks[df_greeks['expiration'] == closest_exp]

def calculate_rr25_by_dte(df_greeks: pd.DataFrame, target_dtes: list = [0, 7, 30, 60]) -> dict:
    """
    Calcule RR25 (Risk Reversal 25-delta) pour chaque maturité spécifique.
    
    Args:
        df_greeks: DataFrame avec toutes les expirations
        target_dtes: Liste des DTEs cibles [0, 7, 30, 60]
    
    Returns:
        {
            'rr25_0dte': float,
            'rr25_7dte': float,
            'rr25_30dte': float,
            'rr25_60dte': float,
            'iv_atm_0dte': float,  # Bonus: IV ATM par DTE aussi
            'iv_atm_7dte': float,
            'iv_atm_30dte': float,
            'iv_atm_60dte': float,
            'actual_dte_0': int,   # DTE réel utilisé (peut différer de target)
            'actual_dte_7': int,
            'actual_dte_30': int,
            'actual_dte_60': int
        }
    """
    metrics = {}
    
    if df_greeks.empty:
        # Retourner NaN pour toutes les métriques
        for dte in target_dtes:
            metrics[f'rr25_{dte}dte'] = np.nan
            metrics[f'iv_atm_{dte}dte'] = np.nan
            metrics[f'actual_dte_{dte}'] = None
        return metrics
    
    # S'assurer que la colonne 'dte' existe
    if 'dte' not in df_greeks.columns:
        # Calculer DTE si manquant
        if 'expiration' in df_greeks.columns and 'date' in df_greeks.columns:
            df_greeks['dte'] = (pd.to_datetime(df_greeks['expiration']) - 
                               pd.to_datetime(df_greeks['date'])).dt.days
        else:
            # Impossible de calculer
            for dte in target_dtes:
                metrics[f'rr25_{dte}dte'] = np.nan
                metrics[f'iv_atm_{dte}dte'] = np.nan
                metrics[f'actual_dte_{dte}'] = None
            return metrics
    
    # Calculer pour chaque DTE cible
    for target_dte in target_dtes:
        # Tolérance adaptative
        if target_dte == 0:
            tolerance = 1  # ±1 jour pour 0DTE (strict)
        elif target_dte == 7:
            tolerance = 2  # ±2 jours pour 7DTE
        else:
            tolerance = 5  # ±5 jours pour 30DTE et 60DTE
        
        # Filtrer sur expiration proche
        df_dte = find_nearest_expiration(df_greeks, target_dte, tolerance)
        
        if df_dte.empty:
            metrics[f'rr25_{target_dte}dte'] = np.nan
            metrics[f'iv_atm_{target_dte}dte'] = np.nan
            metrics[f'actual_dte_{target_dte}'] = None
            continue
        
        # DTE réel utilisé
        actual_dte = int(df_dte['dte'].iloc[0])
        metrics[f'actual_dte_{target_dte}'] = actual_dte
        
        # Filtrer qualité
        df_clean = df_dte[
            (df_dte['iv_pct'] > 0) &
            (df_dte['delta'].notna()) &
            (df_dte['volume'] > 0) &
            (df_dte['bid'] > 0)
        ].copy()
        
        if df_clean.empty:
            metrics[f'rr25_{target_dte}dte'] = np.nan
            metrics[f'iv_atm_{target_dte}dte'] = np.nan
            continue
        
        # Calculer IV 25-delta pour Calls et Puts
        iv_25d_put = interpolate_iv_at_delta_local(df_clean, 0.25, 'PUT')
        iv_25d_call = interpolate_iv_at_delta_local(df_clean, 0.25, 'CALL')
        
        # RR25 = IV Put - IV Call
        if np.isfinite(iv_25d_put) and np.isfinite(iv_25d_call):
            metrics[f'rr25_{target_dte}dte'] = iv_25d_put - iv_25d_call
        else:
            metrics[f'rr25_{target_dte}dte'] = np.nan
        
        # Bonus: IV ATM pour cette maturité
        iv_atm_call = interpolate_iv_at_delta_local(df_clean, 0.50, 'CALL')
        iv_atm_put = interpolate_iv_at_delta_local(df_clean, 0.50, 'PUT')
        metrics[f'iv_atm_{target_dte}dte'] = np.nanmean([iv_atm_call, iv_atm_put])
    
    return metrics

def interpolate_iv_at_delta_local(df: pd.DataFrame, target_delta: float, option_type: str) -> float:
    """
    Version locale de interpolate_iv_at_delta (évite import circulaire).
    Interpole IV à un delta spécifique.
    """
    df_filtered = df[df['right'] == option_type].copy()
    
    if df_filtered.empty:
        return np.nan
    
    df_filtered['abs_delta'] = df_filtered['delta'].abs()
    df_filtered = df_filtered.sort_values('abs_delta')
    
    min_d = df_filtered['abs_delta'].min()
    max_d = df_filtered['abs_delta'].max()
    
    if min_d > target_delta or max_d < target_delta:
        return np.nan
    
    try:
        f = interp1d(
            df_filtered['abs_delta'],
            df_filtered['iv_pct'],
            kind='linear',
            bounds_error=True
        )
        return float(f(target_delta))
    except:
        return np.nan

def calculate_term_structure_metrics(df_greeks: pd.DataFrame) -> dict:
    """
    Calcule TOUTES les métriques de term structure en un seul appel.
    
    Returns:
        {
            # RR25 par maturité
            'rr25_0dte': float,
            'rr25_7dte': float,
            'rr25_30dte': float,
            'rr25_60dte': float,
            
            # IV ATM par maturité
            'iv_atm_0dte': float,
            'iv_atm_7dte': float,
            'iv_atm_30dte': float,
            'iv_atm_60dte': float,
            
            # DTE réels utilisés
            'actual_dte_0': int,
            'actual_dte_7': int,
            'actual_dte_30': int,
            'actual_dte_60': int,
            
            # Dérivées utiles
            'rr25_term_spread': float,  # RR25_60 - RR25_7 (pente du skew)
            'iv_term_spread': float     # IV_60 - IV_7 (pente de la term structure)
        }
    """
    # Calculer RR25 et IV par DTE
    metrics = calculate_rr25_by_dte(df_greeks, [0, 7, 30, 60])
    
    # Calculer spreads (dérivées de term structure)
    if (np.isfinite(metrics.get('rr25_60dte', np.nan)) and 
        np.isfinite(metrics.get('rr25_7dte', np.nan))):
        metrics['rr25_term_spread'] = metrics['rr25_60dte'] - metrics['rr25_7dte']
    else:
        metrics['rr25_term_spread'] = np.nan
    
    if (np.isfinite(metrics.get('iv_atm_60dte', np.nan)) and 
        np.isfinite(metrics.get('iv_atm_7dte', np.nan))):
        metrics['iv_term_spread'] = metrics['iv_atm_60dte'] - metrics['iv_atm_7dte']
    else:
        metrics['iv_term_spread'] = np.nan
    
    return metrics


# ============================================================================
# EXEMPLES D'UTILISATION
# ============================================================================

def example_usage():
    """
    Exemple d'intégration dans data_collector.py
    """
    from core.api_wrapper import ThetaDataAPI
    
    api = ThetaDataAPI("http://localhost:25503/v3", 15)
    
    # 1. Collecter Greeks pour toutes expirations
    df_greeks = api.fetch('/option/history/greeks/eod', {
        'symbol': 'SPY',
        'start_date': '20250127',
        'end_date': '20250127'
        # Pas de 'expiration' → retourne toutes les expirations
    })
    
    # Ajouter colonnes DTE si pas présentes
    if not df_greeks.empty and 'dte' not in df_greeks.columns:
        df_greeks['dte'] = (
            pd.to_datetime(df_greeks['expiration']) - 
            pd.to_datetime('20250127')
        ).dt.days
    
    # 2. Calculer métriques term structure
    term_metrics = calculate_term_structure_metrics(df_greeks)
    
    # 3. Afficher résultats
    print("\n📊 Term Structure Skew Metrics:")
    print("="*60)
    for key, value in term_metrics.items():
        if 'rr25' in key or 'iv_atm' in key:
            if np.isfinite(value):
                print(f"  {key:.<40} {value:.2f}%")
            else:
                print(f"  {key:.<40} N/A")
        elif 'actual_dte' in key:
            print(f"  {key:.<40} {value} days")
    
    return term_metrics


if __name__ == "__main__":
    # Test standalone
    example_usage()
