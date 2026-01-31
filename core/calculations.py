"""
Moteurs de calcul pour métriques dérivées - Version Diamond 10.2
Optimisée avec normalisation systématique, conversion de dates
et Zero-Crash Policy pour les Index et Volumes manquants.
"""
import pandas as pd
import numpy as np
from scipy.interpolate import interp1d
import logging

logger = logging.getLogger(__name__)

# ==============================================================================
# 🛠️ UTILITAIRES DE ROBUSTESSE
# ==============================================================================

def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalise les noms de colonnes et applique corrections empiriques ThetaData.
    
    Gère :
    - Nettoyage (minuscules, strip)
    - Alias courants (vol→volume, oi→open_interest)
    - Correction Empirique #1 : Normalisation 'right' (P/C → PUT/CALL)
    - Correction Empirique #2 : IV en pourcentage (implied_vol * 100)
    - Correction Empirique #3 : Millistrikes (strike / 1000)
    - Conversion Date : Force le format datetime
    """
    if df.empty:
        return df
    
    df = df.copy()
    
    # 1. Nettoyage basique
    df.columns = df.columns.str.strip().str.lower()
    
    # 2. Mapping des alias courants
    mappings = {
        'vol': 'volume',
        'size': 'volume',
        'daily_volume': 'volume',
        'oi': 'open_interest',
        'openinterest': 'open_interest',
        'type': 'right',
        'cp': 'right',
        'exp': 'expiration',
        'expiry': 'expiration',
        'date': 'date'  # Force lowercase
    }
    df = df.rename(columns=mappings)
    
    # 3. CORRECTION EMPIRIQUE #1 : Normalisation 'right'
    if 'right' in df.columns:
        df['right'] = df['right'].astype(str).str.upper().str.strip()
        # Standardisation P/C → PUT/CALL
        df['right'] = df['right'].replace({'P': 'PUT', 'C': 'CALL'})
    
    # 4. CORRECTION EMPIRIQUE #2 : IV en pourcentage
    if 'implied_vol' in df.columns and 'iv_pct' not in df.columns:
        df['iv_pct'] = df['implied_vol'] * 100
    
    # 5. CORRECTION EMPIRIQUE #3 : Millistrikes
    if 'strike' in df.columns and not df.empty:
        max_strike = df['strike'].max()
        if pd.notna(max_strike) and max_strike > 10000:
            df['strike'] = df['strike'] / 1000
            
    # 6. CONVERSION DATES (Votre ajout blindé)
    if 'date' in df.columns:
        # On force en string d'abord pour gérer le format int YYYYMMDD de ThetaData
        try:
            df['date'] = pd.to_datetime(df['date'].astype(str), errors='coerce')
        except Exception:
            # Fallback simple
            df['date'] = pd.to_datetime(df['date'], errors='coerce')

    return df

def safe_column_sum(df: pd.DataFrame, col: str, default: float = 0.0) -> float:
    """Somme sécurisée d'une colonne (retourne default si colonne absente)."""
    if col in df.columns:
        return df[col].sum()
    return default

# ==============================================================================
# 🧠 FONCTIONS CŒUR
# ==============================================================================

def calculate_net_exposures(df_greeks: pd.DataFrame, df_oi: pd.DataFrame, 
                            spot_price: float) -> dict:
    """
    Calcule les expositions nettes Delta et Gamma (market-wide).
    """
    df_greeks = normalize_columns(df_greeks)
    df_oi = normalize_columns(df_oi)
    
    required_cols = ['strike', 'right']
    if not all(col in df_greeks.columns for col in required_cols) or \
       not all(col in df_oi.columns for col in required_cols):
        logger.warning("calculate_net_exposures: Colonnes clés manquantes")
        return {}

    # Fusion
    df = df_greeks.merge(df_oi, on=['strike', 'right'], how='inner')
    
    # Métriques par défaut
    zero_metrics = {
        'net_delta': 0.0, 'net_gamma': 0.0,
        'net_delta_calls': 0.0, 'net_delta_puts': 0.0,
        'net_gamma_calls': 0.0, 'net_gamma_puts': 0.0,
        'total_oi': 0, 'call_oi': 0, 'put_oi': 0,
        'net_gamma_billions': 0.0, 'net_delta_millions': 0.0
    }
    
    if df.empty:
        return zero_metrics
    
    for col in ['delta', 'gamma', 'open_interest']:
        if col not in df.columns:
            return zero_metrics

    df = df[(df['delta'].notna()) & (df['gamma'].notna()) & (df['open_interest'] > 0)]
    
    calls = df[df['right'] == 'CALL'].copy()
    puts = df[df['right'] == 'PUT'].copy()
    
    # Delta Exposure
    calls['delta_exposure'] = calls['delta'] * calls['open_interest'] * 100
    puts['delta_exposure'] = puts['delta'] * puts['open_interest'] * 100
    
    net_delta_calls = calls['delta_exposure'].sum()
    net_delta_puts = puts['delta_exposure'].sum()
    net_delta = net_delta_calls + net_delta_puts
    
    # Gamma Exposure
    calls['gamma_exposure'] = calls['gamma'] * calls['open_interest'] * 100 * spot_price
    puts['gamma_exposure'] = puts['gamma'] * puts['open_interest'] * 100 * spot_price
    
    net_gamma_calls = calls['gamma_exposure'].sum()
    net_gamma_puts = puts['gamma_exposure'].sum()
    net_gamma = net_gamma_calls - net_gamma_puts
    
    return {
        'net_delta': net_delta,
        'net_gamma': net_gamma,
        'net_delta_calls': net_delta_calls,
        'net_delta_puts': net_delta_puts,
        'net_gamma_calls': net_gamma_calls,
        'net_gamma_puts': net_gamma_puts,
        'total_oi': int(df['open_interest'].sum()),
        'call_oi': int(calls['open_interest'].sum()),
        'put_oi': int(puts['open_interest'].sum()),
        'net_gamma_billions': net_gamma / 1e9,
        'net_delta_millions': net_delta / 1e6
    }

def calculate_realized_volatility(df_stock: pd.DataFrame, 
                                  windows: list = [10, 20, 30, 60, 252]) -> pd.DataFrame:
    """
    Calcule la volatilité réalisée sur plusieurs fenêtres.
    BLINDÉE contre KeyError: 'date'.
    """
    df_stock = normalize_columns(df_stock)
    
    if df_stock.empty or 'close' not in df_stock.columns:
        logger.warning("calculate_realized_volatility: Colonne 'close' manquante")
        return pd.DataFrame()
    
    df = df_stock.copy()

    # Gestion Index et Date
    if 'date' not in df.columns:
        if isinstance(df.index, pd.DatetimeIndex) or df.index.name in ['date', 'Date', 'DATE']:
            df = df.reset_index()
            if 'index' in df.columns:
                df = df.rename(columns={'index': 'date'})
                # On s'assure que la nouvelle colonne date est bien au format datetime
                df['date'] = pd.to_datetime(df['date'], errors='coerce')
    
    # Tri sécurisé
    if 'date' in df.columns:
        df = df.sort_values('date')
    else:
        df = df.sort_index()

    # Calculs Rendements
    df['log_return'] = np.log(df['close'] / df['close'].shift(1))
    
    for window in windows:
        df[f'hv_{window}d'] = (
            df['log_return'].rolling(window=window).std() * np.sqrt(252) * 100
        )
    
    # Sélection colonnes
    cols_to_keep = [c for c in ['date', 'close'] if c in df.columns] + \
                   [f'hv_{w}d' for w in windows]
                   
    return df[cols_to_keep].dropna()

def interpolate_iv_at_delta(df: pd.DataFrame, target_delta: float, 
                            option_type: str = 'PUT') -> float:
    """
    Interpole IV à un delta spécifique.
    """
    df = normalize_columns(df)
    
    required = ['right', 'iv_pct', 'delta']
    if not all(c in df.columns for c in required):
        return np.nan

    target_type = 'PUT' if option_type in ['PUT', 'P'] else 'CALL'
    
    df_filtered = df[
        (df['right'] == target_type) &
        (df['iv_pct'] > 0) &
        (df['delta'].notna())
    ].copy()
    
    if 'volume' in df_filtered.columns:
        df_filtered = df_filtered[df_filtered['volume'] > 0]
    
    if df_filtered.empty:
        return np.nan
    
    df_filtered['abs_delta'] = df_filtered['delta'].abs()
    df_filtered = df_filtered.sort_values('abs_delta')
    
    min_d = df_filtered['abs_delta'].min()
    max_d = df_filtered['abs_delta'].max()
    
    if min_d > target_delta or max_d < target_delta:
        return np.nan
    
    try:
        f = interp1d(df_filtered['abs_delta'], df_filtered['iv_pct'], 
                     kind='linear', bounds_error=True)
        return float(f(target_delta))
    except Exception:
        return np.nan

def calculate_skew_metrics(df_greeks: pd.DataFrame) -> dict:
    """
    Calcule métriques de skew (RR25, ATM IV, etc.).
    """
    df_greeks = normalize_columns(df_greeks)
    
    metrics = {
        'iv_atm': np.nan, 'iv_25d_put': np.nan, 'iv_25d_call': np.nan,
        'rr25': np.nan, 'iv_10d_put': np.nan, 'bf25': np.nan
    }
    
    if df_greeks.empty:
        return metrics
    
    iv_atm_call = interpolate_iv_at_delta(df_greeks, 0.50, 'CALL')
    iv_atm_put = interpolate_iv_at_delta(df_greeks, 0.50, 'PUT')
    metrics['iv_atm'] = np.nanmean([iv_atm_call, iv_atm_put])
    
    metrics['iv_25d_put'] = interpolate_iv_at_delta(df_greeks, 0.25, 'PUT')
    metrics['iv_25d_call'] = interpolate_iv_at_delta(df_greeks, 0.25, 'CALL')
    
    if np.isfinite(metrics['iv_25d_put']) and np.isfinite(metrics['iv_25d_call']):
        metrics['rr25'] = metrics['iv_25d_put'] - metrics['iv_25d_call']
    
    metrics['iv_10d_put'] = interpolate_iv_at_delta(df_greeks, 0.10, 'PUT')
    
    if all(np.isfinite([metrics['iv_25d_put'], metrics['iv_atm'], metrics['iv_25d_call']])):
        metrics['bf25'] = (metrics['iv_25d_put'] + metrics['iv_25d_call']) / 2 - metrics['iv_atm']
    
    return metrics

def calculate_pc_ratios(df_price: pd.DataFrame, df_greeks: pd.DataFrame, 
                        df_oi: pd.DataFrame) -> dict:
    """
    Calcule ratios Put/Call (volume, OI, premium, delta).
    """
    df_price = normalize_columns(df_price)
    df_greeks = normalize_columns(df_greeks)
    df_oi = normalize_columns(df_oi)

    keys = ['strike', 'right']
    
    if df_price.empty:
         return {'pc_volume': 0, 'pc_oi': 0, 'sentiment': 'NEUTRAL'}

    df = df_price.merge(df_greeks, on=keys, how='left', suffixes=('', '_greek'))
    
    if not df_oi.empty and all(k in df_oi.columns for k in keys):
        df = df.merge(df_oi.drop(columns=['date'], errors='ignore'), on=keys, how='left')
    
    if df.empty:
        return {}

    puts = df[df['right'] == 'PUT'].copy()
    calls = df[df['right'] == 'CALL'].copy()

    vol_p = safe_column_sum(puts, 'volume')
    vol_c = safe_column_sum(calls, 'volume')
    oi_p = safe_column_sum(puts, 'open_interest')
    oi_c = safe_column_sum(calls, 'open_interest')
    
    ratios = {
        'pc_volume': vol_p / max(vol_c, 1),
        'pc_oi': oi_p / max(oi_c, 1),
    }
    
    if 'close' in puts.columns and 'volume' in puts.columns:
        prem_p = (puts['close'] * puts['volume'] * 100).sum()
        prem_c = (calls['close'] * calls['volume'] * 100).sum()
        ratios['pc_premium'] = prem_p / max(prem_c, 1)
        
    if 'delta' in puts.columns and 'open_interest' in puts.columns:
        delta_p = (puts['delta'].abs() * puts['open_interest']).sum()
        delta_c = (calls['delta'].abs() * calls['open_interest']).sum()
        ratios['pc_delta'] = delta_p / max(delta_c, 1)

    pc_vol = ratios.get('pc_volume', 0)
    if pc_vol > 1.5:
        ratios['sentiment'] = 'BEARISH'
    elif pc_vol < 0.7:
        ratios['sentiment'] = 'BULLISH'
    else:
        ratios['sentiment'] = 'NEUTRAL'
    
    return ratios

def calculate_gex_distribution(df_greeks: pd.DataFrame, df_oi: pd.DataFrame, 
                               spot_price: float) -> pd.DataFrame:
    """
    Calcule distribution GEX par strike.
    """
    df_greeks = normalize_columns(df_greeks)
    df_oi = normalize_columns(df_oi)
    
    df = df_greeks.merge(df_oi, on=['strike', 'right'], how='inner')
    
    if df.empty or 'gamma' not in df.columns or 'open_interest' not in df.columns:
        return pd.DataFrame()
    
    df['gex'] = df['gamma'] * df['open_interest'] * 100 * spot_price
    df.loc[df['right'] == 'PUT', 'gex'] *= -1
    
    gex_by_strike = df.groupby('strike').agg({
        'gex': 'sum', 
        'open_interest': 'sum'
    }).reset_index()
    
    gex_by_strike['spot_price'] = spot_price
    gex_by_strike['distance_to_spot'] = gex_by_strike['strike'] - spot_price
    gex_by_strike['gex_billions'] = gex_by_strike['gex'] / 1e9
    
    gex_by_strike['is_call_wall'] = False
    gex_by_strike['is_put_wall'] = False
    
    if not gex_by_strike.empty:
        max_call_idx = gex_by_strike['gex'].idxmax()
        if gex_by_strike.loc[max_call_idx, 'gex'] > 0:
            gex_by_strike.loc[max_call_idx, 'is_call_wall'] = True
            
        min_put_idx = gex_by_strike['gex'].idxmin()
        if gex_by_strike.loc[min_put_idx, 'gex'] < 0:
            gex_by_strike.loc[min_put_idx, 'is_put_wall'] = True
    
    return gex_by_strike.sort_values('strike')

def analyze_liquidity(df_price: pd.DataFrame) -> dict:
    """
    Analyse santé du marché via spreads et volume.
    """
    df = normalize_columns(df_price)
    
    if df.empty:
        return {}
    
    if 'ask' in df.columns and 'bid' in df.columns and 'close' in df.columns:
        df['spread_abs'] = df['ask'] - df['bid']
        df['spread_pct'] = np.where(df['close'] > 0, 
                                    (df['spread_abs'] / df['close']) * 100, 0)
        df = df[df['spread_pct'] < 100]
    else:
        df['spread_pct'] = 0.0

    if 'volume' not in df.columns:
        df['volume'] = 0
        
    metrics = {
        'avg_spread_pct': df['spread_pct'].mean() if not df.empty else 0,
        'median_spread_pct': df['spread_pct'].median() if not df.empty else 0,
        'max_spread_pct': df['spread_pct'].max() if not df.empty else 0,
        'total_volume': df['volume'].sum(),
        'avg_volume': df['volume'].mean() if not df.empty else 0,
        'illiquid_contracts': (df['spread_pct'] > 10).sum(),
        'zero_volume_contracts': (df['volume'] == 0).sum()
    }
    
    if len(df) > 0:
        stress_score = min(
            (metrics['avg_spread_pct'] / 5 * 50) + 
            (metrics['illiquid_contracts'] / len(df) * 50), 
            100
        )
    else:
        stress_score = 0
        
    metrics['liquidity_stress_index'] = stress_score
    
    return metrics