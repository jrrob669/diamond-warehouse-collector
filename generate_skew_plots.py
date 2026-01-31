"""
Script pratique pour visualiser le skew de volatilité.
Usage: python generate_skew_plots.py
"""
import sys
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, '.')

try:
    from config import settings_test as settings
except:
    from config import settings

from core.api_wrapper import ThetaDataAPI
from storage.manager import StorageManager
from visualization.volatility_skew import VolatilitySkewVisualizer

def collect_fresh_data(symbol: str, date: str = None):
    """Collecte données fraîches via API."""
    
    api = ThetaDataAPI(settings.THETADATA_BASE_URL, settings.THETADATA_TIMEOUT)
    
    if date is None:
        date = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')
    
    print(f"\n📊 Collecting options data for {symbol}...")
    print(f"  Date: {date}")
    
    # Spot price
    df_spot = api.fetch('/stock/history/eod', {
        'symbol': symbol,
        'start_date': date,
        'end_date': date
    })
    
    if df_spot.empty:
        print("❌ No spot price found")
        return None, None
    
    spot_price = float(df_spot['close'].iloc[0])
    print(f"  Spot: ${spot_price:.2f}")
    
    # Expirations
    df_exp = api.fetch('/option/list/expirations', {'symbol': symbol})
    
    if df_exp.empty:
        print("❌ No expirations found")
        return None, None
    
    expirations = df_exp['expiration'].head(15).tolist()
    print(f"  Expirations: {len(expirations)}")
    
    # Collecter Greeks
    all_greeks = []
    
    for i, exp in enumerate(expirations, 1):
        print(f"  [{i}/{len(expirations)}] {exp}...", end=" ", flush=True)
        
        df_greeks = api.fetch('/option/history/greeks/eod', {
            'symbol': symbol,
            'expiration': exp,
            'start_date': date,
            'end_date': date
        })
        
        if not df_greeks.empty:
            df_greeks['expiration'] = pd.to_datetime(exp)
            df_greeks['date'] = pd.to_datetime(date)
            df_greeks['dte'] = (pd.to_datetime(exp) - pd.to_datetime(date)).days
            all_greeks.append(df_greeks)
            print(f"✓ {len(df_greeks)}")
        else:
            print("✗")
    
    if not all_greeks:
        print("❌ No Greeks data collected")
        return None, None
    
    df_greeks = pd.concat(all_greeks, ignore_index=True)
    
    # Filtrage qualité
    df_greeks = df_greeks[
        (df_greeks['iv_pct'] > 0) &
        (df_greeks['delta'].notna()) &
        (df_greeks['volume'] > 0) &
        (df_greeks['bid'] > 0)
    ].copy()
    
    print(f"\n✓ Total contracts: {len(df_greeks)}")
    print(f"  DTE range: {df_greeks['dte'].min()}-{df_greeks['dte'].max()} days")
    
    return df_greeks, spot_price

def load_from_storage(symbol: str):
    """Charge données depuis warehouse."""
    
    storage = StorageManager()
    
    print(f"\n📦 Loading data from warehouse...")
    
    # Charger master daily
    df_master = storage.load_master_daily(symbol)
    
    if df_master.empty:
        print("❌ No master data found")
        return None, None
    
    # Dernière date
    last_date = df_master['date'].max()
    spot_price = df_master.loc[df_master['date'] == last_date, 'spot_price'].iloc[0]
    
    print(f"  Last date: {last_date.date()}")
    print(f"  Spot: ${spot_price:.2f}")
    
    # Charger core Greeks
    date_str = last_date.strftime('%Y%m%d')
    
    # Chercher fichiers Greeks
    greeks_path = settings.PATHS['core_greeks'] / symbol / str(last_date.year) / f"{last_date.month:02d}"
    
    if not greeks_path.exists():
        print(f"❌ Greeks path not found: {greeks_path}")
        return None, None
    
    # Charger tous les Greeks de cette date
    greeks_files = list(greeks_path.glob(f"*{date_str}*.parquet"))
    
    if not greeks_files:
        print("❌ No Greeks files found")
        return None, None
    
    all_greeks = []
    for file in greeks_files:
        df = pd.read_parquet(file)
        all_greeks.append(df)
    
    df_greeks = pd.concat(all_greeks, ignore_index=True)
    
    print(f"✓ Loaded {len(df_greeks)} contracts from {len(greeks_files)} files")
    
    return df_greeks, spot_price

def main():
    """Point d'entrée."""
    
    print("""
╔══════════════════════════════════════════════════════════════════════╗
║                                                                      ║
║             VOLATILITY SKEW VISUALIZATION GENERATOR                  ║
║                                                                      ║
║  Creates professional volatility skew charts from collected data     ║
║                                                                      ║
╚══════════════════════════════════════════════════════════════════════╝
    """)
    
    # Configuration
    symbol = input("\nSymbol (default: SPY): ").strip().upper() or 'SPY'
    
    print("\nData source:")
    print("  1. Collect fresh data from ThetaData API")
    print("  2. Load from warehouse (faster)")
    
    source = input("Choice (1-2, default: 1): ").strip() or '1'
    
    # Charger données
    if source == '2':
        df_greeks, spot_price = load_from_storage(symbol)
    else:
        date_input = input("\nDate YYYYMMDD (Enter for yesterday): ").strip()
        date = date_input if date_input else None
        df_greeks, spot_price = collect_fresh_data(symbol, date)
    
    if df_greeks is None or spot_price is None:
        print("\n❌ Failed to load data")
        return
    
    # Créer output directory
    output_dir = Path('skew_plots')
    output_dir.mkdir(exist_ok=True)
    
    print(f"\n📊 Generating skew plots...")
    print(f"  Output directory: {output_dir}")
    
    # Initialiser visualizer
    viz = VolatilitySkewVisualizer()
    
    # Sélectionner expiration pour plots détaillés
    print("\n📅 Available expirations:")
    expirations = sorted(df_greeks['expiration'].unique())
    
    for i, exp in enumerate(expirations[:10], 1):
        dte = (exp - df_greeks['date'].iloc[0]).days
        print(f"  {i}. {exp.date()} ({dte} DTE)")
    
    exp_choice = input("\nSelect expiration number for detailed plots (default: closest to 30DTE): ").strip()
    
    if exp_choice:
        selected_exp = expirations[int(exp_choice) - 1]
    else:
        # Trouver expiration proche de 30 DTE
        df_greeks['dte_diff'] = (df_greeks['dte'] - 30).abs()
        selected_exp = df_greeks.loc[df_greeks['dte_diff'].idxmin(), 'expiration']
    
    exp_str = selected_exp.strftime('%Y%m%d')
    
    print(f"\n📈 Generating plots...")
    
    # 1. Skew by Strike
    print("  1/6 Skew by Strike...")
    viz.plot_skew_by_strike(
        df_greeks, spot_price, 
        expiration_filter=exp_str,
        save_path=output_dir / f"{symbol}_skew_strike_{exp_str}.png"
    )
    
    # 2. Skew by Delta
    print("  2/6 Skew by Delta...")
    viz.plot_skew_by_delta(
        df_greeks,
        expiration_filter=exp_str,
        save_path=output_dir / f"{symbol}_skew_delta_{exp_str}.png"
    )
    
    # 3. Skew by Moneyness
    print("  3/6 Skew by Moneyness...")
    viz.plot_skew_by_moneyness(
        df_greeks, spot_price,
        expiration_filter=exp_str,
        save_path=output_dir / f"{symbol}_skew_moneyness_{exp_str}.png"
    )
    
    # 4. Term Structure
    print("  4/6 Term Structure of Skew...")
    viz.plot_term_structure_skew(
        df_greeks,
        dte_targets=[7, 14, 30, 60, 90],
        save_path=output_dir / f"{symbol}_skew_term_structure.png"
    )
    
    # 5. 3D Surface
    print("  5/6 3D Volatility Surface...")
    viz.plot_volatility_surface_3d(
        df_greeks, spot_price,
        save_path=output_dir / f"{symbol}_vol_surface_3d.png"
    )
    
    # 6. Dashboard
    print("  6/6 Complete Dashboard...")
    viz.create_skew_dashboard(
        df_greeks, spot_price,
        expiration_filter=exp_str,
        save_path=output_dir / f"{symbol}_skew_dashboard_{exp_str}.png"
    )
    
    print(f"\n{'='*70}")
    print(f"  ✓ ALL PLOTS GENERATED SUCCESSFULLY")
    print(f"{'='*70}")
    
    print(f"\n📁 Output files in {output_dir}/:")
    for file in sorted(output_dir.glob(f"{symbol}_*.png")):
        print(f"  • {file.name}")
    
    print(f"\n💡 Use these plots for:")
    print("  • Trading decisions (put skew analysis)")
    print("  • Risk management (tail risk pricing)")
    print("  • Strategy selection (skew-based strategies)")
    print("  • Market regime identification")
    
    print("\n✓ Done!")

if __name__ == "__main__":
    main()
