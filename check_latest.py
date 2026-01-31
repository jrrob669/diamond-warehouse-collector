"""
Affiche les dernières données collectées.
Usage: python check_latest.py
"""
import pandas as pd
from pathlib import Path
from config import settings

master_file = settings.PATHS['aggregated'] / 'SPY_master_daily.parquet'

if not master_file.exists():
    print("❌ No data collected yet")
    exit(1)

df = pd.read_parquet(master_file)
df = df.sort_values('date')

print(f"\n📊 Latest Data ({len(df)} days collected)")
print("="*80)

latest = df.tail(5)
cols_display = ['date', 'spot_price', 'net_gamma_billions', 'net_delta_millions', 'iv_atm', 'pc_volume']
print(latest[cols_display].to_string(index=False))
print("="*80)

last_date = df['date'].max()
print(f"\nLast collected: {last_date.date()}")

completeness = (1 - df.isna().sum().sum() / (len(df) * len(df.columns))) * 100
print(f"Completeness: {completeness:.1f}%")
