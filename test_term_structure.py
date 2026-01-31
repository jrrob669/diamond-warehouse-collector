"""
Test Term Structure - Validation RR25 par DTE
Usage: python test_term_structure.py
"""
import sys
import pandas as pd
from datetime import datetime, timedelta

# Importer modules
sys.path.insert(0, '.')
try:
    from config import settings_test as settings
except:
    from config import settings

from core.api_wrapper import ThetaDataAPI
from core.term_structure import calculate_term_structure_metrics

def test_term_structure():
    """Test complet du module term structure."""
    
    print("\n" + "="*80)
    print("  TERM STRUCTURE TEST - RR25 by DTE")
    print("="*80)
    
    api = ThetaDataAPI(settings.THETADATA_BASE_URL, settings.THETADATA_TIMEOUT)
    
    # Date de test (hier)
    test_date = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')
    
    print(f"\n📅 Test Date: {test_date}")
    print(f"📊 Symbol: SPY")
    
    # 1. Collecter toutes les expirations
    print("\n[1/3] Fetching all expirations...", end=" ", flush=True)
    
    df_exp = api.fetch('/option/list/expirations', {'symbol': 'SPY'})
    
    if df_exp.empty:
        print("✗ No expirations found")
        return False
    
    print(f"✓ Found {len(df_exp)} expirations")
    
    # 2. Collecter Greeks pour toutes expirations
    print("\n[2/3] Collecting Greeks data...", end=" ", flush=True)
    
    all_greeks = []
    
    for exp in df_exp['expiration'][:20]:  # Limiter à 20 pour test
        df_greeks = api.fetch('/option/history/greeks/eod', {
            'symbol': 'SPY',
            'expiration': exp,
            'start_date': test_date,
            'end_date': test_date
        })
        
        if not df_greeks.empty:
            df_greeks['expiration'] = pd.to_datetime(exp)
            df_greeks['date'] = pd.to_datetime(test_date)
            df_greeks['dte'] = (pd.to_datetime(exp) - pd.to_datetime(test_date)).days
            all_greeks.append(df_greeks)
    
    if not all_greeks:
        print("✗ No Greeks data collected")
        return False
    
    df_all_greeks = pd.concat(all_greeks, ignore_index=True)
    print(f"✓ Collected {len(df_all_greeks)} rows")
    
    # Afficher distribution DTE
    print(f"\n  DTE Range: {df_all_greeks['dte'].min()} to {df_all_greeks['dte'].max()} days")
    
    dte_counts = df_all_greeks.groupby('dte').size()
    print(f"  Expirations available:")
    for dte, count in dte_counts.head(10).items():
        print(f"    DTE {dte:>3}: {count:>4} contracts")
    
    # 3. Calculer term structure metrics
    print("\n[3/3] Calculating term structure...", end=" ", flush=True)
    
    metrics = calculate_term_structure_metrics(df_all_greeks)
    
    print("✓ Done")
    
    # 4. Afficher résultats
    print("\n" + "="*80)
    print("  RESULTS - RR25 by Maturity")
    print("="*80)
    
    print("\n  Risk Reversal 25-Delta (Put Skew):")
    print("  " + "-"*76)
    
    target_dtes = [0, 7, 30, 60]
    
    for dte in target_dtes:
        rr25_key = f'rr25_{dte}dte'
        iv_atm_key = f'iv_atm_{dte}dte'
        actual_dte_key = f'actual_dte_{dte}'
        
        rr25_value = metrics.get(rr25_key)
        iv_atm_value = metrics.get(iv_atm_key)
        actual_dte = metrics.get(actual_dte_key)
        
        print(f"\n  Target: {dte}DTE (Actual: {actual_dte} days)")
        
        if pd.notna(rr25_value):
            print(f"    RR25:   {rr25_value:>6.2f}%  ", end="")
            
            # Interprétation
            if rr25_value > 4:
                print("[HIGH PUT DEMAND - Fear]")
            elif rr25_value > 2:
                print("[Elevated]")
            elif rr25_value > 0:
                print("[Normal]")
            else:
                print("[Call skew - Rare]")
        else:
            print(f"    RR25:   N/A")
        
        if pd.notna(iv_atm_value):
            print(f"    IV ATM: {iv_atm_value:>6.2f}%")
        else:
            print(f"    IV ATM: N/A")
    
    # 5. Term structure spreads
    print("\n  " + "-"*76)
    print("\n  Term Structure Analysis:")
    
    rr25_spread = metrics.get('rr25_term_spread')
    iv_spread = metrics.get('iv_term_spread')
    
    if pd.notna(rr25_spread):
        print(f"    RR25 Spread (60d - 7d): {rr25_spread:>6.2f}%")
        
        if rr25_spread > 1:
            print("      → Skew steepens with maturity (back-end loaded)")
        elif rr25_spread < -1:
            print("      → Skew flattens with maturity (front-end loaded)")
        else:
            print("      → Flat skew term structure")
    
    if pd.notna(iv_spread):
        print(f"    IV Spread (60d - 7d):   {iv_spread:>6.2f}%")
        
        if iv_spread > 2:
            print("      → Contango (normal term structure)")
        elif iv_spread < -2:
            print("      → Backwardation (inverted - stress)")
        else:
            print("      → Flat term structure")
    
    # 6. Comparaison avec global RR25
    print("\n  " + "-"*76)
    print("\n  Comparison:")
    
    # Calculer RR25 global (comme avant)
    from core.calculations import interpolate_iv_at_delta
    
    iv_25d_put = interpolate_iv_at_delta(df_all_greeks, 0.25, 'PUT')
    iv_25d_call = interpolate_iv_at_delta(df_all_greeks, 0.25, 'CALL')
    
    if pd.notna(iv_25d_put) and pd.notna(iv_25d_call):
        rr25_global = iv_25d_put - iv_25d_call
        print(f"    RR25 Global (all expirations mixed): {rr25_global:>6.2f}%")
        print(f"    RR25 30DTE (specific maturity):     {metrics.get('rr25_30dte', 'N/A'):>6}%")
        
        if pd.notna(metrics.get('rr25_30dte')):
            diff = metrics['rr25_30dte'] - rr25_global
            print(f"    Difference:                          {diff:>6.2f}%")
    
    print("\n" + "="*80)
    
    # 7. Summary
    valid_metrics = sum(1 for k, v in metrics.items() if 'rr25' in k and pd.notna(v))
    
    if valid_metrics >= 3:
        print("\n✓ ✓ ✓  TERM STRUCTURE TEST PASSED  ✓ ✓ ✓")
        print(f"\n  {valid_metrics}/4 maturities have valid RR25 data")
        print("\n  Next steps:")
        print("    1. Integrate into data_collector.py")
        print("    2. Collect daily term structure data")
        print("    3. Analyze term structure evolution over time")
        return True
    else:
        print("\n⚠️  PARTIAL DATA - Some maturities missing")
        print(f"\n  Only {valid_metrics}/4 maturities have valid data")
        print("\n  Common reasons:")
        print("    - Weekend (no 0DTE available)")
        print("    - Illiquid options (no volume)")
        print("    - Missing expirations in date range")
        return False

def main():
    """Point d'entrée."""
    
    print("""
╔═══════════════════════════════════════════════════════════════════════╗
║                                                                       ║
║           TERM STRUCTURE TEST - RR25 by DTE                          ║
║                                                                       ║
║  Tests the calculation of Risk Reversal 25-delta for specific        ║
║  maturities: 0DTE, 7DTE, 30DTE, 60DTE                                ║
║                                                                       ║
╚═══════════════════════════════════════════════════════════════════════╝
    """)
    
    success = test_term_structure()
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()
