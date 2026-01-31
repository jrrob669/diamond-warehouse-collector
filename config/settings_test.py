"""
Configuration TEST pour validation locale.
À utiliser AVANT déploiement VPS.
"""
import os
from pathlib import Path
from datetime import datetime, timedelta

# ============================================================================
# MODE TEST
# ============================================================================
TEST_MODE = True

# ============================================================================
# API CONFIGURATION
# ============================================================================
THETADATA_BASE_URL = "http://localhost:25503/v3"
THETADATA_TIMEOUT = 15
RATE_LIMIT_DELAY = 0.05

# ============================================================================
# SYMBOLS TO TRACK (TEST)
# ============================================================================
SYMBOLS = ['SPY']  # Un seul pour test

# ============================================================================
# STORAGE CONFIGURATION (LOCAL)
# ============================================================================
# ADAPTER SELON VOTRE OS:
# Windows:
BASE_PATH = Path("/home/ubuntu/warehouse_test")
# Mac/Linux:
# BASE_PATH_OLD = Path.home() / 'warehouse_test'

PATHS = {
    'core_prices': BASE_PATH / 'core' / 'prices',
    'core_greeks': BASE_PATH / 'core' / 'greeks',
    'core_oi': BASE_PATH / 'core' / 'open_interest',
    'derived_hv': BASE_PATH / 'derived' / 'realized_vol',
    'derived_vol_surface': BASE_PATH / 'derived' / 'vol_surface',
    'derived_pc_ratios': BASE_PATH / 'derived' / 'pc_ratios',
    'derived_gex': BASE_PATH / 'derived' / 'gex_distribution',
    'derived_liquidity': BASE_PATH / 'derived' / 'liquidity_metrics',
    'derived_volume': BASE_PATH / 'derived' / 'volume_concentration',
    'derived_net_exposure': BASE_PATH / 'derived' / 'net_exposure',
    'aggregated': BASE_PATH / 'aggregated',
    'external': BASE_PATH / 'external',
    'state': BASE_PATH / 'state',
    'logs': BASE_PATH / 'logs'
}

for path in PATHS.values():
    path.mkdir(parents=True, exist_ok=True)

STATE_FILE = PATHS['state'] / 'collection_state.json'

# ============================================================================
# TEST PARAMETERS
# ============================================================================
TEST_START_DATE = (datetime.now() - timedelta(days=10)).strftime('%Y-%m-%d')
TEST_END_DATE = datetime.now().strftime('%Y-%m-%d')
MAX_EXPIRATIONS_FOR_GEX = 5

# ============================================================================
# COLLECTION PARAMETERS
# ============================================================================
HV_WINDOWS = [10, 20, 30, 60, 252]
MIN_VOLUME = 5
MIN_OPEN_INTEREST = 10
MIN_BID = 0.01

# ============================================================================
# ALERTES (DÉSACTIVÉES EN TEST)
# ============================================================================
ALERT_EMAIL = None
SMTP_SERVER = None
SMTP_PORT = None
SMTP_USER = None
SMTP_PASSWORD = None

ALERT_THRESHOLDS = {
    'gex_negative_critical': -3_000_000_000,
    'gex_positive_extreme': 10_000_000_000,
    'net_delta_extreme': 500_000_000,
    'iv_percentile_high': 90,
    'iv_percentile_low': 10,
    'liquidity_stress': 75
}

# ============================================================================
# LOGGING
# ============================================================================
LOG_LEVEL = "INFO"
LOG_FILE = PATHS['logs'] / 'collector_test.log'

print(f"""
╔═══════════════════════════════════════════════════════════════╗
║                    TEST MODE ACTIVATED                        ║
╚═══════════════════════════════════════════════════════════════╝
  Symbols: {SYMBOLS}
  Storage: {BASE_PATH}
  Test Period: {TEST_START_DATE} to {TEST_END_DATE}
  Alerts: DISABLED
""")
