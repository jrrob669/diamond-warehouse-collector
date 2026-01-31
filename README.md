# ThetaData Options Warehouse

Production-grade data collection system for SPY options using ThetaData API v3.

## 🎯 Features

- ✅ **Net Delta & Gamma Exposure** tracking (market positioning)
- ✅ **Volatility Surface** (ATM IV, Skew, Term Structure)
- ✅ **Realized Volatility** (multi-period HV)
- ✅ **GEX Distribution** (strike-level gamma walls)
- ✅ **Put/Call Ratios** (4D: volume, OI, premium, delta)
- ✅ **Liquidity Metrics** (spread analysis, stress index)
- ✅ **Automated Alerts** (email for critical events)
- ✅ **Smart Storage** (Parquet with Snappy compression)

## 📋 Requirements

- ThetaData Terminal running (localhost:25503)
- US Options Standard subscription minimum
- Python 3.8+

## 🚀 Quick Start

### Installation

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Configure
# Edit config/settings.py or config/settings_test.py
# Update BASE_PATH and ALERT_EMAIL

# 3. Test
python quick_check.py

# 4. Run
python main.py
```

### Testing Locally

```bash
# Quick validation (30 seconds)
python quick_check.py

# Full test suite (10 minutes)
python test_runner.py

# Check collected data
python check_latest.py
```

### Production Deployment

```bash
# On VPS
nohup python main.py > warehouse.log 2>&1 &

# Monitor
tail -f warehouse.log
```

## 📂 Structure

```
warehouse_collector/
├── config/          # Configuration files
├── core/            # API wrapper & calculations
├── storage/         # Parquet storage manager
├── utils/           # Logging & alerts
├── main.py          # Scheduler (runs 22:15 daily)
├── test_runner.py   # Full test suite
├── quick_check.py   # Quick validation
└── backfill.py      # Historical data collection
```

## 📊 Data Collected

Daily metrics include:
- Net Gamma Exposure (billions $)
- Net Delta Exposure (millions $)
- IV ATM + Skew (RR25)
- Realized Volatility (10d, 20d, 252d)
- Put/Call Ratios (4 types)
- GEX Distribution by strike
- Liquidity stress index

## 🎓 Key Metrics

### Net Gamma
- **> +$5B**: Compression regime (low volatility)
- **< -$3B**: Danger zone (crash risk)

### Net Delta
- **Positive**: Dealers long → resistance
- **Negative**: Dealers short → acceleration

### IV-HV Spread
- **> +2%**: Options expensive → sell premium
- **< -2%**: Options cheap → buy protection

## 📈 Usage Examples

```python
# Load collected data
from storage.manager import StorageManager
storage = StorageManager()
df = storage.load_master_daily('SPY')

# Analyze
print(df[['date', 'net_gamma_billions', 'iv_atm']].tail())
```

## 🔧 Configuration

### Local Testing (settings_test.py)
```python
BASE_PATH = Path(r"C:\Users\YourName\warehouse_test")
SYMBOLS = ['SPY']
```

### Production (settings.py)
```python
BASE_PATH = Path("/home/your_user/warehouse")
SYMBOLS = ['SPY', 'QQQ', 'IWM']
```

## 📧 Alerts

Configure email alerts for:
- Net Gamma < -$3B
- Net Delta extremes
- Liquidity stress > 75

Set SMTP credentials in settings.py or via environment variable:
```bash
export SMTP_PASSWORD="your_app_password"
```

## 🐛 Troubleshooting

**Connection refused**: ThetaData Terminal not running
**HTTP 403**: Upgrade subscription
**HTTP 472**: No data (normal for weekends/invalid dates)
**Empty data**: Try different date

## 📚 Documentation

- ThetaData API: https://thetadata.net/docs
- OpenAPI Spec: See openapiv3.yaml in project

## 📄 License

MIT

## 👤 Contact

For questions: check ThetaData Discord
