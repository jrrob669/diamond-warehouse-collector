"""
Système de logging professionnel.
"""
import logging
import sys
from pathlib import Path

def setup_logging():
    """Configure le système de logging avec double output (console + fichier)."""
    from config import settings
    
    logger = logging.getLogger()
    logger.setLevel(settings.LOG_LEVEL)
    
    formatter = logging.Formatter(
        '%(asctime)s | %(levelname)-8s | %(name)s | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(settings.LOG_LEVEL)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    file_handler = logging.FileHandler(settings.LOG_FILE)
    file_handler.setLevel(settings.LOG_LEVEL)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('requests').setLevel(logging.WARNING)
    
    logger.info("=" * 80)
    logger.info("LOGGING SYSTEM INITIALIZED")
    logger.info(f"Log Level: {settings.LOG_LEVEL}")
    logger.info(f"Log File: {settings.LOG_FILE}")
    logger.info("=" * 80)
    
    return logger

def log_section_header(message: str):
    """Affiche un header de section visuellement distinct."""
    logger = logging.getLogger(__name__)
    logger.info("=" * 80)
    logger.info(f"  {message}")
    logger.info("=" * 80)

def log_metrics_summary(metrics: dict):
    """Affiche un résumé formaté des métriques."""
    logger = logging.getLogger(__name__)
    
    logger.info("-" * 80)
    logger.info("DAILY METRICS SUMMARY")
    logger.info("-" * 80)
    
    key_metrics = [
        ('spot_price', 'Spot Price', '${:.2f}'),
        ('net_gamma_billions', 'Net Gamma', '${:.2f}B'),
        ('net_delta_millions', 'Net Delta', '${:.2f}M'),
        ('iv_atm', 'IV ATM', '{:.2f}%'),
        ('rr25', 'Risk Reversal 25D', '{:.2f}%'),
        ('hv_20d', 'Realized Vol 20D', '{:.2f}%'),
        ('iv_hv_spread', 'IV-HV Spread', '{:.2f}%'),
        ('pc_volume', 'P/C Ratio (Volume)', '{:.2f}'),
        ('liquidity_liquidity_stress_index', 'Liquidity Stress', '{:.1f}')
    ]
    
    for key, label, fmt in key_metrics:
        if key in metrics and metrics[key] is not None:
            try:
                value = fmt.format(metrics[key])
                logger.info(f"  {label:.<40} {value}")
            except:
                pass
    
    logger.info("-" * 80)
