"""
Script de validation pour tester l'installation locale.
Usage: python test_runner.py
"""
import sys
import time
from datetime import datetime, timedelta
import logging

sys.path.insert(0, '.')
try:
    from config import settings_test as settings
except:
    from config import settings

from utils.logger import setup_logging
from core.api_wrapper import ThetaDataAPI
from core.data_collector import OptionsDataCollector
from storage.manager import StorageManager

logger = setup_logging()

class TestRunner:
    """Validateur pour installation locale."""
    
    def __init__(self):
        self.api = ThetaDataAPI(settings.THETADATA_BASE_URL, settings.THETADATA_TIMEOUT)
        self.collector = OptionsDataCollector()
        self.storage = StorageManager()
        self.test_results = []
    
    def log_test(self, name, success, message=""):
        status = "✓ PASS" if success else "✗ FAIL"
        logger.info(f"{status} | {name} | {message}")
        self.test_results.append({'test': name, 'success': success, 'message': message})
    
    def test_1_thetadata_connection(self):
        logger.info("\n" + "="*80)
        logger.info("TEST 1: ThetaData Connection")
        logger.info("="*80)
        
        try:
            df = self.api.fetch('/stock/history/eod', {
                'symbol': 'SPY', 'start_date': '20250127', 'end_date': '20250127'
            })
            
            if not df.empty:
                self.log_test("ThetaData Connection", True, f"Retrieved {len(df)} rows")
                return True
            else:
                self.log_test("ThetaData Connection", False, "Empty response")
                return False
        except Exception as e:
            self.log_test("ThetaData Connection", False, f"Error: {str(e)}")
            return False
    
    def test_2_subscription_check(self):
        logger.info("\n" + "="*80)
        logger.info("TEST 2: Subscription Validation")
        logger.info("="*80)
        
        tests = {
            'Stock EOD': {'endpoint': '/stock/history/eod', 'params': {'symbol': 'SPY', 'start_date': '20250127', 'end_date': '20250127'}},
            'Options Greeks': {'endpoint': '/option/history/greeks/eod', 'params': {'symbol': 'SPY', 'expiration': '20250131', 'start_date': '20250127', 'end_date': '20250127'}}
        }
        
        all_success = True
        for name, config in tests.items():
            try:
                df = self.api.fetch(config['endpoint'], config['params'])
                if not df.empty:
                    self.log_test(f"Subscription: {name}", True, f"{len(df)} rows")
                else:
                    self.log_test(f"Subscription: {name}", False, "Empty")
                    all_success = False
                time.sleep(0.1)
            except Exception as e:
                self.log_test(f"Subscription: {name}", False, str(e))
                all_success = False
        
        return all_success
    
    def run_all_tests(self):
        logger.info("\n" + "╔" + "="*78 + "╗")
        logger.info("║" + " "*25 + "TEST SUITE START" + " "*37 + "║")
        logger.info("╚" + "="*78 + "╝")
        
        start_time = time.time()
        
        self.test_1_thetadata_connection()
        self.test_2_subscription_check()
        
        elapsed = time.time() - start_time
        passed = sum(1 for r in self.test_results if r['success'])
        total = len(self.test_results)
        
        logger.info(f"\nResults: {passed}/{total} tests passed")
        logger.info(f"Duration: {elapsed:.1f}s")
        
        return passed == total

def main():
    runner = TestRunner()
    success = runner.run_all_tests()
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()
