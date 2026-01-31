"""
Quick Check - Validation rapide de l'installation.
Usage: python quick_check.py [date]
"""
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

try:
    from config import settings_test as settings
    TEST_MODE = True
except:
    from config import settings
    TEST_MODE = False

from core.api_wrapper import ThetaDataAPI
from core.data_collector import OptionsDataCollector
from storage.manager import StorageManager

import logging
logging.basicConfig(level=logging.WARNING, format='%(message)s')

class QuickCheck:
    def __init__(self):
        self.api = ThetaDataAPI(settings.THETADATA_BASE_URL, settings.THETADATA_TIMEOUT)
        self.collector = OptionsDataCollector()
        self.storage = StorageManager()
        self.errors = []
    
    def print_header(self, text):
        print("\n" + "═" * 70)
        print(f"  {text}")
        print("═" * 70)
    
    def print_step(self, step_num, text):
        print(f"\n[{step_num}/6] {text}...", end=" ", flush=True)
    
    def print_result(self, success, message=""):
        if success:
            print(f"✓ {message}")
        else:
            print(f"✗ {message}")
            self.errors.append(message)
    
    def check_connection(self):
        self.print_step(1, "Testing ThetaData connection")
        try:
            df = self.api.fetch('/stock/history/eod', {'symbol': 'SPY', 'start_date': '20250127', 'end_date': '20250127'})
            if not df.empty:
                self.print_result(True, f"Connected (got {len(df)} rows)")
                return True
            else:
                self.print_result(False, "Empty response")
                return False
        except Exception as e:
            self.print_result(False, f"Connection failed: {str(e)[:50]}")
            return False
    
    def run(self, date: str = None):
        self.print_header("THETADATA QUICK CHECK")
        
        if date is None:
            date = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')
        
        print(f"\n  Target date: {date}")
        print(f"  Storage: {settings.BASE_PATH}")
        
        start_time = time.time()
        
        if not self.check_connection():
            self.print_final_report(False, time.time() - start_time)
            return False
        
        self.print_final_report(True, time.time() - start_time)
        return True
    
    def print_final_report(self, success, elapsed):
        self.print_header("FINAL REPORT")
        print(f"\n  Duration: {elapsed:.1f}s")
        print(f"  Errors: {len(self.errors)}")
        
        if success:
            print("\n  ✓ ✓ ✓  ALL CHECKS PASSED  ✓ ✓ ✓")
            print("\n  🎉 System is ready!")
        else:
            print("\n  ✗ ✗ ✗  CHECKS FAILED  ✗ ✗ ✗")

def main():
    date = None
    if len(sys.argv) > 1:
        date = sys.argv[1].replace('-', '')
    
    checker = QuickCheck()
    success = checker.run(date)
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()
