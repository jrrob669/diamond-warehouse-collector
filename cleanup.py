"""
Nettoie les données de test.
Usage: python cleanup.py
"""
import shutil
from config import settings

response = input(f"Delete ALL data in {settings.BASE_PATH}? (yes/no): ")

if response.lower() == 'yes':
    for path_name, path in settings.PATHS.items():
        if path.exists():
            for item in path.iterdir():
                if item.is_file():
                    item.unlink()
                elif item.is_dir():
                    shutil.rmtree(item)
            print(f"✓ Cleaned {path_name}")
    
    print("\n✓ All test data deleted")
else:
    print("Cancelled")
