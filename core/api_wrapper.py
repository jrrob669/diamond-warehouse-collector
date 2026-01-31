"""
Wrapper API ThetaData v3 - Diamond Standard.
Zero-Crash Policy avec corrections empiriques.
"""
import requests
import pandas as pd
import io
import logging
from typing import Dict

logger = logging.getLogger(__name__)

class ThetaDataAPI:
    """
    Wrapper robuste pour ThetaData API v3.
    Gère automatiquement les corrections empiriques et les erreurs.
    """
    
    def __init__(self, base_url: str, timeout: int = 15):
        self.base_url = base_url
        self.timeout = timeout
        self.request_count = 0
        
    def fetch(self, endpoint: str, params: Dict) -> pd.DataFrame:
        """
        Méthode principale de récupération.
        
        Returns:
            DataFrame (vide si erreur - Zero-Crash Policy)
        """
        params['format'] = 'csv'
        
        # 1. Nettoyage dates (API requiert YYYYMMDD)
        for key in ['date', 'start_date', 'end_date', 'expiration']:
            if key in params:
                params[key] = str(params[key]).replace('-', '').replace('/', '')
        
        # 2. Auto-conversion strike (bidirectionnelle)
        if 'strike' in params:
            try:
                val = float(params['strike'])
                if val < 10000:
                    params['strike'] = int(val * 1000)
            except:
                pass
        
        # 3. Requête API
        try:
            self.request_count += 1
            
            response = requests.get(
                f"{self.base_url}{endpoint}",
                params=params,
                timeout=self.timeout
            )
            
            # Gestion codes HTTP spécifiques ThetaData
            if response.status_code == 472:
                logger.debug(f"No data for {endpoint} with params {params}")
                return pd.DataFrame()
            
            if response.status_code == 403:
                logger.error(f"Subscription insufficient for {endpoint}")
                return pd.DataFrame()
            
            if response.status_code != 200:
                logger.warning(f"HTTP {response.status_code} on {endpoint}")
                return pd.DataFrame()
            
            if not response.text.strip():
                return pd.DataFrame()
            
            # 4. Parsing CSV
            try:
                df = pd.read_csv(io.StringIO(response.text))
            except pd.errors.ParserError as e:
                logger.warning(f"CSV parsing error on {endpoint}: {e}")
                return pd.DataFrame()
            
            if df.empty:
                return pd.DataFrame()
            
            # 5. Normalisation colonnes
            df.columns = df.columns.str.strip().str.lower()
            
            # 6. CORRECTION EMPIRIQUE #1: Normaliser Put/Call
            if 'right' in df.columns:
                df['right'] = df['right'].astype(str).str.upper().str.strip()
                df = df[df['right'].isin(['CALL', 'PUT'])]
            
            # 7. CORRECTION EMPIRIQUE #2: IV en pourcentage
            if 'implied_vol' in df.columns:
                df['iv_pct'] = df['implied_vol'] * 100
            
            # 8. CORRECTION EMPIRIQUE #3: Millistrikes vers dollars
            if 'strike' in df.columns:
                df['strike'] = pd.to_numeric(df['strike'], errors='coerce') / 1000
            
            # 9. Conversion dates
            date_cols = ['expiration', 'date', 'timestamp', 'created']
            for col in date_cols:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col], errors='coerce')
            
            logger.debug(f"Fetched {len(df)} rows from {endpoint}")
            return df
        
        except requests.exceptions.Timeout:
            logger.error(f"Timeout on {endpoint}")
            return pd.DataFrame()
        
        except Exception as e:
            logger.error(f"Unexpected error on {endpoint}: {e}")
            return pd.DataFrame()
    
    def get_stats(self) -> Dict:
        """Statistiques d'utilisation API."""
        return {
            'total_requests': self.request_count
        }
