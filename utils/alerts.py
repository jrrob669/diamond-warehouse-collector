"""
Système d'alertes par email.
"""
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import logging
from typing import Dict

logger = logging.getLogger(__name__)

class AlertSystem:
    """Système d'alertes intelligent."""
    
    def __init__(self):
        from config import settings
        self.thresholds = settings.ALERT_THRESHOLDS
        self.smtp_config = {
            'server': settings.SMTP_SERVER,
            'port': settings.SMTP_PORT,
            'user': settings.SMTP_USER,
            'password': settings.SMTP_PASSWORD,
            'to': settings.ALERT_EMAIL
        }
        self.enabled = bool(settings.SMTP_PASSWORD)
        
        if not self.enabled:
            logger.warning("Alert system disabled (no SMTP_PASSWORD)")
    
    def send_email(self, subject: str, body: str, html: bool = False):
        """Envoie un email d'alerte."""
        if not self.enabled:
            logger.debug(f"Alert skipped (disabled): {subject}")
            return
        
        try:
            msg = MIMEMultipart('alternative')
            msg['From'] = self.smtp_config['user']
            msg['To'] = self.smtp_config['to']
            msg['Subject'] = f"[ThetaData Warehouse] {subject}"
            
            if html:
                msg.attach(MIMEText(body, 'html'))
            else:
                msg.attach(MIMEText(body, 'plain'))
            
            with smtplib.SMTP(self.smtp_config['server'], self.smtp_config['port']) as server:
                server.starttls()
                server.login(self.smtp_config['user'], self.smtp_config['password'])
                server.send_message(msg)
            
            logger.info(f"Alert sent: {subject}")
        
        except Exception as e:
            logger.error(f"Failed to send alert: {e}")
    
    def check_and_alert(self, metrics: Dict):
        """Vérifie les métriques et déclenche alertes si seuils dépassés."""
        if not self.enabled:
            return
        
        alerts = []
        
        net_gamma = metrics.get('net_gamma', 0)
        if net_gamma < self.thresholds['gex_negative_critical']:
            alerts.append({
                'level': 'CRITICAL',
                'metric': 'Net Gamma',
                'value': f"${net_gamma/1e9:.2f}B",
                'threshold': f"${self.thresholds['gex_negative_critical']/1e9:.2f}B",
                'message': '🚨 NEGATIVE GAMMA REGIME - High volatility risk'
            })
        
        if net_gamma > self.thresholds['gex_positive_extreme']:
            alerts.append({
                'level': 'WARNING',
                'metric': 'Net Gamma',
                'value': f"${net_gamma/1e9:.2f}B",
                'threshold': f"${self.thresholds['gex_positive_extreme']/1e9:.2f}B",
                'message': '⚠️ EXTREME POSITIVE GAMMA - Volatility compression'
            })
        
        net_delta = abs(metrics.get('net_delta', 0))
        if net_delta > self.thresholds['net_delta_extreme']:
            alerts.append({
                'level': 'WARNING',
                'metric': 'Net Delta',
                'value': f"${net_delta/1e6:.2f}M",
                'threshold': f"${self.thresholds['net_delta_extreme']/1e6:.2f}M",
                'message': '⚠️ EXTREME DELTA POSITIONING - Market imbalance'
            })
        
        if alerts:
            self._send_grouped_alerts(metrics, alerts)
    
    def _send_grouped_alerts(self, metrics: Dict, alerts: list):
        """Envoie un email groupé avec toutes les alertes."""
        symbol = metrics.get('symbol', 'UNKNOWN')
        date = metrics.get('date', 'UNKNOWN')
        subject = f"⚠️ {len(alerts)} Alert(s) for {symbol} on {date}"
        body = f"Alerts triggered. Check logs for details. Metrics: {metrics}"
        self.send_email(subject, body)
