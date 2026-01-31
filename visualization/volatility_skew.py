"""
Module de visualisation du skew de volatilité.
Créé des graphiques professionnels du volatility smile/skew.
"""
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d import Axes3D
from matplotlib import cm
from datetime import datetime
from typing import Optional, List
import logging

logger = logging.getLogger(__name__)

# Configuration style Bloomberg
plt.style.use('dark_background')

COLORS = {
    'bg': '#0a0e1a',
    'paper': '#131722',
    'text': '#d1d4dc',
    'grid': '#363c4e',
    'line1': '#26a69a',  # Teal
    'line2': '#ef5350',  # Red
    'line3': '#42a5f5',  # Blue
    'line4': '#ffa726',  # Orange
    'line5': '#ab47bc',  # Purple
}

class VolatilitySkewVisualizer:
    """Visualiseur de skew de volatilité."""
    
    def __init__(self, figsize=(14, 8)):
        self.figsize = figsize
        
    def plot_skew_by_strike(self, df_greeks: pd.DataFrame, spot_price: float, 
                            expiration_filter: Optional[str] = None, 
                            save_path: Optional[str] = None):
        """
        Volatility Skew : IV en fonction du Strike.
        Le graphique classique "smile" ou "smirk".
        
        Args:
            df_greeks: DataFrame avec colonnes [strike, iv_pct, right, expiration]
            spot_price: Prix spot actuel
            expiration_filter: Filtrer sur expiration spécifique (ex: '20250131')
            save_path: Chemin pour sauvegarder (optionnel)
        """
        fig, ax = plt.subplots(figsize=self.figsize, facecolor=COLORS['bg'])
        ax.set_facecolor(COLORS['paper'])
        
        # Filtrer données
        df = df_greeks.copy()
        
        if expiration_filter:
            df = df[df['expiration'] == pd.to_datetime(expiration_filter)]
        
        # Filtrer qualité
        df = df[
            (df['iv_pct'] > 0) & 
            (df['iv_pct'] < 100) &
            (df['volume'] > 0) &
            (df['strike'] > 0)
        ]
        
        if df.empty:
            logger.warning("No data to plot skew by strike")
            return None
        
        # Séparer Calls et Puts
        calls = df[df['right'] == 'CALL'].sort_values('strike')
        puts = df[df['right'] == 'PUT'].sort_values('strike')
        
        # Plot
        if not calls.empty:
            ax.scatter(calls['strike'], calls['iv_pct'], 
                      alpha=0.6, s=50, color=COLORS['line1'], 
                      label='Calls', edgecolors='white', linewidths=0.5)
            ax.plot(calls['strike'], calls['iv_pct'], 
                   alpha=0.3, linewidth=1, color=COLORS['line1'])
        
        if not puts.empty:
            ax.scatter(puts['strike'], puts['iv_pct'], 
                      alpha=0.6, s=50, color=COLORS['line2'], 
                      label='Puts', edgecolors='white', linewidths=0.5)
            ax.plot(puts['strike'], puts['iv_pct'], 
                   alpha=0.3, linewidth=1, color=COLORS['line2'])
        
        # Spot price line
        ax.axvline(spot_price, color=COLORS['line3'], 
                  linestyle='--', linewidth=2, label=f'Spot: ${spot_price:.2f}', alpha=0.7)
        
        # Styling
        ax.set_xlabel('Strike Price ($)', fontsize=12, color=COLORS['text'])
        ax.set_ylabel('Implied Volatility (%)', fontsize=12, color=COLORS['text'])
        
        title = 'Volatility Skew by Strike'
        if expiration_filter:
            exp_date = pd.to_datetime(expiration_filter).strftime('%Y-%m-%d')
            title += f' - Exp: {exp_date}'
        
        ax.set_title(title, fontsize=14, color=COLORS['text'], pad=20)
        
        ax.legend(loc='upper right', framealpha=0.9, facecolor=COLORS['paper'])
        ax.grid(True, alpha=0.3, color=COLORS['grid'])
        ax.tick_params(colors=COLORS['text'])
        
        plt.tight_layout()
        
        if save_path:
            plt.savefig(save_path, dpi=300, facecolor=COLORS['bg'])
            logger.info(f"Saved skew by strike: {save_path}")
        
        return fig
    
    def plot_skew_by_delta(self, df_greeks: pd.DataFrame, 
                          expiration_filter: Optional[str] = None,
                          save_path: Optional[str] = None):
        """
        Volatility Skew : IV en fonction du Delta.
        Représentation plus standardisée que par strike.
        
        Args:
            df_greeks: DataFrame avec colonnes [delta, iv_pct, right, expiration]
            expiration_filter: Filtrer sur expiration
            save_path: Chemin sauvegarde
        """
        fig, ax = plt.subplots(figsize=self.figsize, facecolor=COLORS['bg'])
        ax.set_facecolor(COLORS['paper'])
        
        # Filtrer
        df = df_greeks.copy()
        
        if expiration_filter:
            df = df[df['expiration'] == pd.to_datetime(expiration_filter)]
        
        df = df[
            (df['iv_pct'] > 0) & 
            (df['iv_pct'] < 100) &
            (df['delta'].notna()) &
            (df['volume'] > 0)
        ]
        
        if df.empty:
            logger.warning("No data to plot skew by delta")
            return None
        
        # Abs delta
        df['abs_delta'] = df['delta'].abs()
        
        # Séparer
        calls = df[df['right'] == 'CALL'].sort_values('abs_delta')
        puts = df[df['right'] == 'PUT'].sort_values('abs_delta')
        
        # Plot
        if not calls.empty:
            ax.scatter(calls['abs_delta'], calls['iv_pct'], 
                      alpha=0.6, s=50, color=COLORS['line1'], 
                      label='Calls', edgecolors='white', linewidths=0.5)
            ax.plot(calls['abs_delta'], calls['iv_pct'], 
                   alpha=0.3, linewidth=1, color=COLORS['line1'])
        
        if not puts.empty:
            ax.scatter(puts['abs_delta'], puts['iv_pct'], 
                      alpha=0.6, s=50, color=COLORS['line2'], 
                      label='Puts', edgecolors='white', linewidths=0.5)
            ax.plot(puts['abs_delta'], puts['iv_pct'], 
                   alpha=0.3, linewidth=1, color=COLORS['line2'])
        
        # ATM line (delta = 0.5)
        ax.axvline(0.5, color=COLORS['line3'], 
                  linestyle='--', linewidth=2, label='ATM (Δ=0.5)', alpha=0.7)
        
        # Delta markers
        for delta_level in [0.10, 0.25, 0.75, 0.90]:
            ax.axvline(delta_level, color=COLORS['grid'], 
                      linestyle=':', linewidth=1, alpha=0.5)
        
        # Styling
        ax.set_xlabel('|Delta|', fontsize=12, color=COLORS['text'])
        ax.set_ylabel('Implied Volatility (%)', fontsize=12, color=COLORS['text'])
        
        title = 'Volatility Skew by Delta'
        if expiration_filter:
            exp_date = pd.to_datetime(expiration_filter).strftime('%Y-%m-%d')
            title += f' - Exp: {exp_date}'
        
        ax.set_title(title, fontsize=14, color=COLORS['text'], pad=20)
        
        ax.set_xlim(0, 1)
        ax.legend(loc='upper right', framealpha=0.9, facecolor=COLORS['paper'])
        ax.grid(True, alpha=0.3, color=COLORS['grid'])
        ax.tick_params(colors=COLORS['text'])
        
        plt.tight_layout()
        
        if save_path:
            plt.savefig(save_path, dpi=300, facecolor=COLORS['bg'])
            logger.info(f"Saved skew by delta: {save_path}")
        
        return fig
    
    def plot_skew_by_moneyness(self, df_greeks: pd.DataFrame, spot_price: float,
                               expiration_filter: Optional[str] = None,
                               save_path: Optional[str] = None):
        """
        Volatility Skew : IV en fonction de Moneyness (Strike/Spot).
        Normalise le skew pour comparaison multi-dates.
        
        Args:
            df_greeks: DataFrame Greeks
            spot_price: Prix spot
            expiration_filter: Filtrer expiration
            save_path: Chemin sauvegarde
        """
        fig, ax = plt.subplots(figsize=self.figsize, facecolor=COLORS['bg'])
        ax.set_facecolor(COLORS['paper'])
        
        # Filtrer
        df = df_greeks.copy()
        
        if expiration_filter:
            df = df[df['expiration'] == pd.to_datetime(expiration_filter)]
        
        df = df[
            (df['iv_pct'] > 0) & 
            (df['iv_pct'] < 100) &
            (df['strike'] > 0) &
            (df['volume'] > 0)
        ]
        
        if df.empty:
            logger.warning("No data to plot skew by moneyness")
            return None
        
        # Calculer moneyness
        df['moneyness'] = df['strike'] / spot_price
        
        # Séparer
        calls = df[df['right'] == 'CALL'].sort_values('moneyness')
        puts = df[df['right'] == 'PUT'].sort_values('moneyness')
        
        # Plot
        if not calls.empty:
            ax.scatter(calls['moneyness'], calls['iv_pct'], 
                      alpha=0.6, s=50, color=COLORS['line1'], 
                      label='Calls', edgecolors='white', linewidths=0.5)
            ax.plot(calls['moneyness'], calls['iv_pct'], 
                   alpha=0.3, linewidth=1, color=COLORS['line1'])
        
        if not puts.empty:
            ax.scatter(puts['moneyness'], puts['iv_pct'], 
                      alpha=0.6, s=50, color=COLORS['line2'], 
                      label='Puts', edgecolors='white', linewidths=0.5)
            ax.plot(puts['moneyness'], puts['iv_pct'], 
                   alpha=0.3, linewidth=1, color=COLORS['line2'])
        
        # ATM line (moneyness = 1.0)
        ax.axvline(1.0, color=COLORS['line3'], 
                  linestyle='--', linewidth=2, label='ATM', alpha=0.7)
        
        # Moneyness zones
        ax.axvspan(0, 0.9, alpha=0.1, color='red', label='Deep OTM Puts')
        ax.axvspan(1.1, df['moneyness'].max(), alpha=0.1, color='green', label='Deep OTM Calls')
        
        # Styling
        ax.set_xlabel('Moneyness (Strike / Spot)', fontsize=12, color=COLORS['text'])
        ax.set_ylabel('Implied Volatility (%)', fontsize=12, color=COLORS['text'])
        
        title = 'Volatility Skew by Moneyness'
        if expiration_filter:
            exp_date = pd.to_datetime(expiration_filter).strftime('%Y-%m-%d')
            title += f' - Exp: {exp_date}'
        
        ax.set_title(title, fontsize=14, color=COLORS['text'], pad=20)
        
        ax.legend(loc='upper right', framealpha=0.9, facecolor=COLORS['paper'], fontsize=9)
        ax.grid(True, alpha=0.3, color=COLORS['grid'])
        ax.tick_params(colors=COLORS['text'])
        
        plt.tight_layout()
        
        if save_path:
            plt.savefig(save_path, dpi=300, facecolor=COLORS['bg'])
            logger.info(f"Saved skew by moneyness: {save_path}")
        
        return fig
    
    def plot_term_structure_skew(self, df_greeks: pd.DataFrame, 
                                 dte_targets: List[int] = [7, 30, 60, 90],
                                 save_path: Optional[str] = None):
        """
        Term Structure du Skew : IV en fonction du Delta pour plusieurs maturités.
        Compare le skew à travers différentes expirations.
        
        Args:
            df_greeks: DataFrame Greeks avec colonne 'dte'
            dte_targets: DTEs à afficher (ex: [7, 30, 60])
            save_path: Chemin sauvegarde
        """
        fig, ax = plt.subplots(figsize=self.figsize, facecolor=COLORS['bg'])
        ax.set_facecolor(COLORS['paper'])
        
        colors_map = [COLORS['line1'], COLORS['line2'], COLORS['line3'], 
                     COLORS['line4'], COLORS['line5']]
        
        # Pour chaque DTE
        for i, target_dte in enumerate(dte_targets):
            # Filtrer proche du DTE
            df_dte = df_greeks[
                (df_greeks['dte'] >= target_dte - 3) &
                (df_greeks['dte'] <= target_dte + 3) &
                (df_greeks['iv_pct'] > 0) &
                (df_greeks['delta'].notna()) &
                (df_greeks['volume'] > 0)
            ].copy()
            
            if df_dte.empty:
                continue
            
            df_dte['abs_delta'] = df_dte['delta'].abs()
            df_dte = df_dte.sort_values('abs_delta')
            
            # Plot (moyenne si plusieurs points même delta)
            grouped = df_dte.groupby(pd.cut(df_dte['abs_delta'], bins=20))['iv_pct'].mean()
            
            color = colors_map[i % len(colors_map)]
            actual_dte = int(df_dte['dte'].median())
            
            ax.plot(grouped.index.categories.mid, grouped.values, 
                   linewidth=2, color=color, label=f'{actual_dte} DTE',
                   marker='o', markersize=4, alpha=0.8)
        
        # ATM line
        ax.axvline(0.5, color=COLORS['text'], 
                  linestyle='--', linewidth=1, label='ATM', alpha=0.5)
        
        # Styling
        ax.set_xlabel('|Delta|', fontsize=12, color=COLORS['text'])
        ax.set_ylabel('Implied Volatility (%)', fontsize=12, color=COLORS['text'])
        ax.set_title('Volatility Skew - Term Structure', 
                    fontsize=14, color=COLORS['text'], pad=20)
        
        ax.set_xlim(0, 1)
        ax.legend(loc='upper right', framealpha=0.9, facecolor=COLORS['paper'])
        ax.grid(True, alpha=0.3, color=COLORS['grid'])
        ax.tick_params(colors=COLORS['text'])
        
        plt.tight_layout()
        
        if save_path:
            plt.savefig(save_path, dpi=300, facecolor=COLORS['bg'])
            logger.info(f"Saved term structure skew: {save_path}")
        
        return fig
    
    def plot_volatility_surface_3d(self, df_greeks: pd.DataFrame, spot_price: float,
                                   save_path: Optional[str] = None):
        """
        Surface de Volatilité 3D : IV en fonction de Strike et DTE.
        Visualisation complète de la surface de volatilité.
        
        Args:
            df_greeks: DataFrame Greeks avec [strike, dte, iv_pct]
            spot_price: Prix spot
            save_path: Chemin sauvegarde
        """
        fig = plt.figure(figsize=(16, 10), facecolor=COLORS['bg'])
        ax = fig.add_subplot(111, projection='3d', facecolor=COLORS['paper'])
        
        # Filtrer données
        df = df_greeks[
            (df_greeks['iv_pct'] > 0) &
            (df_greeks['iv_pct'] < 100) &
            (df_greeks['strike'] > 0) &
            (df_greeks['dte'] > 0) &
            (df_greeks['volume'] > 0)
        ].copy()
        
        if df.empty:
            logger.warning("No data for 3D volatility surface")
            return None
        
        # Calculer moneyness
        df['moneyness'] = df['strike'] / spot_price
        
        # Séparer Calls et Puts
        calls = df[df['right'] == 'CALL']
        puts = df[df['right'] == 'PUT']
        
        # Plot Calls
        if not calls.empty:
            ax.scatter(calls['moneyness'], calls['dte'], calls['iv_pct'],
                      c=calls['iv_pct'], cmap='Greens', s=20, alpha=0.6,
                      label='Calls', edgecolors='none')
        
        # Plot Puts
        if not puts.empty:
            ax.scatter(puts['moneyness'], puts['dte'], puts['iv_pct'],
                      c=puts['iv_pct'], cmap='Reds', s=20, alpha=0.6,
                      label='Puts', edgecolors='none')
        
        # Styling
        ax.set_xlabel('Moneyness (K/S)', fontsize=11, color=COLORS['text'])
        ax.set_ylabel('Days to Expiration', fontsize=11, color=COLORS['text'])
        ax.set_zlabel('Implied Volatility (%)', fontsize=11, color=COLORS['text'])
        
        ax.set_title('3D Volatility Surface', 
                    fontsize=14, color=COLORS['text'], pad=20)
        
        ax.tick_params(colors=COLORS['text'])
        ax.xaxis.pane.fill = False
        ax.yaxis.pane.fill = False
        ax.zaxis.pane.fill = False
        
        ax.legend(loc='upper right', framealpha=0.9, facecolor=COLORS['paper'])
        
        plt.tight_layout()
        
        if save_path:
            plt.savefig(save_path, dpi=300, facecolor=COLORS['bg'])
            logger.info(f"Saved 3D volatility surface: {save_path}")
        
        return fig
    
    def create_skew_dashboard(self, df_greeks: pd.DataFrame, spot_price: float,
                             expiration_filter: Optional[str] = None,
                             save_path: Optional[str] = None):
        """
        Dashboard complet avec 4 représentations du skew.
        
        Args:
            df_greeks: DataFrame Greeks complet
            spot_price: Prix spot
            expiration_filter: Filtrer sur expiration
            save_path: Chemin sauvegarde
        """
        fig = plt.figure(figsize=(18, 12), facecolor=COLORS['bg'])
        
        # Grid 2x2
        gs = fig.add_gridspec(2, 2, hspace=0.3, wspace=0.3)
        
        # 1. Skew by Strike
        ax1 = fig.add_subplot(gs[0, 0], facecolor=COLORS['paper'])
        self._plot_skew_subplot(ax1, df_greeks, spot_price, 'strike', expiration_filter)
        
        # 2. Skew by Delta
        ax2 = fig.add_subplot(gs[0, 1], facecolor=COLORS['paper'])
        self._plot_skew_subplot(ax2, df_greeks, spot_price, 'delta', expiration_filter)
        
        # 3. Skew by Moneyness
        ax3 = fig.add_subplot(gs[1, 0], facecolor=COLORS['paper'])
        self._plot_skew_subplot(ax3, df_greeks, spot_price, 'moneyness', expiration_filter)
        
        # 4. Put Skew Focus
        ax4 = fig.add_subplot(gs[1, 1], facecolor=COLORS['paper'])
        self._plot_put_skew_focus(ax4, df_greeks, expiration_filter)
        
        # Titre global
        title_text = 'Volatility Skew Dashboard'
        if expiration_filter:
            exp_date = pd.to_datetime(expiration_filter).strftime('%Y-%m-%d')
            title_text += f' - Exp: {exp_date}'
        
        fig.suptitle(title_text, fontsize=16, color=COLORS['text'], y=0.98)
        
        if save_path:
            plt.savefig(save_path, dpi=300, facecolor=COLORS['bg'], bbox_inches='tight')
            logger.info(f"Saved skew dashboard: {save_path}")
        
        return fig
    
    def _plot_skew_subplot(self, ax, df_greeks, spot_price, mode, expiration_filter):
        """Helper pour subplot skew."""
        df = df_greeks.copy()
        
        if expiration_filter:
            df = df[df['expiration'] == pd.to_datetime(expiration_filter)]
        
        df = df[(df['iv_pct'] > 0) & (df['volume'] > 0)]
        
        if mode == 'strike':
            calls = df[df['right'] == 'CALL'].sort_values('strike')
            puts = df[df['right'] == 'PUT'].sort_values('strike')
            x_col = 'strike'
            xlabel = 'Strike ($)'
            
        elif mode == 'delta':
            df['abs_delta'] = df['delta'].abs()
            calls = df[df['right'] == 'CALL'].sort_values('abs_delta')
            puts = df[df['right'] == 'PUT'].sort_values('abs_delta')
            x_col = 'abs_delta'
            xlabel = '|Delta|'
            
        elif mode == 'moneyness':
            df['moneyness'] = df['strike'] / spot_price
            calls = df[df['right'] == 'CALL'].sort_values('moneyness')
            puts = df[df['right'] == 'PUT'].sort_values('moneyness')
            x_col = 'moneyness'
            xlabel = 'Moneyness (K/S)'
        
        if not calls.empty:
            ax.scatter(calls[x_col], calls['iv_pct'], s=30, alpha=0.6, color=COLORS['line1'])
            ax.plot(calls[x_col], calls['iv_pct'], alpha=0.3, linewidth=1, color=COLORS['line1'], label='Calls')
        
        if not puts.empty:
            ax.scatter(puts[x_col], puts['iv_pct'], s=30, alpha=0.6, color=COLORS['line2'])
            ax.plot(puts[x_col], puts['iv_pct'], alpha=0.3, linewidth=1, color=COLORS['line2'], label='Puts')
        
        ax.set_xlabel(xlabel, color=COLORS['text'])
        ax.set_ylabel('IV (%)', color=COLORS['text'])
        ax.set_title(f'Skew by {mode.capitalize()}', color=COLORS['text'])
        ax.legend(framealpha=0.9, facecolor=COLORS['paper'])
        ax.grid(True, alpha=0.3, color=COLORS['grid'])
        ax.tick_params(colors=COLORS['text'])
    
    def _plot_put_skew_focus(self, ax, df_greeks, expiration_filter):
        """Focus sur le put skew (downside protection pricing)."""
        df = df_greeks.copy()
        
        if expiration_filter:
            df = df[df['expiration'] == pd.to_datetime(expiration_filter)]
        
        puts = df[
            (df['right'] == 'PUT') &
            (df['iv_pct'] > 0) &
            (df['delta'].notna()) &
            (df['volume'] > 0)
        ].copy()
        
        if puts.empty:
            return
        
        puts['abs_delta'] = puts['delta'].abs()
        puts = puts.sort_values('abs_delta')
        
        ax.scatter(puts['abs_delta'], puts['iv_pct'], s=40, alpha=0.7, 
                  c=puts['abs_delta'], cmap='Reds', edgecolors='white', linewidths=0.5)
        ax.plot(puts['abs_delta'], puts['iv_pct'], linewidth=2, color=COLORS['line2'], alpha=0.5)
        
        # Marquer deltas clés
        for delta in [0.10, 0.25, 0.50]:
            ax.axvline(delta, color=COLORS['grid'], linestyle=':', alpha=0.5)
            ax.text(delta, ax.get_ylim()[1]*0.95, f'Δ={delta:.0%}', 
                   ha='center', color=COLORS['text'], fontsize=9)
        
        ax.set_xlabel('|Delta|', color=COLORS['text'])
        ax.set_ylabel('IV (%)', color=COLORS['text'])
        ax.set_title('Put Skew (Downside Protection)', color=COLORS['text'])
        ax.set_xlim(0, 1)
        ax.grid(True, alpha=0.3, color=COLORS['grid'])
        ax.tick_params(colors=COLORS['text'])
