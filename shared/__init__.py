"""
Shared utilities for aimarketdata

This package provides common functionality used across multiple data collection modules:
- spx_universe: S&P 500 ticker list fetching
- sector_mapping: GICS sector classifications
- cache_manager: Intelligent caching for yfinance data
- yfinance_fetcher: Centralized data downloads with caching

Usage:
    from shared.spx_universe import fetch_spx_tickers
    from shared.sector_mapping import get_sector_for_ticker
    from shared.cache_manager import CachedDataFetcher
"""

__version__ = "1.0.0"

# Export commonly used functions
from .spx_universe import fetch_spx_tickers, get_spx_tickers
from .sector_mapping import get_sector, TICKER_TO_SECTOR, get_tickers_by_sector

__all__ = [
    'fetch_spx_tickers',
    'get_spx_tickers',
    'get_sector',
    'TICKER_TO_SECTOR',
    'get_tickers_by_sector',
]
