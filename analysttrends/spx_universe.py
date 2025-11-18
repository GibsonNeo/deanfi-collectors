"""
S&P 500 Universe Fetcher with Deduplication

Fetches the list of S&P 500 tickers with multiple fallback sources:
1. Wikipedia (primary)
2. GitHub dataset (fallback)
3. Hardcoded list (last resort)

Includes logic to remove duplicate share classes and keep the most liquid ticker.
"""
from pathlib import Path
from typing import Optional
import pandas as pd
import requests
from io import StringIO
import json
import sys

WIKI_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
GITHUB_SPX_URL = "https://raw.githubusercontent.com/datasets/s-and-p-500-companies/main/data/constituents.csv"

# Fallback: Static list (last updated: 2025-11-11)
FALLBACK_TICKERS = [
    "A", "AAPL", "ABBV", "ABNB", "ABT", "ACGL", "ACN", "ADBE", "ADI", "ADM",
    "ADP", "ADSK", "AEE", "AEP", "AES", "AFL", "AIG", "AIZ", "AJG", "AKAM",
    "ALB", "ALGN", "ALL", "ALLE", "AMAT", "AMCR", "AMD", "AME", "AMGN", "AMP",
    "AMT", "AMZN", "ANET", "AON", "AOS", "APA", "APD", "APH", "APO", "APP",
    "APTV", "ARE", "ATO", "AVB", "AVGO", "AVY", "AWK", "AXON", "AXP", "AZO",
    "BA", "BAC", "BALL", "BAX", "BBY", "BDX", "BEN", "BF-B", "BG", "BIIB",
    "BK", "BKNG", "BKR", "BLDR", "BLK", "BMY", "BR", "BRK-B", "BRO", "BSX",
    "BX", "BXP", "C", "CAG", "CAH", "CARR", "CAT", "CB", "CBOE", "CBRE",
    "CCI", "CCL", "CDNS", "CDW", "CEG", "CF", "CFG", "CHD", "CHRW", "CHTR",
    "CI", "CINF", "CL", "CLX", "CMCSA", "CME", "CMG", "CMI", "CMS", "CNC",
    "CNP", "COF", "COIN", "COO", "COP", "COR", "COST", "CPAY", "CPB", "CPRT",
    "CPT", "CRL", "CRM", "CRWD", "CSCO", "CSGP", "CSX", "CTAS", "CTRA", "CTSH",
    "CTVA", "CVS", "CVX", "D", "DAL", "DASH", "DAY", "DD", "DDOG", "DE",
    "DECK", "DELL", "DG", "DGX", "DHI", "DHR", "DIS", "DLR", "DLTR", "DOC",
    "DOV", "DOW", "DPZ", "DRI", "DTE", "DUK", "DVA", "DVN", "DXCM", "EA",
    "EBAY", "ECL", "ED", "EFX", "EG", "EIX", "EL", "ELV", "EME", "EMR",
    "EOG", "EPAM", "EQIX", "EQR", "EQT", "ERIE", "ES", "ESS", "ETN", "ETR",
    "EVRG", "EW", "EXC", "EXE", "EXPD", "EXPE", "EXR", "F", "FANG", "FAST",
    "FCX", "FDS", "FDX", "FE", "FFIV", "FI", "FICO", "FIS", "FITB", "FOXA",
    "FRT", "FSLR", "FTNT", "FTV", "GD", "GDDY", "GE", "GEHC", "GEN",
    "GEV", "GILD", "GIS", "GL", "GLW", "GM", "GNRC", "GOOGL", "GPC",
    "GPN", "GRMN", "GS", "GWW", "HAL", "HAS", "HBAN", "HCA", "HD", "HIG",
    "HII", "HLT", "HOLX", "HON", "HOOD", "HPE", "HPQ", "HRL", "HSIC", "HST",
    "HSY", "HUBB", "HUM", "HWM", "IBKR", "IBM", "ICE", "IDXX", "IEX", "IFF",
    "INCY", "INTC", "INTU", "INVH", "IP", "IPG", "IQV", "IR", "IRM", "ISRG",
    "IT", "ITW", "IVZ", "J", "JBHT", "JBL", "JCI", "JKHY", "JNJ", "JPM",
    "K", "KDP", "KEY", "KEYS", "KHC", "KIM", "KKR", "KLAC", "KMB", "KMI",
    "KO", "KR", "KVUE", "L", "LDOS", "LEN", "LH", "LHX", "LII", "LIN",
    "LKQ", "LLY", "LMT", "LNT", "LOW", "LRCX", "LULU", "LUV", "LVS", "LW",
    "LYB", "LYV", "MA", "MAA", "MAR", "MAS", "MCD", "MCHP", "MCK", "MCO",
    "MDLZ", "MDT", "MET", "META", "MGM", "MHK", "MKC", "MLM", "MMC", "MMM",
    "MNST", "MO", "MOH", "MOS", "MPC", "MPWR", "MRK", "MRNA", "MS", "MSCI",
    "MSFT", "MSI", "MTB", "MTCH", "MTD", "MU", "NCLH", "NDAQ", "NDSN", "NEE",
    "NEM", "NFLX", "NI", "NKE", "NOC", "NOW", "NRG", "NSC", "NTAP", "NTRS",
    "NUE", "NVDA", "NVR", "NWSA", "NXPI", "O", "ODFL", "OKE", "OMC",
    "ON", "ORCL", "ORLY", "OTIS", "OXY", "PANW", "PAYC", "PAYX", "PCAR", "PCG",
    "PEG", "PEP", "PFE", "PFG", "PG", "PGR", "PH", "PHM", "PKG", "PLD",
    "PLTR", "PM", "PNC", "PNR", "PNW", "PODD", "POOL", "PPG", "PPL", "PRU",
    "PSA", "PSKY", "PSX", "PTC", "PWR", "PYPL", "Q", "QCOM", "RCL", "REG",
    "REGN", "RF", "RJF", "RL", "RMD", "ROK", "ROL", "ROP", "ROST", "RSG",
    "RTX", "RVTY", "SBAC", "SBUX", "SCHW", "SHW", "SJM", "SLB", "SMCI", "SNA",
    "SNPS", "SO", "SOLS", "SOLV", "SPG", "SPGI", "SRE", "STE", "STLD", "STT",
    "STX", "STZ", "SW", "SWK", "SWKS", "SYF", "SYK", "SYY", "T", "TAP",
    "TDG", "TDY", "TECH", "TEL", "TER", "TFC", "TGT", "TJX", "TKO", "TMO",
    "TMUS", "TPL", "TPR", "TRGP", "TRMB", "TROW", "TRV", "TSCO", "TSLA", "TSN",
    "TT", "TTD", "TTWO", "TXN", "TXT", "TYL", "UAL", "UBER", "UDR", "UHS",
    "ULTA", "UNH", "UNP", "UPS", "URI", "USB", "V", "VICI", "VLO", "VLTO",
    "VMC", "VRSK", "VRSN", "VRTX", "VST", "VTR", "VTRS", "VZ", "WAB", "WAT",
    "WBD", "WDAY", "WDC", "WEC", "WELL", "WFC", "WM", "WMB", "WMT", "WRB",
    "WSM", "WST", "WTW", "WY", "WYNN", "XEL", "XOM", "XYL", "XYZ", "YUM",
    "ZBH", "ZBRA", "ZTS"
]


def fetch_from_wikipedia() -> list[str]:
    """Fetch S&P 500 tickers from Wikipedia."""
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/122.0 Safari/537.36"
        )
    }
    resp = requests.get(WIKI_URL, headers=headers, timeout=20)
    resp.raise_for_status()

    tables = pd.read_html(StringIO(resp.text))
    
    # Find the table with 'Symbol' column
    df = None
    for table in tables:
        if 'Symbol' in table.columns and len(table) > 400:
            df = table
            break
    
    if df is None:
        raise ValueError("Could not find S&P 500 constituents table in Wikipedia HTML")
    
    tickers = df['Symbol'].str.replace('.', '-').tolist()
    return tickers


def fetch_from_github() -> list[str]:
    """Fetch S&P 500 tickers from GitHub dataset."""
    resp = requests.get(GITHUB_SPX_URL, timeout=20)
    resp.raise_for_status()
    
    df = pd.read_csv(StringIO(resp.text))
    tickers = df['Symbol'].str.replace('.', '-').tolist()
    return tickers


def deduplicate_tickers(tickers: list[str], exclusions: list[str]) -> list[str]:
    """
    Remove duplicate share classes based on exclusions list.
    
    Args:
        tickers: List of all tickers
        exclusions: List of tickers to exclude (less liquid duplicates)
        
    Returns:
        Deduplicated list of tickers
    """
    return [t for t in tickers if t not in exclusions]


def get_spx_tickers(exclusions: Optional[list[str]] = None) -> list[str]:
    """
    Get S&P 500 tickers with fallback sources and deduplication.
    
    Args:
        exclusions: List of tickers to exclude (e.g., less liquid duplicates)
        
    Returns:
        List of unique S&P 500 ticker symbols
    """
    exclusions = exclusions or []
    
    # Try Wikipedia first
    try:
        print("📊 Fetching S&P 500 tickers from Wikipedia...", file=sys.stderr)
        tickers = fetch_from_wikipedia()
        print(f"✅ Fetched {len(tickers)} tickers from Wikipedia", file=sys.stderr)
    except Exception as e:
        print(f"⚠️  Wikipedia fetch failed: {e}", file=sys.stderr)
        
        # Fallback to GitHub
        try:
            print("📊 Fetching S&P 500 tickers from GitHub dataset...", file=sys.stderr)
            tickers = fetch_from_github()
            print(f"✅ Fetched {len(tickers)} tickers from GitHub", file=sys.stderr)
        except Exception as e2:
            print(f"⚠️  GitHub fetch failed: {e2}", file=sys.stderr)
            print("📊 Using fallback hardcoded ticker list...", file=sys.stderr)
            tickers = FALLBACK_TICKERS.copy()
            print(f"✅ Using {len(tickers)} hardcoded tickers", file=sys.stderr)
    
    # Deduplicate
    if exclusions:
        original_count = len(tickers)
        tickers = deduplicate_tickers(tickers, exclusions)
        removed = original_count - len(tickers)
        if removed > 0:
            print(f"🔍 Removed {removed} duplicate ticker(s): {exclusions}", file=sys.stderr)
    
    return sorted(tickers)


if __name__ == "__main__":
    # Test the function
    tickers = get_spx_tickers()
    print(f"\n📋 Total S&P 500 tickers: {len(tickers)}")
    print(f"First 10: {tickers[:10]}")
    print(f"Last 10: {tickers[-10:]}")
