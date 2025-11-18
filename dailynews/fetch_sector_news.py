"""
Production script to fetch sector-based news using top stocks from each GICS sector.

Strategy:
- Uses top 5 stocks per sector (55 stocks total)
- Fetches news for each stock
- Groups by sector
- Limits to 9 articles per sector, max 3 per ticker
- Filters for premium sources: Bloomberg → CNBC → MarketWatch

Output: dailynews/sector_news.json
Runtime: ~1-2 minutes (60 API calls/minute limit)
"""
import yaml
import json
from pathlib import Path
from datetime import datetime
from collections import defaultdict

from finnhub_client import FinnhubClient, get_date_range


# GICS Sector mapping with top 5 stocks per sector
SECTOR_TICKERS = {
    'Information Technology': ['MSFT', 'AAPL', 'NVDA', 'AVGO', 'ORCL'],
    'Financials': ['JPM', 'BRK.B', 'BAC', 'WFC', 'C'],
    'Communication Services': ['GOOGL', 'META', 'T', 'NFLX', 'DIS'],
    'Consumer Discretionary': ['AMZN', 'TSLA', 'HD', 'MCD', 'NKE'],
    'Health Care': ['UNH', 'JNJ', 'PFE', 'MRK', 'ABBV', 'LLY'],
    'Industrials': ['BA', 'CAT', 'UNP', 'LMT', 'GE'],
    'Consumer Staples': ['PG', 'KO', 'WMT', 'COST', 'PEP'],
    'Energy': ['XOM', 'CVX', 'COP', 'SLB', 'EOG'],
    'Utilities': ['NEE', 'DUK', 'SO', 'SRE', 'AEP'],
    'Real Estate': ['PLD', 'AMT', 'SPG', 'DLR', 'EQIX'],
    'Materials': ['LIN', 'SHW', 'FCX', 'ECL', 'APD']
}

# Map to SPDR ETF tickers
SECTOR_TO_ETF = {
    'Information Technology': 'XLK',
    'Financials': 'XLF',
    'Communication Services': 'XLC',
    'Consumer Discretionary': 'XLY',
    'Health Care': 'XLV',
    'Industrials': 'XLI',
    'Consumer Staples': 'XLP',
    'Energy': 'XLE',
    'Utilities': 'XLU',
    'Real Estate': 'XLRE',
    'Materials': 'XLB'
}


def fetch_sector_news(config: dict, max_per_sector: int = 9, max_per_ticker: int = 3) -> dict:
    """
    Fetch news for top stocks and group by sector.
    
    Args:
        config: Configuration dictionary
        max_per_sector: Maximum articles per sector (default: 9)
        max_per_ticker: Maximum articles per ticker (default: 3)
        
    Returns:
        Dictionary mapping sector -> list of ranked articles
    """
    # Initialize client
    import os
    api_key = os.getenv('FINNHUB_API_KEY')
    if not api_key:
        api_key = config['api'].get('finnhub_api_key')
    if not api_key:
        raise ValueError("FINNHUB_API_KEY environment variable not set")
    
    client = FinnhubClient(api_key=api_key)
    
    # Get date range
    lookback_days = config['news']['lookback_days']
    from_date, to_date = get_date_range(lookback_days)
    
    # Flatten ticker list
    all_tickers = []
    ticker_to_sector = {}
    for sector, tickers in SECTOR_TICKERS.items():
        all_tickers.extend(tickers)
        for ticker in tickers:
            ticker_to_sector[ticker] = sector
    
    print(f"Fetching news for {len(all_tickers)} stocks across {len(SECTOR_TICKERS)} sectors...")
    print(f"Date range: {from_date} to {to_date}\n")
    
    # Fetch news for all tickers
    ticker_news = client.get_company_news_batch(all_tickers, from_date, to_date)
    
    # Define source priority (1 = highest)
    source_priority = {
        'Bloomberg': 1,
        'CNBC': 2,
        'MarketWatch': 3
    }
    
    # Sort function for articles: source priority first, then most recent
    def sort_key(article):
        source = article.get('source', 'Unknown')
        priority = source_priority.get(source, 999)
        timestamp = article.get('datetime', 0)
        # Negative timestamp for descending order (newest first)
        return (priority, -timestamp)
    
    # Group articles by sector with ticker limits
    sector_articles = defaultdict(list)
    
    for sector, tickers in SECTOR_TICKERS.items():
        sector_news = []
        
        for ticker in tickers:
            articles = ticker_news.get(ticker, [])
            
            # Filter to only priority sources and add ticker
            priority_articles = [
                {**article, 'ticker': ticker}
                for article in articles 
                if article.get('source') in source_priority
            ]
            
            # Sort and limit per ticker
            priority_articles.sort(key=sort_key)
            sector_news.extend(priority_articles[:max_per_ticker])
        
        # Sort all sector articles by priority
        sector_news.sort(key=sort_key)
        
        # Take top N per sector
        sector_articles[sector] = sector_news[:max_per_sector]
    
    # Print results
    print("Sector news summary:")
    total_articles = 0
    for sector, articles in sector_articles.items():
        etf = SECTOR_TO_ETF.get(sector, '???')
        
        # Count by source
        source_counts = {}
        ticker_counts = {}
        for article in articles:
            source = article.get('source', 'Unknown')
            ticker = article.get('ticker', 'Unknown')
            source_counts[source] = source_counts.get(source, 0) + 1
            ticker_counts[ticker] = ticker_counts.get(ticker, 0) + 1
        
        sources_str = ", ".join([f"{src}: {cnt}" for src, cnt in sorted(source_counts.items())])
        tickers_str = ", ".join([f"{tkr}: {cnt}" for tkr, cnt in sorted(ticker_counts.items(), key=lambda x: -x[1])[:3]])
        
        print(f"  {etf:5s} ({sector:25s}): {len(articles):2d} articles")
        print(f"        Sources: {sources_str}")
        print(f"        Top tickers: {tickers_str}")
        
        total_articles += len(articles)
    
    print(f"\n✓ Total articles: {total_articles}")
    
    return sector_articles


def format_output(sector_news: dict, config: dict) -> dict:
    """Format sector news data for JSON output."""
    from_date, to_date = get_date_range(config['news']['lookback_days'])
    
    # Calculate total and count by source
    total_articles = sum(len(articles) for articles in sector_news.values())
    all_sources = {}
    
    for articles in sector_news.values():
        for article in articles:
            source = article.get('source', 'Unknown')
            all_sources[source] = all_sources.get(source, 0) + 1
    
    output = {
        "metadata": {
            "generated_at": datetime.now().isoformat(),
            "type": "sector_news",
            "date_range": {
                "from": from_date,
                "to": to_date,
                "lookback_days": config['news']['lookback_days']
            },
            "source_priority": ["Bloomberg", "CNBC", "MarketWatch"],
            "max_per_sector": 9,
            "max_per_ticker": 3,
            "method": "Top 5 stocks per GICS sector",
            "stats": {
                "total_sectors": len(sector_news),
                "total_articles": total_articles,
                "total_tickers": sum(len(tickers) for tickers in SECTOR_TICKERS.values()),
                "by_source": all_sources
            }
        },
        "sectors": {}
    }
    
    # Format articles for each sector
    for sector, articles in sector_news.items():
        etf_ticker = SECTOR_TO_ETF.get(sector, sector)
        output["sectors"][etf_ticker] = {
            "sector_name": sector,
            "articles": []
        }
        
        for article in articles:
            formatted = {
                "headline": article.get('headline'),
                "summary": article.get('summary'),
                "source": article.get('source'),
                "ticker": article.get('ticker'),
                "url": article.get('url'),
                "category": article.get('category'),
                "datetime": datetime.fromtimestamp(article.get('datetime', 0)).isoformat(),
                "timestamp": article.get('datetime'),
                "id": article.get('id')
            }
            output["sectors"][etf_ticker]["articles"].append(formatted)
    
    return output


def main():
    """Main entry point."""
    # Load config
    config_path = Path(__file__).parent / 'config.yml'
    
    with open(config_path) as f:
        config = yaml.safe_load(f)
    
    print("="*70)
    print("FETCH SECTOR NEWS (Production)")
    print("Priority: Bloomberg → CNBC → MarketWatch")
    print("Max: 9 articles per sector, 3 per ticker")
    print("Stocks: Top 5 per sector (55 total)")
    print("="*70)
    print()
    
    # Fetch and rank sector news
    sector_news = fetch_sector_news(config, max_per_sector=9, max_per_ticker=3)
    
    # Format output
    output_data = format_output(sector_news, config)
    
    # Save to dailynews directory
    output_file = Path(__file__).parent / 'sector_news.json'
    with open(output_file, 'w') as f:
        json.dump(output_data, f, indent=2)
    
    print(f"\n✓ Saved to: {output_file}")
    print("\n" + "="*70)


if __name__ == "__main__":
    main()
