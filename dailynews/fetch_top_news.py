"""
Production script to fetch top 100 market news articles.

Strategy:
- Fetches latest market news from Finnhub
- Prioritizes premium sources: Bloomberg → CNBC → MarketWatch
- Sorts by source priority then recency
- Returns top 100 articles

Output: dailynews/top_news.json
Runtime: ~1-2 seconds (single API call)
"""
import yaml
import json
import os
from pathlib import Path
from datetime import datetime

from finnhub_client import FinnhubClient, get_date_range


def fetch_and_rank_news(config: dict) -> list:
    """
    Fetch market news and rank by source priority from config.
    
    Args:
        config: Configuration dictionary with news.source_priority settings
        
    Returns:
        List of ranked news articles
    """
    # Get API key from environment variable (GitHub Actions secret)
    api_key = os.getenv('FINNHUB_API_KEY')
    if not api_key:
        # Fallback to config file for local development
        api_key = config['api'].get('finnhub_api_key')
        if not api_key or api_key.startswith('${'):
            raise ValueError("FINNHUB_API_KEY environment variable not set. "
                           "Set it in your shell or GitHub repository secrets.")
    
    # Initialize client
    client = FinnhubClient(api_key=api_key)
    
    # Get config settings
    max_articles = config['news'].get('max_articles', 100)
    source_priority_list = config['news'].get('source_priority', [])
    excluded_sources = config['news'].get('excluded_sources', [])
    
    # Build source priority dictionary from config
    source_priority = {}
    priority_source_names = []
    for item in source_priority_list:
        source = item['source']
        priority = item['priority']
        source_priority[source] = priority
        priority_source_names.append(source)
    
    print(f"Source priority: {' → '.join(priority_source_names)}")
    if excluded_sources:
        print(f"Excluded sources: {', '.join(excluded_sources)}")
    
    # Fetch market news
    print(f"\nFetching market news...")
    all_news = client.get_market_news(category='general')
    print(f"✓ Retrieved {len(all_news)} articles")
    
    # Filter out excluded sources
    if excluded_sources:
        all_news = [
            article for article in all_news 
            if article.get('source') not in excluded_sources
        ]
        print(f"✓ After exclusions: {len(all_news)} articles")
    
    # Filter to only include priority sources
    priority_news = [
        article for article in all_news 
        if article.get('source') in source_priority
    ]
    
    print(f"✓ Filtered to {len(priority_news)} articles from priority sources")
    
    # Sort by priority, then by datetime (newest first)
    def sort_key(article):
        source = article.get('source', 'Unknown')
        priority = source_priority.get(source, 999)
        timestamp = article.get('datetime', 0)
        return (priority, -timestamp)  # Negative timestamp for descending order
    
    priority_news.sort(key=sort_key)
    
    # Take top N articles
    ranked_news = priority_news[:max_articles]
    
    # Count by source
    source_counts = {}
    for article in ranked_news:
        source = article.get('source', 'Unknown')
        source_counts[source] = source_counts.get(source, 0) + 1
    
    print(f"\n✓ Selected {len(ranked_news)} articles (max: {max_articles}):")
    for source in priority_source_names:
        count = source_counts.get(source, 0)
        if count > 0:
            print(f"  {source:15s}: {count:3d} articles")
    
    # Show any other sources that made it through
    other_sources = set(source_counts.keys()) - set(priority_source_names)
    if other_sources:
        print(f"\n  Other sources:")
        for source in sorted(other_sources):
            print(f"  {source:15s}: {source_counts[source]:3d} articles")
    
    return ranked_news


def format_output(news: list, config: dict) -> dict:
    """Format news data for JSON output."""
    # Get source priority list for metadata
    source_priority_list = config['news'].get('source_priority', [])
    excluded_sources = config['news'].get('excluded_sources', [])
    max_articles = config['news'].get('max_articles', 100)
    
    priority_sources = [item['source'] for item in source_priority_list]
    
    output = {
        "metadata": {
            "generated_at": datetime.now().isoformat(),
            "type": "top_market_news",
            "source_priority": priority_sources,
            "excluded_sources": excluded_sources,
            "max_articles": max_articles,
            "actual_articles": len(news),
            "stats": {
                "total_articles": len(news)
            }
        },
        "articles": []
    }
    
    # Format articles
    for article in news:
        formatted = {
            "headline": article.get('headline'),
            "summary": article.get('summary'),
            "source": article.get('source'),
            "url": article.get('url'),
            "category": article.get('category'),
            "datetime": datetime.fromtimestamp(article.get('datetime', 0)).isoformat(),
            "timestamp": article.get('datetime'),
            "id": article.get('id')
        }
        output["articles"].append(formatted)
    
    return output


def main():
    """Main entry point."""
    # Load config
    config_path = Path(__file__).parent / 'config.yml'
    
    with open(config_path) as f:
        config = yaml.safe_load(f)
    
    # Get settings from config
    source_priority_list = config['news'].get('source_priority', [])
    max_articles = config['news'].get('max_articles', 100)
    priority_sources = [item['source'] for item in source_priority_list]
    
    print("="*70)
    print("FETCH TOP MARKET NEWS (Production)")
    print(f"Priority: {' → '.join(priority_sources)}")
    print(f"Max Articles: {max_articles}")
    print("="*70)
    print()
    
    # Fetch and rank news
    ranked_news = fetch_and_rank_news(config)
    
    # Format output
    output_data = format_output(ranked_news, config)
    
    # Save to dailynews directory
    output_file = Path(__file__).parent / 'top_news.json'
    with open(output_file, 'w') as f:
        json.dump(output_data, f, indent=2)
    
    print(f"\n✓ Saved to: {output_file}")
    print("\n" + "="*70)


if __name__ == "__main__":
    main()
