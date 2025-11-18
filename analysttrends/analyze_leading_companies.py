"""
Analyze recommendation trends for leading companies in each sector.

This script identifies the top companies in each GICS sector and extracts
their recommendation trend data, providing a focused view of how analyst
sentiment is changing for market leaders.
"""

import json
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Any

# Leading companies by sector (5-6 per sector)
SECTOR_LEADING_COMPANIES = {
    'XLK': ['AAPL', 'MSFT', 'NVDA', 'AVGO', 'CRM', 'ORCL'],
    'XLV': ['LLY', 'UNH', 'JNJ', 'ABBV', 'MRK', 'TMO'],
    'XLF': ['BRK-B', 'JPM', 'V', 'MA', 'BAC', 'WFC'],
    'XLY': ['AMZN', 'TSLA', 'HD', 'MCD', 'NKE', 'SBUX'],
    'XLC': ['META', 'GOOGL', 'NFLX', 'DIS', 'CMCSA', 'T'],
    'XLI': ['GE', 'CAT', 'RTX', 'UNP', 'HON', 'BA'],
    'XLP': ['PG', 'KO', 'PEP', 'COST', 'WMT', 'PM'],
    'XLE': ['XOM', 'CVX', 'COP', 'EOG', 'SLB', 'MPC'],
    'XLU': ['NEE', 'SO', 'DUK', 'CEG', 'SRE', 'AEP'],
    'XLRE': ['PLD', 'AMT', 'CCI', 'EQIX', 'PSA', 'O'],
    'XLB': ['LIN', 'NEM', 'SHW', 'APD', 'ECL', 'FCX']
}

SECTOR_NAMES = {
    'XLK': 'Information Technology',
    'XLV': 'Health Care',
    'XLF': 'Financials',
    'XLY': 'Consumer Discretionary',
    'XLC': 'Communication Services',
    'XLI': 'Industrials',
    'XLP': 'Consumer Staples',
    'XLE': 'Energy',
    'XLU': 'Utilities',
    'XLRE': 'Real Estate',
    'XLB': 'Materials'
}


def load_recommendation_data() -> Dict[str, Any]:
    """Load the recommendation trends data."""
    input_file = Path(__file__).parent / 'recommendation_trends.json'
    
    print("📂 Loading data from recommendation_trends.json...")
    with open(input_file, 'r') as f:
        data = json.load(f)
    
    print(f"✅ Loaded {len(data['data'])} tickers")
    return data


def calculate_ticker_change(periods: List[Dict]) -> Dict[str, float]:
    """Calculate the change in bullish ratio for a ticker."""
    if not periods or len(periods) < 2:
        return {
            'bullish_ratio_change': 0.0,
            'latest_bullish_ratio': 0.0,
            'latest_total': 0
        }
    
    latest = periods[0]
    oldest = periods[-1]
    
    latest_ratio = latest.get('bullish_ratio', 0.0)
    oldest_ratio = oldest.get('bullish_ratio', 0.0)
    
    return {
        'bullish_ratio_change': latest_ratio - oldest_ratio,
        'latest_bullish_ratio': latest_ratio,
        'latest_total': latest.get('total_recommendations', 0)
    }


def extract_leading_companies(data: Dict[str, Any]) -> Dict[str, List[Dict]]:
    """Extract leading companies data for each sector."""
    
    print("\n🏢 Extracting leading companies data...")
    
    tickers_data = data['data']
    leading_by_sector = {}
    
    for sector, tickers in SECTOR_LEADING_COMPANIES.items():
        sector_companies = []
        
        for ticker in tickers:
            if ticker in tickers_data:
                periods = tickers_data[ticker]
                
                if periods:
                    changes = calculate_ticker_change(periods)
                    
                    company_data = {
                        'ticker': ticker,
                        'periods': periods,
                        'change_analysis': changes
                    }
                    sector_companies.append(company_data)
        
        if sector_companies:
            # Sort by bullish ratio change (most bullish first)
            sector_companies.sort(
                key=lambda x: x['change_analysis']['bullish_ratio_change'],
                reverse=True
            )
            leading_by_sector[sector] = sector_companies
    
    print(f"✅ Extracted data for {len(leading_by_sector)} sectors")
    return leading_by_sector


def print_summary(leading_by_sector: Dict[str, List[Dict]]) -> None:
    """Print summary of leading companies by sector."""
    
    print("\n" + "=" * 70)
    print("LEADING COMPANIES BY SECTOR - RECOMMENDATION TRENDS")
    print("=" * 70)
    
    for sector in ['XLK', 'XLV', 'XLF', 'XLY', 'XLC', 'XLI', 'XLP', 'XLE', 'XLU', 'XLRE', 'XLB']:
        if sector not in leading_by_sector:
            continue
        
        companies = leading_by_sector[sector]
        sector_name = SECTOR_NAMES[sector]
        
        print(f"\n{sector} - {sector_name}")
        print("-" * 70)
        
        for i, company in enumerate(companies, 1):
            ticker = company['ticker']
            change = company['change_analysis']['bullish_ratio_change']
            latest_ratio = company['change_analysis']['latest_bullish_ratio']
            total = company['change_analysis']['latest_total']
            
            sign = '+' if change >= 0 else ''
            arrow = '→'
            
            print(f"  {i}. {ticker:6s} {sign}{change:6.2f}% {arrow} {latest_ratio:5.1f}% bullish ({total:2d} analysts)")
    
    print("\n" + "=" * 70)


def format_output(leading_by_sector: Dict[str, List[Dict]], metadata: Dict) -> Dict[str, Any]:
    """Format the output data structure."""
    
    sectors_output = {}
    
    for sector, companies in leading_by_sector.items():
        sectors_output[sector] = {
            'sector_name': SECTOR_NAMES[sector],
            'etf_ticker': sector,
            'num_companies': len(companies),
            'companies': [
                {
                    'ticker': c['ticker'],
                    'periods': c['periods'],
                    'latest_bullish_ratio': c['change_analysis']['latest_bullish_ratio'],
                    'bullish_ratio_change': c['change_analysis']['bullish_ratio_change'],
                    'latest_total_analysts': c['change_analysis']['latest_total']
                }
                for c in companies
            ]
        }
    
    return {
        '_README': {
            'description': 'Analyst Recommendation Trends for Market-Leading Companies in Each Sector',
            'usage': 'This file focuses on 5-6 top companies per sector (by market cap) to show how analyst sentiment is changing for market leaders',
            'navigation': {
                'metadata': 'Information about when and how this data was generated',
                'field_descriptions': 'Definitions of all data fields used for each company',
                'sectors': 'Main data section - contains all 11 sectors, each with their leading companies'
            },
            'companies_included': 'Each sector shows 5-6 companies sorted by bullish_ratio_change (most improving sentiment first)',
            'interpretation': 'Positive bullish_ratio_change = analysts becoming more bullish on that company. Negative = becoming more bearish.'
        },
        'metadata': {
            'generated_at': datetime.utcnow().isoformat() + 'Z',
            'source': 'Filtered from recommendation_trends.json',
            'description': 'Leading companies by sector with recommendation trend analysis',
            'num_sectors': len(sectors_output),
            'original_metadata': metadata
        },
        'field_descriptions': {
            'ticker': 'Stock ticker symbol',
            'periods': 'Array of recommendation data for each time period',
            'latest_bullish_ratio': 'Most recent percentage of bullish recommendations',
            'bullish_ratio_change': 'Change in bullish ratio from oldest to latest period',
            'latest_total_analysts': 'Number of analysts covering the stock in latest period'
        },
        'sectors': sectors_output
    }


def main():
    """Main execution function."""
    
    print("=" * 70)
    print("ANALYZE LEADING COMPANIES BY SECTOR")
    print("=" * 70)
    print()
    
    # Load data
    data = load_recommendation_data()
    
    # Extract leading companies
    leading_by_sector = extract_leading_companies(data)
    
    # Print summary
    print_summary(leading_by_sector)
    
    # Format output
    output_data = format_output(leading_by_sector, data['metadata'])
    
    # Save to file
    output_file = Path(__file__).parent / 'leading_companies_by_sector.json'
    print(f"\n💾 Writing to {output_file.name}...")
    
    with open(output_file, 'w') as f:
        json.dump(output_data, f, indent=2)
    
    print(f"✅ Saved to {output_file}")
    print("\n" + "=" * 70)


if __name__ == '__main__':
    main()
