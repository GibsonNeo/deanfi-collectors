#!/usr/bin/env python3
"""
Leading Companies Earnings Surprises Analyzer
=============================================
Analyzes earnings surprises for market-leading companies in each sector.

Features:
- Tracks 5-6 key companies per sector (market leaders)
- Full earnings surprise history (last 4 quarters)
- Beat rate, trends, and consistency metrics
- Sector-level insights from biggest players

Output: leading_companies_by_sector.json
"""

import json
import yaml
from pathlib import Path
from datetime import datetime
from statistics import mean, stdev


# Leading companies by sector (same as analysttrends)
SECTOR_LEADING_COMPANIES = {
    "Technology": ["AAPL", "MSFT", "NVDA", "AVGO", "ORCL", "ADBE"],
    "Healthcare": ["LLY", "UNH", "JNJ", "ABBV", "MRK", "TMO"],
    "Financials": ["BRK-B", "JPM", "V", "MA", "BAC", "WFC"],
    "Consumer Discretionary": ["AMZN", "TSLA", "HD", "MCD", "NKE", "SBUX"],
    "Communication Services": ["GOOGL", "META", "NFLX", "DIS", "CMCSA"],
    "Industrials": ["GE", "CAT", "RTX", "UNP", "HON", "UPS"],
    "Consumer Staples": ["WMT", "PG", "KO", "PEP", "COST", "MDLZ"],
    "Energy": ["XOM", "CVX", "COP", "SLB", "EOG", "PXD"],
    "Utilities": ["NEE", "DUK", "SO", "D", "AEP", "SRE"],
    "Real Estate": ["PLD", "AMT", "EQIX", "PSA", "WELL", "SPG"],
    "Materials": ["LIN", "APD", "SHW", "FCX", "NEM", "DD"]
}


class LeadingCompaniesAnalyzer:
    """Analyzes earnings surprises for sector-leading companies."""
    
    def __init__(self, config_path="config.yml"):
        """Initialize analyzer with configuration."""
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        self.thresholds = self.config['surprise_classification']
    
    def load_earnings_data(self, input_file):
        """Load raw earnings data from JSON file."""
        with open(input_file, 'r') as f:
            data = json.load(f)
        return data['data']
    
    def classify_surprise(self, surprise_percent):
        """Classify earnings surprise into category."""
        if surprise_percent is None:
            return "unknown"
        
        if surprise_percent > self.thresholds['strong_beat_threshold']:
            return "strong_beat"
        elif surprise_percent > self.thresholds['beat_threshold']:
            return "beat"
        elif abs(surprise_percent) <= self.thresholds['meet_threshold']:
            return "meet"
        elif surprise_percent >= self.thresholds['miss_threshold']:
            return "miss"
        else:
            return "strong_miss"
    
    def calculate_beat_streak(self, quarters_data):
        """Calculate current beat streak."""
        sorted_quarters = sorted(quarters_data, key=lambda x: x.get('period', ''), reverse=True)
        
        if not sorted_quarters:
            return 0
        
        streak = 0
        last_category = None
        
        for q in sorted_quarters:
            surprise_pct = q.get('surprisePercent')
            if surprise_pct is None:
                break
            
            category = self.classify_surprise(surprise_pct)
            
            if category in ['strong_beat', 'beat']:
                if last_category is None or last_category in ['strong_beat', 'beat']:
                    streak += 1
                    last_category = 'beat'
                else:
                    break
            elif category in ['miss', 'strong_miss']:
                if last_category is None or last_category in ['miss', 'strong_miss']:
                    streak -= 1
                    last_category = 'miss'
                else:
                    break
            else:
                break
        
        return streak
    
    def calculate_trend_direction(self, quarters_data):
        """Calculate trend direction."""
        if len(quarters_data) < 2:
            return "insufficient_data"
        
        sorted_quarters = sorted(quarters_data, key=lambda x: x.get('period', ''))
        surprises = [q.get('surprisePercent') for q in sorted_quarters if q.get('surprisePercent') is not None]
        
        if len(surprises) < 2:
            return "insufficient_data"
        
        mid = len(surprises) // 2
        older_avg = mean(surprises[:mid])
        recent_avg = mean(surprises[mid:])
        
        diff = recent_avg - older_avg
        
        if diff > 2.0:
            return "improving"
        elif diff < -2.0:
            return "declining"
        else:
            return "stable"
    
    def analyze_company(self, ticker, quarters_data):
        """Analyze a single company's earnings surprises."""
        if not quarters_data:
            return None
        
        surprises = [q.get('surprisePercent') for q in quarters_data if q.get('surprisePercent') is not None]
        
        if not surprises:
            return None
        
        # Calculate metrics
        avg_surprise = mean(surprises)
        beats = sum(1 for s in surprises if s > self.thresholds['beat_threshold'])
        beat_rate = (beats / len(surprises)) * 100
        consistency_score = stdev(surprises) if len(surprises) > 1 else 0
        beat_streak = self.calculate_beat_streak(quarters_data)
        trend = self.calculate_trend_direction(quarters_data)
        
        # Get most recent quarter
        most_recent = max(quarters_data, key=lambda x: x.get('period', ''))
        
        # Category breakdown
        categories = [self.classify_surprise(s) for s in surprises]
        category_counts = {
            'strong_beat': categories.count('strong_beat'),
            'beat': categories.count('beat'),
            'meet': categories.count('meet'),
            'miss': categories.count('miss'),
            'strong_miss': categories.count('strong_miss')
        }
        
        return {
            'ticker': ticker,
            'quarters_analyzed': len(quarters_data),
            'average_surprise_percent': round(avg_surprise, 2),
            'beat_rate': round(beat_rate, 1),
            'beat_streak': beat_streak,
            'trend_direction': trend,
            'consistency_score': round(consistency_score, 2),
            'most_recent_quarter': {
                'period': most_recent.get('period'),
                'quarter': most_recent.get('quarter'),
                'year': most_recent.get('year'),
                'actual': most_recent.get('actual'),
                'estimate': most_recent.get('estimate'),
                'surprise': most_recent.get('surprise'),
                'surprise_percent': most_recent.get('surprisePercent'),
                'category': self.classify_surprise(most_recent.get('surprisePercent'))
            },
            'category_breakdown': category_counts,
            'quarterly_history': [
                {
                    'period': q.get('period'),
                    'quarter': f"Q{q.get('quarter')}",
                    'year': q.get('year'),
                    'actual': q.get('actual'),
                    'estimate': q.get('estimate'),
                    'surprise': q.get('surprise'),
                    'surprise_percent': q.get('surprisePercent'),
                    'category': self.classify_surprise(q.get('surprisePercent'))
                }
                for q in sorted(quarters_data, key=lambda x: x.get('period', ''), reverse=True)
            ]
        }
    
    def extract_leading_companies(self, earnings_data):
        """Extract and analyze leading companies by sector."""
        sectors_data = {}
        
        for sector, companies in SECTOR_LEADING_COMPANIES.items():
            sector_companies = []
            
            for ticker in companies:
                if ticker in earnings_data:
                    analysis = self.analyze_company(ticker, earnings_data[ticker])
                    if analysis:
                        sector_companies.append(analysis)
            
            if sector_companies:
                # Sort by average surprise percent (descending)
                sector_companies.sort(key=lambda x: x['average_surprise_percent'], reverse=True)
                
                # Calculate sector-level statistics
                avg_surprises = [c['average_surprise_percent'] for c in sector_companies]
                beat_rates = [c['beat_rate'] for c in sector_companies]
                
                sectors_data[sector] = {
                    'sector_statistics': {
                        'total_companies': len(sector_companies),
                        'average_surprise_percent': round(mean(avg_surprises), 2),
                        'average_beat_rate': round(mean(beat_rates), 1),
                        'best_performer': max(avg_surprises),
                        'worst_performer': min(avg_surprises)
                    },
                    'companies': sector_companies
                }
        
        return sectors_data
    
    def print_summary(self, sectors_data):
        """Print summary of leading companies analysis."""
        print("\n" + "=" * 70)
        print("LEADING COMPANIES BY SECTOR")
        print("=" * 70)
        
        for sector, data in sorted(sectors_data.items()):
            stats = data['sector_statistics']
            print(f"\n{sector}")
            print(f"  Sector Avg Surprise: {stats['average_surprise_percent']:+.2f}%")
            print(f"  Sector Beat Rate: {stats['average_beat_rate']:.1f}%")
            print(f"  Companies analyzed: {stats['total_companies']}")
            
            print(f"  Top 3 performers:")
            for i, company in enumerate(data['companies'][:3], 1):
                print(f"    {i}. {company['ticker']:6s} - Avg: {company['average_surprise_percent']:+6.2f}% | "
                      f"Beat Rate: {company['beat_rate']:5.1f}% | Streak: {company['beat_streak']:+2d}")
        
        print("=" * 70)
    
    def save_to_json(self, sectors_data, output_file):
        """Save leading companies analysis to JSON with README."""
        
        readme = {
            "_README": {
                "description": "Earnings surprises for market-leading companies grouped by sector",
                "purpose": "Track earnings performance of the biggest players in each sector",
                
                "company_selection": "5-6 largest/most influential companies per sector based on market cap and sector representation",
                
                "data_organization": {
                    "sector_statistics": "Aggregated metrics across all leading companies in the sector",
                    "companies": "Individual company data sorted by average_surprise_percent (best to worst)"
                },
                
                "sector_statistics_explained": {
                    "total_companies": "Number of leading companies analyzed in this sector",
                    "average_surprise_percent": "Mean surprise % across all sector leaders",
                    "average_beat_rate": "Mean beat rate across all sector leaders",
                    "best_performer": "Highest average surprise % in the sector",
                    "worst_performer": "Lowest average surprise % in the sector"
                },
                
                "company_metrics_explained": {
                    "ticker": "Stock ticker symbol",
                    "quarters_analyzed": "Number of quarters included (typically 4)",
                    "average_surprise_percent": "Mean earnings surprise % across all quarters",
                    "beat_rate": "Percentage of quarters beating estimates",
                    "beat_streak": "Consecutive quarters beating (positive) or missing (negative)",
                    "trend_direction": "improving/declining/stable based on recent vs older performance",
                    "consistency_score": "Standard deviation (lower = more predictable)",
                    "most_recent_quarter": "Latest reported quarter details",
                    "category_breakdown": "Count of quarters in each surprise category",
                    "quarterly_history": "Full quarter-by-quarter history (most recent first)"
                },
                
                "usage_examples": {
                    "sector_comparison": "Compare sector_statistics.average_surprise_percent across sectors",
                    "finding_leaders": "Companies sorted within each sector by performance",
                    "consistency_check": "Low consistency_score + high beat_rate = reliable performer",
                    "momentum_tracking": "beat_streak shows current momentum direction",
                    "quarterly_trends": "Review quarterly_history to see improvement/decline patterns"
                },
                
                "interpretation_tips": {
                    "sector_divergence": "If sector avg is positive but some companies negative, check for competitive issues",
                    "consistent_beaters": "Look for beat_rate >75% AND consistency_score <5",
                    "turnaround_plays": "Companies with trend_direction='improving' despite low avg_surprise",
                    "risk_signals": "beat_streak <=-2 combined with trend_direction='declining' = caution"
                }
            }
        }
        
        output = {
            **readme,
            "metadata": {
                "data_source": "Finnhub Earnings Surprises API",
                "analysis_type": "Leading Companies by Sector",
                "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "total_sectors": len(sectors_data),
                "quarters_analyzed": 4
            },
            "surprise_classification": {
                "strong_beat": f"> {self.thresholds['strong_beat_threshold']}%",
                "beat": f"0% to {self.thresholds['strong_beat_threshold']}%",
                "meet": f"± {self.thresholds['meet_threshold']}%",
                "miss": f"{self.thresholds['miss_threshold']}% to 0%",
                "strong_miss": f"< {self.thresholds['miss_threshold']}%"
            },
            "sectors": sectors_data
        }
        
        with open(output_file, 'w') as f:
            json.dump(output, f, indent=2)
        
        file_size = Path(output_file).stat().st_size
        print(f"\n✓ Leading companies analysis saved to {output_file}")
        print(f"  File size: {file_size:,} bytes")
        print(f"  Sectors: {len(sectors_data)}")


def main():
    """Main execution function."""
    print("=" * 70)
    print("ANALYZING LEADING COMPANIES EARNINGS SURPRISES")
    print("=" * 70)
    
    analyzer = LeadingCompaniesAnalyzer()
    
    # Load raw earnings data
    input_file = analyzer.config['output']['raw_data']
    print(f"\nLoading data from {input_file}...")
    earnings_data = analyzer.load_earnings_data(input_file)
    print(f"✓ Loaded data for {len(earnings_data)} tickers")
    
    # Extract and analyze leading companies
    print("\nAnalyzing leading companies by sector...")
    sectors_data = analyzer.extract_leading_companies(earnings_data)
    
    # Print summary
    analyzer.print_summary(sectors_data)
    
    # Save to JSON
    output_file = analyzer.config['output']['leading_companies']
    analyzer.save_to_json(sectors_data, output_file)


if __name__ == "__main__":
    main()
