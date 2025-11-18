"""
Test script to explore yfinance options data availability.

Tests what option data is available for ETFs we track in majorindexes:
- Sector ETFs (XLK, XLV, XLF, etc.)
- Index ETFs (SPY, QQQ, IWM, DIA)
- VIX index (^VIX)

Goal: Determine if we can get enough data to calculate implied volatility.
"""

import yfinance as yf
import pandas as pd
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

# ETFs from our majorindexes tracking
TEST_SYMBOLS = [
    # Major index ETFs (liquid, widely traded)
    'SPY',   # S&P 500
    'QQQ',   # Nasdaq 100
    'IWM',   # Russell 2000
    'DIA',   # Dow Jones
    
    # Sector ETFs
    'XLK',   # Technology
    'XLV',   # Healthcare
    'XLF',   # Financials
    'XLE',   # Energy
    'XLY',   # Consumer Discretionary
    
    # VIX (volatility index - may not have options)
    '^VIX',
]

def test_options_availability(symbol):
    """
    Test what options data is available for a symbol.
    
    Returns dict with:
    - has_options: bool
    - expiration_dates: list
    - sample_chain: DataFrame or None
    - fields_available: list of column names
    """
    print(f"\n{'='*80}")
    print(f"Testing: {symbol}")
    print(f"{'='*80}")
    
    try:
        ticker = yf.Ticker(symbol)
        
        # Get current price
        hist = ticker.history(period="1d")
        if len(hist) > 0:
            current_price = hist['Close'].iloc[-1]
            print(f"Current Price: ${current_price:.2f}")
        else:
            print("⚠️  Could not get current price")
            current_price = None
        
        # Check if options are available
        try:
            exp_dates = ticker.options
            if not exp_dates:
                print("❌ No option expiration dates available")
                return {
                    'symbol': symbol,
                    'has_options': False,
                    'current_price': current_price,
                    'expiration_dates': [],
                    'sample_chain': None,
                    'fields_available': []
                }
            
            print(f"✅ Options available!")
            print(f"   Expiration dates: {len(exp_dates)}")
            print(f"   First expiry: {exp_dates[0]}")
            print(f"   Last expiry: {exp_dates[-1]}")
            
            # Get option chain for first expiration
            opt_chain = ticker.option_chain(exp_dates[0])
            calls = opt_chain.calls
            puts = opt_chain.puts
            
            print(f"\n   Calls: {len(calls)} contracts")
            print(f"   Puts: {len(puts)} contracts")
            
            # Check what fields are available
            print(f"\n   Available fields in calls DataFrame:")
            for col in calls.columns:
                print(f"     - {col}")
            
            # Check if impliedVolatility is present
            has_iv = 'impliedVolatility' in calls.columns
            print(f"\n   {'✅' if has_iv else '❌'} impliedVolatility field present: {has_iv}")
            
            # Show sample data
            if len(calls) > 0:
                print(f"\n   Sample call option (ATM or near):")
                # Find ATM option
                if current_price:
                    calls['strike_diff'] = abs(calls['strike'] - current_price)
                    atm_idx = calls['strike_diff'].idxmin()
                    sample = calls.loc[atm_idx]
                else:
                    sample = calls.iloc[len(calls)//2]
                
                print(f"     Strike: ${sample['strike']:.2f}")
                print(f"     Last Price: ${sample['lastPrice']:.2f}")
                print(f"     Bid: ${sample['bid']:.2f}")
                print(f"     Ask: ${sample['ask']:.2f}")
                print(f"     Volume: {sample['volume']}")
                print(f"     Open Interest: {sample['openInterest']}")
                if has_iv:
                    print(f"     Implied Vol: {sample['impliedVolatility']:.4f}")
                
                # Calculate mid price
                mid_price = (sample['bid'] + sample['ask']) / 2 if sample['bid'] > 0 and sample['ask'] > 0 else sample['lastPrice']
                print(f"     Mid Price: ${mid_price:.2f}")
            
            return {
                'symbol': symbol,
                'has_options': True,
                'current_price': current_price,
                'expiration_dates': exp_dates,
                'num_expirations': len(exp_dates),
                'calls_count': len(calls),
                'puts_count': len(puts),
                'fields_available': list(calls.columns),
                'has_iv_field': has_iv,
                'sample_call': calls.iloc[0].to_dict() if len(calls) > 0 else None
            }
            
        except Exception as e:
            print(f"❌ Error accessing options: {e}")
            return {
                'symbol': symbol,
                'has_options': False,
                'current_price': current_price,
                'error': str(e)
            }
            
    except Exception as e:
        print(f"❌ Error with ticker: {e}")
        return {
            'symbol': symbol,
            'has_options': False,
            'error': str(e)
        }

def main():
    print("="*80)
    print("YFINANCE OPTIONS DATA AVAILABILITY TEST")
    print("="*80)
    print(f"Testing {len(TEST_SYMBOLS)} symbols")
    print(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    results = []
    
    for symbol in TEST_SYMBOLS:
        result = test_options_availability(symbol)
        results.append(result)
    
    # Summary
    print(f"\n\n{'='*80}")
    print("SUMMARY")
    print(f"{'='*80}")
    
    symbols_with_options = [r for r in results if r.get('has_options', False)]
    symbols_with_iv = [r for r in symbols_with_options if r.get('has_iv_field', False)]
    
    print(f"\nTotal tested: {len(results)}")
    print(f"With options data: {len(symbols_with_options)}")
    print(f"With IV field: {len(symbols_with_iv)}")
    
    print(f"\n✅ Symbols WITH options and IV field:")
    for r in symbols_with_iv:
        print(f"   {r['symbol']:8s} - {r['num_expirations']} expirations, {r['calls_count']} calls, {r['puts_count']} puts")
    
    print(f"\n⚠️  Symbols WITH options but NO IV field:")
    for r in symbols_with_options:
        if not r.get('has_iv_field', False):
            print(f"   {r['symbol']:8s} - Need to calculate IV manually")
    
    print(f"\n❌ Symbols WITHOUT options:")
    for r in results:
        if not r.get('has_options', False):
            print(f"   {r['symbol']:8s}")
    
    # Check what fields are common
    if symbols_with_options:
        print(f"\n\nCommon fields across all options DataFrames:")
        all_fields = set(symbols_with_options[0].get('fields_available', []))
        for r in symbols_with_options[1:]:
            all_fields &= set(r.get('fields_available', []))
        
        for field in sorted(all_fields):
            print(f"   - {field}")
    
    print(f"\n{'='*80}")
    print("CONCLUSION")
    print(f"{'='*80}")
    
    if symbols_with_iv:
        print("✅ yfinance DOES provide impliedVolatility field for these symbols!")
        print("   We can use it directly without calculation.")
    
    if len(symbols_with_options) > len(symbols_with_iv):
        print("\n⚠️  Some symbols need IV calculation from price + Black-Scholes.")
        print("   We have bid/ask/strike/expiration data available.")
    
    print("\n💡 Recommendation:")
    print("   1. Use impliedVolatility field when available")
    print("   2. Calculate IV manually for symbols without it")
    print("   3. Focus on liquid ETFs (SPY, QQQ, sector ETFs)")

if __name__ == "__main__":
    main()
