"""
Orchestrator to fetch all implied volatility data.

Runs all IV fetchers in sequence:
1. Sector ETFs (11 symbols)
2. Major Indices (4 symbols)
3. VIX Options (1 symbol)

Generates 6 JSON files total:
- sector_etfs_iv_snapshot.json
- sector_etfs_iv_historical.json
- major_indices_iv_snapshot.json
- major_indices_iv_historical.json
- vix_options_snapshot.json
- vix_options_historical.json
"""

import sys
import importlib
from datetime import datetime
from pathlib import Path
import time


# List of fetcher modules to run
FETCHERS = [
    {
        'module': 'fetch_sector_etfs_iv',
        'name': 'Sector ETFs IV',
        'description': '11 sector ETFs (XLK, XLV, XLF, etc.)'
    },
    {
        'module': 'fetch_major_indices_iv',
        'name': 'Major Indices IV',
        'description': '4 major index ETFs (SPY, QQQ, IWM, DIA)'
    },
    {
        'module': 'fetch_vix_options',
        'name': 'VIX Options IV',
        'description': 'VIX volatility of volatility'
    }
]


def run_fetcher(fetcher_info: dict) -> dict:
    """
    Run a single fetcher module.
    
    Args:
        fetcher_info: Dict with module, name, description
    
    Returns:
        Dict with success status, duration, error (if any)
    """
    module_name = fetcher_info['module']
    display_name = fetcher_info['name']
    
    print(f"\n{'='*80}")
    print(f"Running: {display_name}")
    print(f"Module: {module_name}.py")
    print(f"Description: {fetcher_info['description']}")
    print(f"{'='*80}\n")
    
    start_time = time.time()
    
    try:
        # Dynamically import and run the fetcher
        module = importlib.import_module(module_name)
        module.main()
        
        duration = time.time() - start_time
        
        return {
            'success': True,
            'duration': round(duration, 2),
            'error': None
        }
        
    except Exception as e:
        duration = time.time() - start_time
        
        print(f"\n❌ ERROR in {display_name}:")
        print(f"   {str(e)}")
        
        return {
            'success': False,
            'duration': round(duration, 2),
            'error': str(e)
        }


def main():
    """Main orchestrator function."""
    print("="*80)
    print("IMPLIED VOLATILITY DATA ORCHESTRATOR")
    print("="*80)
    print(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Fetchers to run: {len(FETCHERS)}")
    print()
    
    for i, fetcher in enumerate(FETCHERS, 1):
        print(f"{i}. {fetcher['name']} ({fetcher['description']})")
    
    print("\n" + "="*80)
    print("STARTING FETCH SEQUENCE")
    print("="*80)
    
    overall_start = time.time()
    results = []
    
    # Run each fetcher
    for fetcher in FETCHERS:
        result = run_fetcher(fetcher)
        results.append({
            'name': fetcher['name'],
            'module': fetcher['module'],
            **result
        })
        
        # Brief pause between fetchers to avoid rate limiting
        if result['success']:
            time.sleep(1)
    
    overall_duration = time.time() - overall_start
    
    # Summary report
    print("\n\n" + "="*80)
    print("FETCH SEQUENCE COMPLETE - SUMMARY")
    print("="*80)
    print(f"End time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Total duration: {overall_duration:.2f} seconds")
    print()
    
    successful = [r for r in results if r['success']]
    failed = [r for r in results if not r['success']]
    
    print(f"✅ Successful: {len(successful)}/{len(results)}")
    print(f"❌ Failed: {len(failed)}/{len(results)}")
    print()
    
    if successful:
        print("Successful fetchers:")
        for r in successful:
            print(f"  ✅ {r['name']:30s} - {r['duration']:.2f}s")
    
    if failed:
        print("\nFailed fetchers:")
        for r in failed:
            print(f"  ❌ {r['name']:30s} - {r['error']}")
    
    # List generated files
    print("\n" + "="*80)
    print("GENERATED FILES")
    print("="*80)
    
    output_dir = Path(__file__).parent
    json_files = sorted(output_dir.glob('*.json'))
    
    if json_files:
        total_size = 0
        for json_file in json_files:
            # Skip test output if exists
            if 'test' in json_file.name.lower():
                continue
            
            size = json_file.stat().st_size
            total_size += size
            size_kb = size / 1024
            
            print(f"  📄 {json_file.name:40s} - {size_kb:8.2f} KB")
        
        print(f"\n  Total size: {total_size/1024:.2f} KB ({len(json_files)} files)")
    else:
        print("  No JSON files found")
    
    # Exit code
    print("\n" + "="*80)
    if failed:
        print("⚠️  COMPLETED WITH ERRORS")
        print("="*80)
        sys.exit(1)
    else:
        print("✅ ALL FETCHERS COMPLETED SUCCESSFULLY")
        print("="*80)
        sys.exit(0)


if __name__ == "__main__":
    main()
