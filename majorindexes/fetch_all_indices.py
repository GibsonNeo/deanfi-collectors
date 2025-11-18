"""
Fetch All Major Indices Data

Orchestrator script that runs all individual index fetchers in sequence.
Fetches 98+ indices across 9 categories with error handling and progress tracking.

Usage:
    python fetch_all_indices.py
"""

import os
import sys
from datetime import datetime
import importlib.util

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# Define all fetcher modules in execution order
FETCHERS = [
    ("fetch_us_major", "US Major Indices"),
    ("fetch_growth_value", "US Growth/Value Indices"),
    ("fetch_equal_weight", "US Equal-Weight Indices"),
    ("fetch_sectors", "US Sector Indices"),
    ("fetch_international", "International Indices"),
    ("fetch_emerging", "Emerging Markets"),
    ("fetch_dow_family", "Dow Family Indices"),
    ("fetch_bonds", "Bond/Treasury Indices"),
    ("fetch_commodities", "Commodity Indices"),
    ("fetch_currency", "Currency Index")
]


def run_fetcher(module_name: str, display_name: str) -> bool:
    """
    Run a single fetcher module.
    
    Args:
        module_name: Python module name (without .py)
        display_name: Human-readable name for logging
    
    Returns:
        True if successful, False if error
    """
    try:
        print(f"\n{'='*80}")
        print(f"Running: {display_name}")
        print(f"{'='*80}")
        
        # Import module
        module_path = os.path.join(SCRIPT_DIR, f"{module_name}.py")
        spec = importlib.util.spec_from_file_location(module_name, module_path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        
        # Run main function
        module.main()
        
        print(f"\n✅ {display_name} completed successfully")
        return True
        
    except Exception as e:
        print(f"\n❌ ERROR in {display_name}: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """
    Main orchestrator function.
    """
    start_time = datetime.now()
    
    print("=" * 80)
    print("MAJOR INDICES DATA FETCH - ALL CATEGORIES")
    print("=" * 80)
    print(f"Start Time: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Total Categories: {len(FETCHERS)}")
    print(f"Script Directory: {SCRIPT_DIR}")
    print("=" * 80)
    
    results = {}
    successful = 0
    failed = 0
    
    # Run each fetcher
    for module_name, display_name in FETCHERS:
        success = run_fetcher(module_name, display_name)
        results[display_name] = "✅ Success" if success else "❌ Failed"
        
        if success:
            successful += 1
        else:
            failed += 1
    
    # Summary
    end_time = datetime.now()
    duration = end_time - start_time
    
    print("\n" + "=" * 80)
    print("EXECUTION SUMMARY")
    print("=" * 80)
    print(f"Total Categories: {len(FETCHERS)}")
    print(f"Successful: {successful}")
    print(f"Failed: {failed}")
    print(f"Success Rate: {(successful/len(FETCHERS)*100):.1f}%")
    print(f"Duration: {duration}")
    print("=" * 80)
    
    print("\nDetailed Results:")
    for name, status in results.items():
        print(f"  {status} - {name}")
    
    print("\n" + "=" * 80)
    if failed == 0:
        print("✅ ALL FETCHERS COMPLETED SUCCESSFULLY")
    else:
        print(f"⚠️  COMPLETED WITH {failed} ERROR(S)")
    print("=" * 80)
    
    # List output files
    print("\nGenerated Files:")
    json_files = [f for f in os.listdir(SCRIPT_DIR) if f.endswith('.json')]
    json_files.sort()
    for f in json_files:
        file_path = os.path.join(SCRIPT_DIR, f)
        file_size = os.path.getsize(file_path) / 1024  # KB
        print(f"  {f} ({file_size:.1f} KB)")
    
    print(f"\nTotal JSON Files: {len(json_files)}")
    print("=" * 80)
    
    return failed == 0


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
