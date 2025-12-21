#!/usr/bin/env python3
"""
Export Housing & Affordability economic indicators to JSON.
Includes: Housing activity, prices, mortgage rates, debt service, and inflation expectations.
"""
import argparse
import sys
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

# Add shared to path for imports
SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent
SHARED_DIR = REPO_ROOT / "shared"
if str(SHARED_DIR) not in sys.path:
    sys.path.insert(0, str(SHARED_DIR))

from fred_client import FREDClient
from economy_indicators import get_indicators_by_category
from economy_compute import (
    calculate_percentile_rank,
    calculate_grade,
    calculate_overall_grade,
    calculate_trend,
    is_trend_favorable,
    calculate_change_metrics,
    adaptive_resample,
    sanitize_for_json,
)
from economy_io import load_config, save_json


def export_housing_affordability_json(output_path: str, config_path: str = None, override_history_days: int = None) -> dict:
    """Generate Housing & Affordability indicators JSON."""
    config = load_config(config_path)
    fred = FREDClient(rate_limit=config.get("fred", {}).get("rate_limit_seconds", 0.1))
    indicators = get_indicators_by_category("housing_affordability")

    if override_history_days:
        history_days = override_history_days
    else:
        history_days = 7300  # 20 years

    end_date = datetime.now()
    start_date = end_date - timedelta(days=history_days)
    start_date_str = start_date.strftime("%Y-%m-%d")
    end_date_str = end_date.strftime("%Y-%m-%d")

    print(f"Fetching Housing & Affordability data from {start_date_str} to {end_date_str}...")
    print("  (20-year lookback for robust percentile calculations)")

    indicator_data = {}
    for indicator in indicators:
        if indicator.is_derived:
            continue
        print(f"  Fetching {indicator.series_id} ({indicator.name})...")
        try:
            df = fred.get_series_range(indicator.series_id, start_date_str, end_date_str)
            indicator_data[indicator.series_id] = df
        except Exception as fetch_error:
            print(f"  Warning: Failed to fetch {indicator.series_id}: {fetch_error}")
            indicator_data[indicator.series_id] = None

    eastern = ZoneInfo("America/New_York")
    now = datetime.now(eastern)

    readme = {
        "title": "Housing & Affordability Dashboard",
        "description": "Housing activity, home prices, financing costs, and household affordability metrics from FRED.",
        "purpose": "Summarize housing supply/demand health and borrower cost pressure for macro monitoring and market commentary.",
        "metrics_explained": {
            "HOUST": {
                "description": "New privately-owned housing units started (construction pipeline).",
                "interpretation": "Higher starts signal growing supply; sustained drops can flag builder pullback."
            },
            "PERMIT": {
                "description": "Housing units authorized by permits (leading indicator for future starts).",
                "interpretation": "Permits tend to turn before starts; falling permits often precede slower construction."
            },
            "HSN1F": {
                "description": "New single-family houses sold (demand for new construction).",
                "interpretation": "Rising sales point to healthy builder demand; sharp drops can warn of demand softening."
            },
            "EXHOSLUSM495S": {
                "description": "Existing single-family home sales (resale market volume).",
                "interpretation": "Higher turnover suggests active resale demand; low volumes can reflect tight inventory or weak demand."
            },
            "CSUSHPINSA": {
                "description": "S&P/Case-Shiller U.S. National Home Price Index (broad home price level).",
                "interpretation": "Higher is not always better; fast price gains can worsen affordability while broad declines can signal stress."
            },
            "MORTGAGE30US": {
                "description": "Average 30-year fixed mortgage rate (weekly).",
                "interpretation": "Lower rates improve monthly payments and refinance ability; rapid increases pressure affordability."
            },
            "MORTGAGE15US": {
                "description": "Average 15-year fixed mortgage rate (weekly).",
                "interpretation": "Tracks shorter-term mortgage pricing; helpful for refinance affordability comparisons."
            },
            "TDSP": {
                "description": "Household debt service payments as a percent of disposable personal income.",
                "interpretation": "Lower ratios indicate more cushion for debt service; rising ratios tighten household cash flow."
            },
            "MDSP": {
                "description": "Mortgage debt service payments as a percent of disposable personal income.",
                "interpretation": "Direct affordability gauge for mortgage payments; rising values reduce affordability."
            },
            "T10YIE": {
                "description": "10-year breakeven inflation rate (market-based inflation expectations).",
                "interpretation": "Higher expectations can pressure mortgage rates and real affordability over time."
            }
        },
        "trading_applications": {
            "homebuilders": "Pair starts/permits/sales trends with mortgage rates to assess homebuilder earnings sensitivity.",
            "macro_risk": "Use debt service ratios and breakeven inflation as affordability stress markers in macro risk dashboards."
        },
        "notes": {
            "history_window": "20-year history (7300 days) for percentile grading.",
            "grading": "Percentile grades use indicator interpretation (higher vs lower is better).",
            "resampling": "Series are adaptively resampled to a storage-friendly cadence per frequency before history is saved."
        }
    }

    json_data = {
        "_README": readme,
        "metadata": {
            "generated_at": now.isoformat(),
            "data_source": "FRED API",
            "category": "Housing & Affordability",
            "indicators": [
                {
                    "series_id": ind.series_id,
                    "name": ind.name,
                    "frequency": ind.frequency,
                    "unit": ind.unit,
                }
                for ind in indicators
            ],
            "history_days": history_days,
            "data_start": start_date_str,
            "data_end": end_date_str,
        },
        "current": {"date": None, "overall_grade": {}, "indicators": {}},
        "history": {"series": {}},
    }

    grades = []

    for indicator in indicators:
        series_id = indicator.series_id
        if series_id not in indicator_data or indicator_data[series_id] is None or indicator_data[series_id].empty:
            print(f"  Warning: No data for {series_id}")
            continue

        df = indicator_data[series_id]
        df_clean = df.dropna(subset=["value"])
        if df_clean.empty:
            continue

        current_value = df_clean.iloc[-1]["value"]
        current_date = df_clean.iloc[-1]["date"]
        previous_value = df_clean.iloc[-2]["value"] if len(df_clean) > 1 else current_value

        historical_values = df_clean["value"]
        percentile = calculate_percentile_rank(historical_values, current_value)
        grade = calculate_grade(percentile, indicator.interpretation)
        grades.append(grade)

        trend = calculate_trend(current_value, previous_value)
        is_favorable = is_trend_favorable(trend, indicator.interpretation)
        changes = calculate_change_metrics(df_clean, frequency=indicator.frequency)

        if json_data["current"]["date"] is None:
            json_data["current"]["date"] = current_date.strftime("%Y-%m-%d")

        json_data["current"]["indicators"][series_id] = {
            "name": indicator.name,
            "value": round(current_value, 4) if current_value else None,
            "unit": indicator.unit,
            "frequency": indicator.frequency,
            "percentile": percentile,
            "grade": grade,
            "trend": trend,
            "is_favorable": is_favorable,
            "changes": changes,
            "interpretation": indicator.interpretation,
        }

        print(f"  Resampling {series_id} ({indicator.frequency} → storage format)...")
        df_resampled = adaptive_resample(df_clean, indicator.frequency, series_id)

        if not df_resampled.empty:
            json_data["history"]["series"][series_id] = {
                "dates": [d.strftime("%Y-%m-%d") for d in df_resampled["date"]],
                "values": df_resampled["value"].tolist(),
            }
            print(f"    → Stored {len(df_resampled)} data points")

    json_data["current"]["overall_grade"] = calculate_overall_grade(grades)
    json_data["current"]["summary"] = get_summary_description(json_data["current"]["overall_grade"].get("grade", "N/A"))
    json_data = sanitize_for_json(json_data)

    if output_path:
        save_json(json_data, output_path, indent=config.get("output", {}).get("indent", 2))

    return json_data


def get_summary_description(grade: str) -> str:
    """Get summary description based on overall grade."""
    descriptions = {
        "A+": "Housing activity, prices, and affordability metrics are exceptionally favorable.",
        "A": "Housing indicators are very healthy with supportive affordability conditions.",
        "B": "Housing indicators are generally supportive though affordability should be monitored.",
        "C": "Housing indicators are mixed with emerging affordability or activity concerns.",
        "D": "Housing indicators signal stress in activity, affordability, or financing costs.",
        "N/A": "Insufficient data to assess housing and affordability conditions.",
    }
    return descriptions.get(grade, "Housing and affordability indicators are being monitored.")


def main():
    parser = argparse.ArgumentParser(description="Export Housing & Affordability indicators to JSON")
    parser.add_argument("--output", type=str, default="housing_affordability.json")
    parser.add_argument("--config", type=str, default=None)
    args = parser.parse_args()

    try:
        export_housing_affordability_json(args.output, args.config)
        print("\n✓ Housing & Affordability data export complete!")
    except Exception as e:
        print(f"\n✗ Error: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
