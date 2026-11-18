import logging
import sys
from pathlib import Path

import pandas as pd

from etl import DatabaseConnection, HealthcareETL

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

ROOT = Path(__file__).resolve().parent
DATA_DIR = ROOT / "data"
ANALYTICS_OUT_DIR = ROOT / "reports" / "analytics"
PREVIEW_ROWS = 10


def print_and_save(results):
    """Print a preview of each analytics result and save the full result as CSV."""
    ANALYTICS_OUT_DIR.mkdir(parents=True, exist_ok=True)
    pd.set_option("display.width", 200)
    pd.set_option("display.max_columns", None)
    for name, df in results.items():
        df.to_csv(ANALYTICS_OUT_DIR / f"{name}.csv", index=False)
        print(f"\n=== {name} ({len(df)} rows) ===")
        print(df.head(PREVIEW_ROWS).to_string(index=False) if len(df) else "(no rows)")
        if len(df) > PREVIEW_ROWS:
            print(f"... {len(df) - PREVIEW_ROWS} more rows in reports/analytics/{name}.csv")


def main():
    db = DatabaseConnection()
    db.test_connection()
    pipeline = HealthcareETL(
        patients_file=DATA_DIR / "patients.csv",
        appointments_file=DATA_DIR / "appointments.csv",
        treatments_file=DATA_DIR / "treatments.csv",
    )
    results = pipeline.run(db)

    print("\n=== ETL metrics ===")
    for key, value in pipeline.metrics.items():
        print(f"{key}: {value}")
    print("\n=== Summary statistics ===")
    for key, value in pipeline.stats.items():
        print(f"{key}: {value:.2f}")

    print_and_save(results)
    print(f"\nFull analytics CSVs: {ANALYTICS_OUT_DIR}")
    print(f"Data quality report: {pipeline.report_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
