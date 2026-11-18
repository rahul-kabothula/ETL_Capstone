# Healthcare ETL Pipeline

ETL pipeline that loads patient, appointment and treatment CSVs into a PostgreSQL star schema (Python, pandas, NumPy, SQL).

For a detailed explanation of each stage (inputs, outputs, files involved), see [docs/PIPELINE.md](docs/PIPELINE.md).

```
CSV files -> Extract -> Validate -> Transform -> Enrich -> Load (upsert dim/fact) -> SQL analytics
                                                                      \-> etl_audit_log   \-> reports/data_quality_report.md
```

## Project structure

```
capstone/
├── data/                     # input CSVs: patients.csv, appointments.csv, treatments.csv
├── etl/
│   ├── db_connection.py      # PostgreSQL connection + helpers to run SQL files
│   ├── data_utils.py         # reusable functions (schema checks, dedup, metrics)
│   └── healthcare_etl.py     # Patient, Appointment, HealthcareETL classes
├── sql/
│   ├── ddl/                  # tables, audit log, indexes
│   ├── transformations/      # upserts into dim/fact tables + post-load validation
│   └── analytics/            # 10 business queries
├── tests/test_etl.py         # unit tests
├── reports/                  # data_quality_report.md (generated on each run)
├── run_etl.py                # entry point
├── requirements.txt
└── .env.example              # database settings template
```

## 1. Prerequisites

- Python 3.10+
- PostgreSQL 14+ (`psql` and `createdb` on your PATH)

macOS with Homebrew:

```bash
brew install postgresql@16
brew services start postgresql@16
```

If `psql` is not found after installing, add it to your PATH (Homebrew prints the exact line), or use the full path under `/opt/homebrew/opt/postgresql@16/bin/`.

## 2. Set up the Python environment

Run from the project root (`capstone/`):

```bash
python3 -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

To leave the environment later: `deactivate`.

## 3. Set up the database

### 3.1 Find a user that can log in

On a Homebrew install, the superuser is usually your macOS username, not `postgres`. Check which users exist:

```bash
psql -d postgres -c "\du"
```

If it asks for a password you don't know, reset it from a local connection (replace `your_user` and the password):

```bash
psql -d postgres -c "ALTER USER your_user WITH PASSWORD 'choose_a_password';"
```

### 3.2 Create the database

```bash
createdb -U your_user healthcare_dw
```

(Or inside `psql`: `CREATE DATABASE healthcare_dw;`)

### 3.3 Configure the connection

```bash
cp .env.example .env
```

Edit `.env`:

```
DB_HOST=localhost
DB_PORT=5432
DB_NAME=healthcare_dw
DB_USER=your_user
DB_PASSWORD=choose_a_password
```

`.env` is git-ignored, so the password is not committed.

### 3.4 Tables

You do not need to create tables by hand. The pipeline runs everything in `sql/ddl/` on each run (all statements use `IF NOT EXISTS`, so re-running is safe):

| Table | Purpose |
|---|---|
| `dim_patient`, `dim_doctor`, `dim_appointment` | Dimension tables |
| `fact_treatment` | Fact table, partitioned by month on `visit_date` |
| `etl_audit_log` | One row of metrics per pipeline run |

To create the schema manually instead:

```bash
for f in sql/ddl/*.sql; do psql -U your_user -d healthcare_dw -f "$f"; done
```

## 4. Add the input data

Place the three CSVs in `data/`:

```
data/patients.csv       # patient_id, patient_name, dob, gender
data/appointments.csv   # appointment_id, patient_id, doctor_id, start_time, end_time
data/treatments.csv     # treatment_id, appointment_id, treatment_type, duration_minutes, cost
```

They come from the sample generator script in the project brief (needs `faker`, already in `requirements.txt`). Run that script inside `data/` so the files land there.

## 5. Run the pipeline

```bash
source .venv/bin/activate
python run_etl.py
```

The run:

1. Checks the database connection.
2. **Extract**: reads the CSVs, skipping corrupt lines.
3. **Validate**: checks schema, types, duplicates and orphans, and writes `reports/data_quality_report.md`.
4. **Transform**: cleans, computes visit durations, removes duplicates, invalid rows and orphans, and merges the datasets.
5. **Enrich**: NumPy statistics, frequent visitors, overlapping appointments, cost outliers and monthly aggregates.
6. **Load**: creates the schema, upserts the cleaned DataFrames straight into the dimension and fact tables (`ON CONFLICT DO UPDATE`, one SQL file per table in `sql/transformations/`), runs the post-load checks and writes `etl_audit_log`.
7. Runs all queries in `sql/analytics/`.

Progress is logged to the console. At the end it prints the ETL metrics, summary statistics and a 10-row preview of each analytics query, and saves the full results as CSVs in `reports/analytics/`. The run is idempotent; running it again updates existing rows rather than duplicating them.

### Check the results

```bash
psql -U your_user -d healthcare_dw -c "SELECT * FROM etl_audit_log ORDER BY audit_id DESC LIMIT 1;"
psql -U your_user -d healthcare_dw -f sql/transformations/05_validate_load.sql
```

All checks in `05_validate_load.sql` should show `violations = 0`.

### Run analytics queries on their own

```bash
psql -U your_user -d healthcare_dw -f sql/analytics/01_most_common_treatments.sql
psql -U your_user -d healthcare_dw -v threshold=3 -f sql/analytics/07_frequent_visit_patients.sql
```

`07_frequent_visit_patients.sql` needs `-v threshold=N` (patients with more than N visits in a month).

### Use the pipeline from Python

```python
from etl import DatabaseConnection, HealthcareETL

etl = HealthcareETL("data/patients.csv", "data/appointments.csv", "data/treatments.csv",
                    frequent_visit_threshold=3)
results = etl.run(DatabaseConnection())     # dict of DataFrames, one per analytics query
print(etl.metrics)                          # rows extracted / transformed / loaded, duplicates, orphans
print(etl.stats)                            # average cost, median visit duration, etc.
```

## 6. Run the tests

```bash
python -m pytest -v
```

Tests cover visit duration, treatment cost aggregation, duplicate detection, overlaps, outliers and a no-database end-to-end run.

## Troubleshooting

| Problem | Fix |
|---|---|
| `password authentication failed` | Check `DB_USER` and `DB_PASSWORD` in `.env` (see 3.1) |
| `database "healthcare_dw" does not exist` | Run `createdb` (see 3.2) or change `DB_NAME` |
| `role "postgres" does not exist` | Set `DB_USER` to your actual user (usually your macOS username) |
| `connection refused` | Start PostgreSQL: `brew services start postgresql@16` |
| `ModuleNotFoundError` | Activate the venv and run `pip install -r requirements.txt` |
| `FileNotFoundError` for a CSV | Put the three CSVs in `data/` (see section 4) |

## Design notes and assumptions

- **Star schema**: `fact_treatment` references `dim_appointment`, `dim_patient` and `dim_doctor`. Indexes are on `patient_id`, `appointment_id`, `treatment_id` and `doctor_id`.
- **Partitions**: `fact_treatment` has monthly partitions from 36 months before to 12 months after the current month. Treatments outside that range fail to load, so widen the range in `sql/ddl/01_dimensions_and_facts.sql` for older data.
- **Duplicates**: the first occurrence of a repeated ID is kept.
- **Orphans**: appointments without a valid patient, and treatments without a valid appointment, are dropped.
- **Missing values**: a missing patient name becomes `Unknown`. A missing or invalid `cost` or `duration_minutes` stays null so totals are not invented.
- **Invalid dates**: unparseable dates of birth are stored as null. Appointments with an unparseable or non-positive time range are dropped.
- **Cohort analysis**: the data has no signup date, so a patient's first appointment month is used as the signup month.
- **Outliers**: high-cost treatments are those at or above the 99th percentile.
