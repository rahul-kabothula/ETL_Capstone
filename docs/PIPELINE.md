# Healthcare ETL Pipeline: Step-by-Step Documentation

## 1. What this pipeline does

A healthcare provider keeps three flat CSV files: patients, appointments and treatments. The files contain missing values, invalid dates, duplicate IDs and records that point at things that do not exist (for example an appointment for an unknown patient).

This pipeline:

1. **Reads** the three CSV files.
2. **Checks** their quality (schema, types, missing values, duplicates, orphans) and writes a data quality report.
3. **Cleans and standardises** the data, and computes visit durations.
4. **Enriches** it with statistics and flags (frequent visitors, overlapping appointments, high-cost outliers, monthly totals).
5. **Loads** the clean data into a PostgreSQL star schema (dimension and fact tables), using upserts so it can be re-run safely.
6. **Audits** the run by saving counts (rows extracted, loaded, invalid, duplicates, orphans) to the `etl_audit_log` table.
7. **Runs 10 SQL analytics queries** on the loaded data and saves the results.

The result is a clean, queryable database and a set of reports for analysis.

## 2. The pipeline at a glance

```
 data/*.csv
     |
     v
 [1 EXTRACT]    raw text tables (self.raw)
     |
     v
 [2 VALIDATE]   typed tables + data quality report + duplicate/orphan counts
     |
     v
 [3 TRANSFORM]  clean patients / appointments / treatments + merged tables
     |
     v
 [4 ENRICH]     statistics, flags, summaries (held in memory)
     |
     v
 [5 LOAD]       PostgreSQL: dim_patient, dim_doctor, dim_appointment, fact_treatment, etl_audit_log
     |
     v
 [6 ANALYTICS]  10 SQL queries -> printed + reports/analytics/*.csv
```

All steps are methods of `HealthcareETL` in [etl/healthcare_etl.py](../etl/healthcare_etl.py), run in order by `HealthcareETL.run()`. The script [run_etl.py](../run_etl.py) starts everything.

### Files involved (overview)

| File / folder | Role |
|---|---|
| [run_etl.py](../run_etl.py) | Entry point. Connects to the DB, runs the pipeline, prints and saves results |
| [etl/healthcare_etl.py](../etl/healthcare_etl.py) | `Patient`, `Appointment` and `HealthcareETL` classes; the pipeline stages |
| [etl/data_utils.py](../etl/data_utils.py) | Reusable functions: reading, schema validation, cleaning, deduplication, metrics |
| [etl/db_connection.py](../etl/db_connection.py) | `DatabaseConnection`: PostgreSQL connection and helpers to run the SQL files |
| [sql/ddl/](../sql/ddl/) | Table definitions and indexes |
| [sql/transformations/](../sql/transformations/) | Upsert statements and post-load validation |
| [sql/analytics/](../sql/analytics/) | The 10 analytics queries |
| [data/](../data/) | Input CSVs |
| [reports/](../reports/) | Generated outputs |
| [.env](../.env.example) | Database settings (host, port, name, user, password) |
| [tests/test_etl.py](../tests/test_etl.py) | Unit tests |

## 3. Step-by-step

### Step 0: Start-up and database connection

**Files:** [run_etl.py](../run_etl.py), [etl/db_connection.py](../etl/db_connection.py), `.env`

**Input:** `.env` settings (`DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD`).

**What happens:**
- `DatabaseConnection` reads the settings and builds a SQLAlchemy engine (psycopg2 driver).
- `test_connection()` runs `SELECT 1`. If the database is unreachable or the password is wrong, the run stops here, before any data is touched.
- `HealthcareETL` is created with the three CSV paths.

**Output:** a working database connection and a `HealthcareETL` object with an empty `metrics` dictionary and a unique `run_id`.

---

### Step 1: Extract

**Method:** `HealthcareETL.extract()`
**Files:** [etl/healthcare_etl.py](../etl/healthcare_etl.py), [etl/data_utils.py](../etl/data_utils.py) (`read_csv_safe`)

**Input:**

| File | Columns |
|---|---|
| `data/patients.csv` | `patient_id, patient_name, dob, gender` |
| `data/appointments.csv` | `appointment_id, patient_id, doctor_id, start_time, end_time` |
| `data/treatments.csv` | `treatment_id, appointment_id, treatment_type, duration_minutes, cost` |

**What happens:**
- Each CSV is read with pandas **as text** (every value is a string). Nothing is converted yet, so bad values such as `invalid_date` or `invalid_timestamp` do not crash the load.
- Corrupt lines (for example, the wrong number of fields) are skipped and counted instead of stopping the run.
- `metrics["rows_extracted"]` is set to the total row count of all three files.

**Output (in memory):** `self.raw`, a dictionary of three all-text DataFrames (`patients`, `appointments`, `treatments`), and `self.corrupt_lines` (count per file).

*Example run: 1,100 + 5,500 + 7,500 = 14,100 rows extracted.*

---

### Step 2: Validate

**Method:** `HealthcareETL.validate()`
**Files:** [etl/healthcare_etl.py](../etl/healthcare_etl.py), [etl/data_utils.py](../etl/data_utils.py) (`validate_schema`, `coerce_types`, `detect_duplicates`, `find_orphans`)

**Input:** `self.raw` from Step 1.

**What happens:**
1. **Schema check** (`validate_schema`): every expected column must exist. A missing column raises `SchemaError` and stops the run. For each column it also counts missing values and values that cannot be converted to the expected type (for example a non-date in `dob`, or `end_time = invalid_timestamp`).
2. **Type conversion** (`coerce_types`): values are converted to their real types (integers, floats, timestamps). Anything that cannot be converted becomes null rather than raising an error.
3. **Duplicate detection** (`detect_duplicates`): repeated `patient_id`, `appointment_id` and `treatment_id` values (second occurrence onward). Null keys are not counted as duplicates.
4. **Orphan detection** (`find_orphans`):
   - appointments whose `patient_id` is not in the patients file;
   - treatments whose `appointment_id` is not in the appointments file.
5. **Invalid time ranges:** appointments where `end_time` is not after `start_time`.
6. The counts are stored in `metrics` (`duplicate_*`, `orphan_*`).
7. The data quality report is written.

**Output:**
- `self.patients_df`, `self.appointments_df`, `self.treatments_df`: typed but **not yet cleaned** (duplicates, orphans and nulls are still there).
- `self.quality_report` (dictionary).
- **File: [reports/data_quality_report.md](../reports/data_quality_report.md)** with, per input file, the row count, corrupt lines, missing values per column, invalid values per column, then duplicates and orphans.

*Example run: 28 duplicate patients, 172 duplicate appointments, 225 duplicate treatments, 589 orphan appointments, 341 orphan treatments.*

---

### Step 3: Transform

**Method:** `HealthcareETL.transform()`
**Files:** [etl/healthcare_etl.py](../etl/healthcare_etl.py), [etl/data_utils.py](../etl/data_utils.py) (`normalize_patients`, `handle_missing`, `compute_visit_duration`, `drop_invalid_rows`, `deduplicate`)

**Input:** the typed frames from Step 2.

**What happens, per dataset:**

*Patients*
- Names: whitespace collapsed, title case (suffixes such as MD and PhD kept).
- Gender standardised to `M` or `F`; anything else becomes null.
- Date of birth standardised to a date. Unparseable or future dates become null.
- Missing names become `Unknown`.
- Rows without a `patient_id` are dropped. Repeated `patient_id` values: first occurrence kept.

*Appointments*
- **Visit duration** is computed as `end_time - start_time` in minutes (`duration_minutes`), vectorised with NumPy.
- Rows are dropped if `appointment_id`, `patient_id`, `doctor_id`, `start_time` or `end_time` is missing, or if the duration is zero or negative.
- Repeated `appointment_id` values are removed (first kept), as are repeated (`patient_id`, `doctor_id`, `start_time`) combinations.
- **Orphans removed:** appointments whose patient is not in the clean patient set.

*Treatments*
- `treatment_type` whitespace and case are standardised.
- Non-positive `duration_minutes` and negative `cost` are set to null. Missing values are left null (not filled with made-up numbers).
- Rows without `treatment_id`, `appointment_id` or `treatment_type` are dropped. Repeated `treatment_id` values are removed.
- **Orphans removed:** treatments whose appointment is not in the clean appointment set.

*Merges*
- `appointment_patient_df`: appointments joined to patients.
- `treatment_appointment_df`: treatments joined to appointments (adds `patient_id`, `doctor_id`, `start_time`, `end_time`). This is the table the fact load uses.

**Output (in memory):**
- Clean `self.patients_df`, `self.appointments_df`, `self.treatments_df`.
- `self.appointment_patient_df`, `self.treatment_appointment_df`.
- `self.dropped` (orphans removed) and updated `metrics["invalid_rows"]`, `metrics["rows_transformed"]`.

*Example run: 1,072 patients, 4,311 appointments, 5,565 treatments (10,948 rows transformed), 501 invalid rows.*

---

### Step 4: Enrich

**Method:** `HealthcareETL.enrich()`
**Files:** [etl/healthcare_etl.py](../etl/healthcare_etl.py), [etl/data_utils.py](../etl/data_utils.py) (`summary_statistics`, `find_frequent_visitors`, `find_overlapping_appointments`, `flag_cost_outliers`, `aggregate_treatment_cost`, `monthly_summary`, `optimize_memory`)

**Input:** the clean frames and merged frames from Step 3.

**What happens (NumPy and pandas):**

| Result | How it is computed |
|---|---|
| `stats["avg_treatment_cost"]` | mean treatment cost |
| `stats["median_visit_duration"]` | median visit duration (minutes) |
| `stats["std_treatment_duration"]` | standard deviation of treatment duration |
| `stats["high_cost_cutoff"]` | 99th percentile of cost |
| `frequent_visits` | patient-months with more than `frequent_visit_threshold` appointments (default 3) |
| `overlapping_appointments` | appointments that overlap another appointment of the same patient |
| `high_cost_treatments` | treatments at or above the 99th percentile cost |
| `patient_summary` | visits, treatment count and total cost per patient, with a frequent-visitor flag |
| `visit_summary` | treatment count and total cost per appointment |
| `monthly_summary` | appointments, treatments and total cost per month |

Memory optimisation: `doctor_id` and `treatment_type` are converted to the categorical type.

**Output (in memory, not written to the database or files):** the attributes listed above on the `HealthcareETL` object. `stats` is printed at the end of `run_etl.py`. The SQL analytics in Step 6 compute the equivalent reports in the database.

*Example run: average cost 2537.63, median visit 68.00 min, std of treatment duration 50.51, 99th-percentile cost 4960.67.*

---

### Step 5: Load

**Method:** `HealthcareETL.load(db_connection)`
**Files:** [etl/healthcare_etl.py](../etl/healthcare_etl.py), [etl/db_connection.py](../etl/db_connection.py), [sql/ddl/](../sql/ddl/), [sql/transformations/](../sql/transformations/)

**Input:** the clean frames from Step 3 and the open database connection.

**What happens, in order:**

1. **Create the schema** by running every file in `sql/ddl/` (safe to repeat; everything uses `IF NOT EXISTS`):

   | File | Creates |
   |---|---|
   | `01_dimensions_and_facts.sql` | `dim_patient`, `dim_doctor`, `dim_appointment`, `fact_treatment` (partitioned by month on `visit_date`), with primary keys, foreign keys, NOT NULL, UNIQUE and CHECK constraints |
   | `02_audit_log.sql` | `etl_audit_log` |
   | `03_indexes.sql` | indexes on `patient_id`, `appointment_id`, `treatment_id`, `doctor_id`, `start_time` and `treatment_type` |

2. **Upsert the data**, parents before children. Each SQL file is a single-row `INSERT ... ON CONFLICT DO UPDATE` statement that `DatabaseConnection.upsert_dataframe()` runs for every DataFrame row, in batches, in one transaction:

   | Order | SQL file | Target table | Source DataFrame |
   |---|---|---|---|
   | 1 | `01_upsert_dim_patient.sql` | `dim_patient` | patients |
   | 2 | `02_upsert_dim_doctor.sql` | `dim_doctor` | distinct `doctor_id` of appointments |
   | 3 | `03_upsert_dim_appointment.sql` | `dim_appointment` | appointments |
   | 4 | `04_upsert_fact_treatment.sql` | `fact_treatment` | treatments merged with appointments, plus `visit_date` (the appointment start date) |

   Existing rows are updated instead of duplicated, so running the pipeline again is safe.

3. **Post-load validation** (`05_validate_load.sql`): checks for orphan treatments, duplicate appointment IDs and invalid appointment durations. Every check should return 0 violations. Failures are logged as warnings and the audit status becomes `FAILED_VALIDATION`.

4. **Audit log:** `metrics["rows_loaded"]` is set to the total number of rows now in `dim_patient`, `dim_appointment` and `fact_treatment`, and one row with all metrics is inserted into `etl_audit_log`.

**Output (in PostgreSQL):**
- Populated `dim_patient`, `dim_doctor`, `dim_appointment`, `fact_treatment`.
- One new row in `etl_audit_log`, containing: `run_id`, `run_timestamp`, `rows_extracted`, `rows_transformed`, `rows_loaded`, `invalid_rows`, `duplicate_patients`, `duplicate_appointments`, `duplicate_treatments`, `orphan_appointments`, `orphan_treatments`, `status`.

*Example run: 1,072 patients, 100 doctors, 4,311 appointments, 5,565 treatments; 0 violations on all three checks.*

---

### Step 6: SQL analytics

**Method:** `HealthcareETL.run_analytics(db_connection)`, then `print_and_save()` in [run_etl.py](../run_etl.py)
**Files:** [sql/analytics/](../sql/analytics/), [etl/db_connection.py](../etl/db_connection.py) (`read_sql_file`)

**Input:** the loaded database tables. The only parameter is `:threshold` (default 3), used by the frequent-visit query.

**What happens:** every `.sql` file in `sql/analytics/` runs in filename order, and each result is read into a DataFrame.

| File | Question answered |
|---|---|
| `01_most_common_treatments.sql` | Which treatments are most common? |
| `02_avg_visits_per_patient.sql` | How many visits does a patient have on average? |
| `03_monthly_appointment_trends.sql` | How many appointments per month, and the month-over-month change? |
| `04_total_cost_per_patient.sql` | What is each patient's total treatment cost? |
| `05_avg_treatment_duration_by_type.sql` | What is the average duration for each treatment type? |
| `06_overlapping_appointments.sql` | Which patients have overlapping appointments? |
| `07_frequent_visit_patients.sql` | Which patients visit more than the threshold per month? |
| `08_treatments_per_doctor.sql` | How many treatments does each doctor perform? |
| `09_treatment_cost_outliers.sql` | Which treatments are in the top 1% by cost? |
| `10_cohort_analysis.sql` | How does treatment frequency vary by the month a patient first appeared? |

**Output:**
- Terminal: a 10-row preview of each result.
- Files: one CSV per query in `reports/analytics/` (for example `reports/analytics/01_most_common_treatments.csv`), holding the full result.

## 4. Final output summary

| Output | Location | Content |
|---|---|---|
| Star schema tables | PostgreSQL | `dim_patient`, `dim_doctor`, `dim_appointment`, `fact_treatment` |
| Audit log | PostgreSQL table `etl_audit_log` | One row per run with ETL metrics and status |
| Data quality report | [reports/data_quality_report.md](../reports/data_quality_report.md) | Quality of the **raw** input: missing values, invalid values, duplicates, orphans |
| Analytics results | `reports/analytics/*.csv` | Full result of each of the 10 queries on the **clean** data |
| Console output | Terminal | Progress logs, ETL metrics, summary statistics, 10-row previews |

The report files are overwritten on each run.

## 5. How the metrics relate

For the example run:

```
rows_extracted    14,100   all rows in the 3 CSVs
rows_transformed  10,948   rows left after cleaning (1,072 + 4,311 + 5,565)
rows_loaded       10,948   total rows in dim_patient + dim_appointment + fact_treatment
invalid_rows         501   corrupt lines + rows dropped for missing keys or non-positive duration
```

- The duplicate and orphan counts in the audit log are measured on the **raw** data in Step 2, before cleaning.
- `rows_loaded` counts the rows in the tables after the load, so on a re-run with the same data it stays the same.
- `dim_doctor` rows are not included in `rows_transformed` or `rows_loaded`.

## 6. Error handling and re-running

| Situation | Behaviour |
|---|---|
| Corrupt CSV line | Skipped, counted, logged |
| Missing required column | `SchemaError`, run stops |
| Bad date or number in a value | Becomes null, then handled in Step 3 (dropped or left null) |
| Database unreachable or wrong password | Run stops at Step 0 |
| Re-running the pipeline | Dimension and fact rows are updated, not duplicated; a new `etl_audit_log` row is added |

## 7. Running and testing

```bash
source .venv/bin/activate
python run_etl.py          # run the whole pipeline
python -m pytest -v        # run the unit tests
```

Environment and database setup are in the [README](../README.md).
