"""Healthcare ETL pipeline: extract -> validate -> transform -> enrich -> load.

Stage data flow:
  extract   raw CSV frames (all strings) in self.raw
  validate  schema checks, quality report; typed (but uncleaned) frames in self.*_df
  transform cleaned frames, durations, dedup, orphan removal, merged frames
  enrich    NumPy stats, flags, aggregates, memory optimisation
  load      pandas rows -> upsert SQL (sql/transformations) -> validate -> audit log
"""

import logging
import uuid
from pathlib import Path
from typing import Dict, Optional

import numpy as np
import pandas as pd

from . import data_utils as du
from .db_connection import ANALYTICS_DIR, DDL_DIR, PROJECT_ROOT, TRANSFORM_DIR, DatabaseConnection

logger = logging.getLogger(__name__)

# Upsert scripts in sql/transformations, in dependency order (parents before children).
# Each script's %(name)s parameters match the DataFrame columns listed here.
UPSERT_SCRIPTS = [
    ("01_upsert_dim_patient.sql", ["patient_id", "patient_name", "dob", "gender"]),
    ("02_upsert_dim_doctor.sql", ["doctor_id"]),
    ("03_upsert_dim_appointment.sql", ["appointment_id", "patient_id", "doctor_id",
                                       "start_time", "end_time", "duration_minutes"]),
    ("04_upsert_fact_treatment.sql", ["treatment_id", "appointment_id", "patient_id", "doctor_id",
                                      "treatment_type", "duration_minutes", "cost", "visit_date"]),
]
VALIDATION_SCRIPT = "05_validate_load.sql"

# Columns of the cleaned frames kept in memory after transform().
PATIENT_COLUMNS = ["patient_id", "patient_name", "dob", "gender"]
APPOINTMENT_COLUMNS = ["appointment_id", "patient_id", "doctor_id", "start_time", "end_time", "duration_minutes"]
TREATMENT_COLUMNS = ["treatment_id", "appointment_id", "treatment_type", "duration_minutes", "cost"]

SCHEMAS = {
    "patients": du.PATIENT_SCHEMA,
    "appointments": du.APPOINTMENT_SCHEMA,
    "treatments": du.TREATMENT_SCHEMA,
}

DEFAULT_REPORT_PATH = PROJECT_ROOT / "reports" / "data_quality_report.md"


class Patient:
    def __init__(self, patient_id, name, dob, gender):
        self.patient_id = patient_id
        self.name = name
        self.dob = dob
        self.gender = gender

    def normalized(self) -> "Patient":
        """Copy with cleaned name, standardised gender and a parsed (or None) date of birth."""
        dob = pd.to_datetime(self.dob, errors="coerce")
        return Patient(
            self.patient_id,
            du.normalize_name(self.name),
            None if pd.isna(dob) else dob.normalize(),
            du.normalize_gender(self.gender),
        )

    def is_valid(self) -> bool:
        return self.patient_id is not None and not pd.isna(self.patient_id)

    def __repr__(self):
        return f"Patient({self.patient_id!r}, {self.name!r}, {self.dob!r}, {self.gender!r})"


class Appointment:
    def __init__(self, appointment_id, patient_id, start_time, end_time, doctor_id):
        self.appointment_id = appointment_id
        self.patient_id = patient_id
        self.start_time = pd.to_datetime(start_time, errors="coerce")
        self.end_time = pd.to_datetime(end_time, errors="coerce")
        self.doctor_id = doctor_id

    def duration_minutes(self) -> Optional[float]:
        """Minutes between end and start; None if either timestamp is missing/invalid."""
        if pd.isna(self.start_time) or pd.isna(self.end_time):
            return None
        return round((self.end_time - self.start_time).total_seconds() / 60, 2)

    def is_valid(self) -> bool:
        duration = self.duration_minutes()
        return (duration is not None and duration > 0
                and not pd.isna(self.appointment_id) and not pd.isna(self.patient_id))

    def overlaps(self, other: "Appointment") -> bool:
        """True when both belong to the same patient and their time ranges intersect."""
        if self.patient_id != other.patient_id or not (self.is_valid() and other.is_valid()):
            return False
        return self.start_time < other.end_time and other.start_time < self.end_time

    def __repr__(self):
        return (f"Appointment({self.appointment_id!r}, {self.patient_id!r}, "
                f"{self.start_time!r}, {self.end_time!r}, {self.doctor_id!r})")


class HealthcareETL:
    def __init__(self, patients_file, appointments_file, treatments_file,
                 frequent_visit_threshold: int = 3, outlier_percentile: float = 99,
                 report_path=None):
        self.patients_file = patients_file
        self.appointments_file = appointments_file
        self.treatments_file = treatments_file
        self.frequent_visit_threshold = frequent_visit_threshold
        self.outlier_percentile = outlier_percentile
        self.report_path = Path(report_path) if report_path else DEFAULT_REPORT_PATH

        # Cleaned frames used by load().
        self.patients_df = pd.DataFrame(columns=PATIENT_COLUMNS)
        self.appointments_df = pd.DataFrame(columns=APPOINTMENT_COLUMNS)
        self.treatments_df = pd.DataFrame(columns=TREATMENT_COLUMNS)

        # Intermediate / analytical outputs
        self.raw: Dict[str, pd.DataFrame] = {}
        self.corrupt_lines: Dict[str, int] = {}
        self.quality_report: dict = {}
        self.dropped: Dict[str, int] = {}
        self.appointment_patient_df = pd.DataFrame()
        self.treatment_appointment_df = pd.DataFrame()
        self.stats: Dict[str, float] = {}
        self.patient_summary = pd.DataFrame()
        self.visit_summary = pd.DataFrame()
        self.monthly_summary = pd.DataFrame()
        self.frequent_visits = pd.DataFrame()
        self.overlapping_appointments = pd.DataFrame()
        self.high_cost_treatments = pd.DataFrame()

        self.run_id = str(uuid.uuid4())
        self.metrics: Dict[str, int] = {
            "rows_extracted": 0,
            "rows_transformed": 0,
            "rows_loaded": 0,
            "invalid_rows": 0,
            "duplicate_patients": 0,
            "duplicate_appointments": 0,
            "duplicate_treatments": 0,
            "orphan_appointments": 0,
            "orphan_treatments": 0,
        }

    # ------------------------------------------------------------------ extract
    def extract(self):
        """Read the three CSVs (as strings), skipping corrupt lines."""
        logger.info("Extract started")
        files = {"patients": self.patients_file,
                 "appointments": self.appointments_file,
                 "treatments": self.treatments_file}
        for name, path in files.items():
            self.raw[name], self.corrupt_lines[name] = du.read_csv_safe(path)
            logger.info("Extracted %d rows from %s", len(self.raw[name]), path)
        self.metrics["rows_extracted"] = sum(len(df) for df in self.raw.values())

    # ----------------------------------------------------------------- validate
    def validate(self):
        """Schema validation, type coercion, duplicate/orphan detection, quality report."""
        logger.info("Validate started")
        if not self.raw:
            raise RuntimeError("validate() called before extract()")

        report = {"datasets": {}, "orphans": {}, "duplicates": {}}
        for name, schema in SCHEMAS.items():
            report["datasets"][name] = du.validate_schema(self.raw[name], schema, name)
            report["datasets"][name]["corrupt_lines"] = self.corrupt_lines.get(name, 0)

        self.patients_df = du.coerce_types(self.raw["patients"], du.PATIENT_SCHEMA)
        self.appointments_df = du.coerce_types(self.raw["appointments"], du.APPOINTMENT_SCHEMA)
        self.treatments_df = du.coerce_types(self.raw["treatments"], du.TREATMENT_SCHEMA)
        p, a, t = self.patients_df, self.appointments_df, self.treatments_df

        report["duplicates"] = {
            "patients": int(du.detect_duplicates(p, ["patient_id"]).sum()),
            "appointments": int(du.detect_duplicates(a, ["appointment_id"]).sum()),
            "treatments": int(du.detect_duplicates(t, ["treatment_id"]).sum()),
        }
        report["orphans"] = {
            "appointments_missing_patient": int(du.find_orphans(a, "patient_id", p, "patient_id").sum()),
            "treatments_missing_appointment": int(du.find_orphans(t, "appointment_id", a, "appointment_id").sum()),
        }
        report["datasets"]["appointments"]["end_not_after_start"] = int(
            (a["start_time"].notna() & a["end_time"].notna() & (a["end_time"] <= a["start_time"])).sum())

        self.metrics["duplicate_patients"] = report["duplicates"]["patients"]
        self.metrics["duplicate_appointments"] = report["duplicates"]["appointments"]
        self.metrics["duplicate_treatments"] = report["duplicates"]["treatments"]
        self.metrics["orphan_appointments"] = report["orphans"]["appointments_missing_patient"]
        self.metrics["orphan_treatments"] = report["orphans"]["treatments_missing_appointment"]

        self.quality_report = report
        self._write_quality_report()
        logger.info("Validation done: duplicates=%s orphans=%s", report["duplicates"], report["orphans"])
        return report

    # ---------------------------------------------------------------- transform
    def transform(self):
        """Clean and normalise, compute durations, dedupe, drop invalid/orphan rows, merge."""
        logger.info("Transform started")
        invalid = sum(self.corrupt_lines.values())

        # Patients
        p = du.normalize_patients(self.patients_df)
        p = du.handle_missing(p, fill_values={"patient_name": "Unknown"})
        p, n = du.drop_invalid_rows(p, ["patient_id"])
        invalid += n
        p, _ = du.deduplicate(p, ["patient_id"])

        # Appointments
        a = self.appointments_df.copy()
        a["duration_minutes"] = du.compute_visit_duration(a["start_time"], a["end_time"])
        a, n = du.drop_invalid_rows(
            a, ["appointment_id", "patient_id", "doctor_id", "start_time", "end_time"],
            extra_valid=a["duration_minutes"] > 0)
        invalid += n
        a, _ = du.deduplicate(a, ["appointment_id"])
        a, _ = du.deduplicate(a, ["patient_id", "doctor_id", "start_time"])
        orphan_a = ~a["patient_id"].isin(p["patient_id"])
        self.dropped["appointments_without_patient"] = int(orphan_a.sum())
        a = a[~orphan_a].copy()

        # Treatments: non-positive duration / negative cost are treated as missing values.
        t = self.treatments_df.copy()
        t["treatment_type"] = t["treatment_type"].map(
            lambda v: None if pd.isna(v) else " ".join(str(v).split()).title())
        t["duration_minutes"] = t["duration_minutes"].where(t["duration_minutes"] > 0)
        t["cost"] = t["cost"].where(t["cost"] >= 0)
        t, n = du.drop_invalid_rows(t, ["treatment_id", "appointment_id", "treatment_type"])
        invalid += n
        t, _ = du.deduplicate(t, ["treatment_id"])
        orphan_t = ~t["appointment_id"].isin(a["appointment_id"])
        self.dropped["treatments_without_appointment"] = int(orphan_t.sum())
        t = t[~orphan_t].copy()

        self.patients_df, self.appointments_df, self.treatments_df = (
            p.reset_index(drop=True), a.reset_index(drop=True), t.reset_index(drop=True))

        # Merges: appointments -> patients, treatments -> appointments
        self.appointment_patient_df = self.appointments_df.merge(
            self.patients_df, on="patient_id", how="left", validate="many_to_one")
        self.treatment_appointment_df = self.treatments_df.merge(
            self.appointments_df[["appointment_id", "patient_id", "doctor_id", "start_time", "end_time"]],
            on="appointment_id", how="left", validate="many_to_one")

        self.metrics["invalid_rows"] = int(invalid)
        self.metrics["rows_transformed"] = int(len(p) + len(a) + len(t))
        logger.info("Transformed rows: patients=%d appointments=%d treatments=%d (invalid=%d, dropped=%s)",
                    len(p), len(a), len(t), invalid, self.dropped)

    # ------------------------------------------------------------------ enrich
    def enrich(self):
        """Statistics, frequent-visit / overlap / outlier flags, aggregates, memory optimisation."""
        logger.info("Enrich started")
        a, t, tv = self.appointments_df, self.treatments_df, self.treatment_appointment_df

        self.stats = du.summary_statistics(a, t)

        self.frequent_visits = du.find_frequent_visitors(a, self.frequent_visit_threshold)
        self.overlapping_appointments = du.find_overlapping_appointments(a)

        outlier_mask, cutoff = du.flag_cost_outliers(t["cost"], self.outlier_percentile)
        self.high_cost_treatments = t[outlier_mask].sort_values("cost", ascending=False)
        self.stats["high_cost_cutoff"] = cutoff

        visits = a.groupby("patient_id").size().rename("visit_count").reset_index()
        cost_by_patient = du.aggregate_treatment_cost(tv, "patient_id")
        self.patient_summary = visits.merge(cost_by_patient, on="patient_id", how="left")
        self.patient_summary[["treatment_count", "total_cost"]] = (
            self.patient_summary[["treatment_count", "total_cost"]].fillna(0))
        self.patient_summary["is_frequent_visitor"] = self.patient_summary["patient_id"].isin(
            self.frequent_visits["patient_id"])

        self.visit_summary = a[["appointment_id", "patient_id", "start_time", "duration_minutes"]].merge(
            du.aggregate_treatment_cost(tv, "appointment_id"), on="appointment_id", how="left")
        self.visit_summary[["treatment_count", "total_cost"]] = (
            self.visit_summary[["treatment_count", "total_cost"]].fillna(0))

        self.monthly_summary = du.monthly_summary(a, tv)

        # Memory optimisation (after all groupby work above)
        self.appointments_df = du.optimize_memory(a, ["doctor_id"])
        self.treatments_df = du.optimize_memory(t, ["treatment_type"])

        logger.info("Stats: %s | frequent patient-months=%d overlapping=%d high-cost=%d",
                    {k: round(v, 2) for k, v in self.stats.items()},
                    len(self.frequent_visits), len(self.overlapping_appointments),
                    len(self.high_cost_treatments))

    # -------------------------------------------------------------------- load
    def load(self, db_connection: DatabaseConnection):
        """Create schema, upsert the cleaned DataFrames into dim/fact tables, validate, audit."""
        logger.info("Load started")
        db_connection.run_sql_dir(DDL_DIR)

        facts = self.treatment_appointment_df.copy()
        facts["visit_date"] = facts["start_time"].dt.date
        frames = {
            "01_upsert_dim_patient.sql": self.patients_df,
            "02_upsert_dim_doctor.sql": self.appointments_df[["doctor_id"]].drop_duplicates(),
            "03_upsert_dim_appointment.sql": self.appointments_df,
            "04_upsert_fact_treatment.sql": facts,
        }
        for script, columns in UPSERT_SCRIPTS:
            db_connection.upsert_dataframe(frames[script][columns], TRANSFORM_DIR / script)

        checks = db_connection.read_sql_file(TRANSFORM_DIR / VALIDATION_SCRIPT)
        failed = checks[checks["violations"] > 0]
        if not failed.empty:
            logger.warning("Post-load validation failures:\n%s", failed.to_string(index=False))

        with db_connection.engine.connect() as conn:
            self.metrics["rows_loaded"] = int(
                pd.read_sql("SELECT (SELECT COUNT(*) FROM dim_patient) "
                            "+ (SELECT COUNT(*) FROM dim_appointment) "
                            "+ (SELECT COUNT(*) FROM fact_treatment) AS total", conn)["total"].iloc[0]
            )
        self._write_audit_log(db_connection, "FAILED_VALIDATION" if not failed.empty else "SUCCESS")

    def run_analytics(self, db_connection: DatabaseConnection, frequent_visit_threshold: Optional[int] = None
                      ) -> Dict[str, pd.DataFrame]:
        """Run every query in sql/analytics and return {file_stem: DataFrame}."""
        params = {"threshold": frequent_visit_threshold or self.frequent_visit_threshold}
        results = {}
        for path in sorted(ANALYTICS_DIR.glob("*.sql")):
            results[path.stem] = db_connection.read_sql_file(path, params)
            logger.info("Analytics %s -> %d rows", path.stem, len(results[path.stem]))
        return results

    def run(self, db_connection: Optional[DatabaseConnection] = None):
        db = db_connection or DatabaseConnection()
        try:
            self.extract()
            self.validate()
            self.transform()
            self.enrich()
            self.load(db)
            results = self.run_analytics(db)
            logger.info("ETL metrics: %s", self.metrics)
            return results
        finally:
            if db_connection is None:
                db.dispose()

    # ----------------------------------------------------------------- helpers
    def _write_audit_log(self, db_connection: DatabaseConnection, status: str):
        row = pd.DataFrame([{"run_id": self.run_id, **self.metrics, "status": status}])
        row = row.astype({c: np.int64 for c in self.metrics})
        db_connection.write_dataframe(row, "etl_audit_log")

    def _write_quality_report(self):
        """Write the data quality report (missing values, duplicates, invalid dates, orphans) as Markdown."""
        r = self.quality_report
        lines = ["# Data Quality Report", "", f"Run ID: `{self.run_id}`", ""]
        for name, info in r["datasets"].items():
            lines += [f"## {name}", "",
                      f"- Rows: {info['rows']}",
                      f"- Corrupt lines skipped: {info['corrupt_lines']}",
                      f"- Extra columns: {info['extra_columns'] or 'none'}"]
            if "end_not_after_start" in info:
                lines.append(f"- Appointments with end <= start: {info['end_not_after_start']}")
            lines += ["", "| Column | Missing values | Invalid values (wrong type / date) |", "|---|---|---|"]
            for col in info["null_counts"]:
                lines.append(f"| {col} | {info['null_counts'][col]} | {info['type_errors'][col]} |")
            lines.append("")
        lines += ["## Duplicates (repeated primary keys)", ""]
        lines += [f"- {k}: {v}" for k, v in r["duplicates"].items()]
        lines += ["", "## Orphans", ""]
        lines += [f"- {k.replace('_', ' ')}: {v}" for k, v in r["orphans"].items()]
        self.report_path.parent.mkdir(parents=True, exist_ok=True)
        self.report_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        logger.info("Data quality report written to %s", self.report_path)
