"""Reusable, side-effect-free helpers for the healthcare ETL (schema checks, cleaning, metrics)."""

import logging
from typing import Dict, Iterable, List, Optional, Tuple

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Expected schemas: column -> logical type (int | float | str | datetime)
# ---------------------------------------------------------------------------
PATIENT_SCHEMA = {"patient_id": "int", "patient_name": "str", "dob": "datetime", "gender": "str"}
APPOINTMENT_SCHEMA = {"appointment_id": "int", "patient_id": "int", "doctor_id": "int",
                      "start_time": "datetime", "end_time": "datetime"}
TREATMENT_SCHEMA = {"treatment_id": "int", "appointment_id": "int", "treatment_type": "str",
                    "duration_minutes": "int", "cost": "float"}

_NAME_SUFFIXES = {"Md": "MD", "Dds": "DDS", "Dvm": "DVM", "Phd": "PhD",
                  "Ii": "II", "Iii": "III", "Iv": "IV"}


class SchemaError(ValueError):
    """Raised when a dataset is missing required columns."""


# ---------------------------------------------------------------------------
# Reading and type coercion
# ---------------------------------------------------------------------------
def read_csv_safe(path) -> Tuple[pd.DataFrame, int]:
    """Read a CSV as strings, skipping corrupt lines. Returns (frame, corrupt_line_count)."""
    corrupt: List[list] = []

    def _skip(line):
        corrupt.append(line)
        return None

    df = pd.read_csv(path, dtype=str, engine="python", on_bad_lines=_skip)
    if corrupt:
        logger.warning("%s: skipped %d corrupt line(s)", path, len(corrupt))
    return df, len(corrupt)


def coerce_series(series: pd.Series, kind: str) -> pd.Series:
    """Convert a raw series to its logical type; unparseable values become null."""
    s = series.astype("string").str.strip()
    s = s.mask(s == "", pd.NA)
    if kind == "int":
        num = pd.to_numeric(s, errors="coerce").astype("float64")
        return num.where(num % 1 == 0).astype("Int64")
    if kind == "float":
        return pd.to_numeric(s, errors="coerce").astype("float64")
    if kind == "datetime":
        return pd.to_datetime(s, errors="coerce", format="mixed")
    return s


def coerce_types(df: pd.DataFrame, schema: Dict[str, str]) -> pd.DataFrame:
    """Return a frame holding only the schema columns, converted to their logical types."""
    return pd.DataFrame({col: coerce_series(df[col], kind) for col, kind in schema.items()})


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------
def validate_schema(df: pd.DataFrame, schema: Dict[str, str], name: str = "dataset") -> dict:
    """Check columns and value types of a raw (string) frame.

    Raises SchemaError for missing columns. Otherwise returns a report with
    row count, extra columns, null counts and per-column type errors
    (non-null values that cannot be converted to the expected type).
    """
    missing = [c for c in schema if c not in df.columns]
    if missing:
        raise SchemaError(f"{name}: missing required column(s): {missing}")

    null_counts, type_errors = {}, {}
    for col, kind in schema.items():
        raw_has_value = df[col].astype("string").str.strip().replace("", pd.NA).notna()
        converted = coerce_series(df[col], kind)
        null_counts[col] = int((~raw_has_value).sum())
        type_errors[col] = int((raw_has_value & converted.isna()).sum())
    return {
        "rows": int(len(df)),
        "extra_columns": [c for c in df.columns if c not in schema],
        "null_counts": null_counts,
        "type_errors": type_errors,
    }


def detect_duplicates(df: pd.DataFrame, subset: List[str]) -> pd.Series:
    """Boolean mask of repeated rows (2nd occurrence onward) by key; rows with null keys are ignored."""
    has_key = df[subset].notna().all(axis=1)
    return df.duplicated(subset=subset, keep="first") & has_key


def find_orphans(child: pd.DataFrame, fk: str, parent: pd.DataFrame, pk: str) -> pd.Series:
    """Mask of child rows whose non-null foreign key is absent from the parent's key."""
    return child[fk].notna() & ~child[fk].isin(parent[pk].dropna())


# ---------------------------------------------------------------------------
# Cleaning
# ---------------------------------------------------------------------------
def normalize_name(value) -> Optional[str]:
    """Collapse whitespace and title-case a name (keeps suffixes such as MD / PhD)."""
    if value is None or pd.isna(value):
        return None
    text = " ".join(str(value).split())
    if not text:
        return None
    return " ".join(_NAME_SUFFIXES.get(w, w) for w in text.title().split())


def normalize_gender(value) -> Optional[str]:
    """Map to 'M' / 'F'; anything else becomes null."""
    if value is None or pd.isna(value):
        return None
    key = str(value).strip().upper()
    return {"M": "M", "MALE": "M", "F": "F", "FEMALE": "F"}.get(key)


def normalize_patients(df: pd.DataFrame, today: Optional[pd.Timestamp] = None) -> pd.DataFrame:
    """Fix names/gender, standardise DOB to a date, and null out future dates of birth."""
    today = today or pd.Timestamp.today().normalize()
    out = df.copy()
    out["patient_name"] = out["patient_name"].map(normalize_name)
    out["gender"] = out["gender"].map(normalize_gender)
    dob = pd.to_datetime(out["dob"], errors="coerce").dt.normalize()
    out["dob"] = dob.where(dob <= today)
    return out


def handle_missing(df: pd.DataFrame, fill_values: Optional[dict] = None,
                   drop_subset: Optional[List[str]] = None) -> pd.DataFrame:
    """Fill nulls with the given per-column values and/or drop rows with nulls in drop_subset."""
    out = df.copy()
    if fill_values:
        for col, value in fill_values.items():
            out[col] = out[col].fillna(value)
    if drop_subset:
        out = out.dropna(subset=drop_subset)
    return out


def drop_invalid_rows(df: pd.DataFrame, required: List[str],
                      extra_valid: Optional[pd.Series] = None) -> Tuple[pd.DataFrame, int]:
    """Keep rows with all required columns non-null (and extra_valid True). Returns (clean, dropped)."""
    valid = df[required].notna().all(axis=1)
    if extra_valid is not None:
        valid &= extra_valid.fillna(False).astype(bool)
    return df[valid].copy(), int((~valid).sum())


def deduplicate(df: pd.DataFrame, subset: List[str]) -> Tuple[pd.DataFrame, int]:
    """Drop repeated keys, keeping the first occurrence. Returns (clean, removed)."""
    dup = detect_duplicates(df, subset)
    return df[~dup].copy(), int(dup.sum())


# ---------------------------------------------------------------------------
# Metrics
# ---------------------------------------------------------------------------
def compute_visit_duration(start: pd.Series, end: pd.Series) -> pd.Series:
    """Visit duration in minutes (vectorised); null when either timestamp is missing."""
    delta = np.asarray(end - start, dtype="timedelta64[ns]")
    minutes = delta / np.timedelta64(1, "m")
    return pd.Series(np.round(minutes, 2), index=start.index, name="duration_minutes")


def aggregate_treatment_cost(df: pd.DataFrame, by: str) -> pd.DataFrame:
    """Treatment count, total and average cost grouped by a column (e.g. patient_id, appointment_id)."""
    return (
        df.groupby(by, observed=True)
        .agg(treatment_count=("treatment_id", "count"),
             total_cost=("cost", "sum"),
             avg_cost=("cost", "mean"))
        .reset_index()
    )


def _finite(series: pd.Series) -> np.ndarray:
    values = series.to_numpy(dtype="float64", na_value=np.nan)
    return values[~np.isnan(values)]


def summary_statistics(appointments: pd.DataFrame, treatments: pd.DataFrame) -> Dict[str, float]:
    """NumPy summaries: mean cost, median visit duration, std of treatment duration."""
    cost = _finite(treatments["cost"])
    visit = _finite(appointments["duration_minutes"])
    treat = _finite(treatments["duration_minutes"])
    return {
        "avg_treatment_cost": float(np.mean(cost)) if cost.size else float("nan"),
        "median_visit_duration": float(np.median(visit)) if visit.size else float("nan"),
        "std_treatment_duration": float(np.std(treat, ddof=1)) if treat.size > 1 else float("nan"),
    }


def flag_cost_outliers(cost: pd.Series, percentile: float = 99) -> Tuple[pd.Series, float]:
    """Mask of costs at or above the given percentile (NumPy), plus the cutoff value."""
    values = cost.to_numpy(dtype="float64", na_value=np.nan)
    finite = values[~np.isnan(values)]
    if finite.size == 0:
        return pd.Series(False, index=cost.index), float("nan")
    cutoff = float(np.percentile(finite, percentile))
    return pd.Series(values >= cutoff, index=cost.index), cutoff


def find_frequent_visitors(appointments: pd.DataFrame, threshold: int) -> pd.DataFrame:
    """Patient-months with more than `threshold` appointments."""
    month = appointments["start_time"].dt.to_period("M").dt.to_timestamp().rename("month")
    counts = (appointments.groupby([appointments["patient_id"], month], observed=True)
              .size().rename("visits").reset_index())
    return counts[counts["visits"] > threshold].sort_values(
        ["visits", "patient_id"], ascending=[False, True]).reset_index(drop=True)


def find_overlapping_appointments(appointments: pd.DataFrame) -> pd.DataFrame:
    """Appointments that overlap another appointment of the same patient (vectorised)."""
    df = appointments.sort_values(["patient_id", "start_time", "end_time"]).copy()
    by_patient = df.groupby("patient_id", sort=False)
    prev_max_end = by_patient["end_time"].cummax().groupby(df["patient_id"], sort=False).shift()
    next_start = by_patient["start_time"].shift(-1)
    overlaps = (df["start_time"] < prev_max_end) | (next_start < df["end_time"])
    return df[overlaps].reset_index(drop=True)


def monthly_summary(appointments: pd.DataFrame, treatment_visits: pd.DataFrame) -> pd.DataFrame:
    """Monthly appointment count, treatment count and total cost."""
    def month_of(frame):
        return frame["start_time"].dt.to_period("M").dt.to_timestamp()

    appts = appointments.groupby(month_of(appointments)).size().rename("appointment_count")
    treats = treatment_visits.groupby(month_of(treatment_visits)).agg(
        treatment_count=("treatment_id", "count"), total_cost=("cost", "sum"))
    out = pd.concat([appts, treats], axis=1).fillna(0)
    out[["appointment_count", "treatment_count"]] = out[["appointment_count", "treatment_count"]].astype(int)
    return out.rename_axis("month").reset_index().sort_values("month").reset_index(drop=True)


def optimize_memory(df: pd.DataFrame, categorical_cols: Iterable[str]) -> pd.DataFrame:
    """Convert low-cardinality columns to the categorical dtype."""
    out = df.copy()
    for col in categorical_cols:
        if col in out.columns:
            out[col] = out[col].astype("category")
    before = df.memory_usage(deep=True).sum()
    after = out.memory_usage(deep=True).sum()
    logger.info("Memory %.1f KB -> %.1f KB (categorical: %s)", before / 1024, after / 1024, list(categorical_cols))
    return out
