import pandas as pd
import pytest

from etl import Appointment, HealthcareETL, Patient
from etl import data_utils as du


def ts(s):
    return pd.Timestamp(s)


# ---- required unit tests -------------------------------------------------
def test_visit_duration_computation():
    start = pd.Series(pd.to_datetime(["2026-01-01 09:00", "2026-01-01 10:00", None]))
    end = pd.Series(pd.to_datetime(["2026-01-01 09:45", "2026-01-01 11:30", "2026-01-01 12:00"]))
    result = du.compute_visit_duration(start, end)
    assert result.iloc[0] == 45
    assert result.iloc[1] == 90
    assert pd.isna(result.iloc[2])


def test_appointment_duration_minutes():
    assert Appointment(1, 1, "2026-01-01 09:00", "2026-01-01 09:30", 5).duration_minutes() == 30
    assert Appointment(1, 1, "2026-01-01 09:00", "invalid_timestamp", 5).duration_minutes() is None


def test_treatment_cost_aggregation():
    df = pd.DataFrame({
        "treatment_id": [1, 2, 3, 4],
        "patient_id": [1, 1, 2, 2],
        "cost": [100.0, 50.0, 200.0, None],
    })
    out = du.aggregate_treatment_cost(df, "patient_id").set_index("patient_id")
    assert out.loc[1, "total_cost"] == 150.0
    assert out.loc[1, "treatment_count"] == 2
    assert out.loc[2, "total_cost"] == 200.0   # null cost is ignored
    assert out.loc[2, "avg_cost"] == 200.0


def test_patient_duplicate_detection():
    df = pd.DataFrame({"patient_id": pd.array([1, 2, 2, None, None, 3], dtype="Int64")})
    mask = du.detect_duplicates(df, ["patient_id"])
    assert mask.tolist() == [False, False, True, False, False, False]   # null ids are not duplicates
    clean, removed = du.deduplicate(df, ["patient_id"])
    assert removed == 1 and len(clean) == 5


# ---- supporting tests ----------------------------------------------------
def test_validate_schema_missing_column():
    with pytest.raises(du.SchemaError):
        du.validate_schema(pd.DataFrame({"patient_id": ["1"]}), du.PATIENT_SCHEMA, "patients")


def test_validate_schema_type_errors():
    raw = pd.DataFrame({"patient_id": ["1", "x"], "patient_name": ["A", None],
                        "dob": ["1990-01-01", "invalid_date"], "gender": ["M", "F"]})
    report = du.validate_schema(raw, du.PATIENT_SCHEMA, "patients")
    assert report["type_errors"]["patient_id"] == 1
    assert report["type_errors"]["dob"] == 1
    assert report["null_counts"]["patient_name"] == 1


def test_normalize_patients():
    df = pd.DataFrame({"patient_id": [1], "patient_name": ["  brandy   moses md "],
                       "dob": ["1990-05-01"], "gender": [" f "]})
    out = du.normalize_patients(df).iloc[0]
    assert out["patient_name"] == "Brandy Moses MD"
    assert out["gender"] == "F"
    assert out["dob"] == ts("1990-05-01")
    assert Patient(1, " john  doe", "bad", "x").normalized().dob is None


def test_overlapping_appointments():
    df = pd.DataFrame({
        "appointment_id": [1, 2, 3, 4],
        "patient_id": [1, 1, 1, 2],
        "start_time": pd.to_datetime(["2026-01-01 09:00", "2026-01-01 09:30", "2026-01-01 12:00", "2026-01-01 09:00"]),
        "end_time": pd.to_datetime(["2026-01-01 10:00", "2026-01-01 10:30", "2026-01-01 13:00", "2026-01-01 10:00"]),
    })
    assert sorted(du.find_overlapping_appointments(df)["appointment_id"]) == [1, 2]
    assert Appointment(1, 1, df.start_time[0], df.end_time[0], 1).overlaps(
        Appointment(2, 1, df.start_time[1], df.end_time[1], 1))


def test_cost_outliers_and_stats():
    cost = pd.Series([1.0] * 99 + [1000.0, None])
    mask, cutoff = du.flag_cost_outliers(cost, 99)
    assert mask.sum() == 1 and cost[mask].iloc[0] == 1000.0 and cutoff > 1
    stats = du.summary_statistics(
        pd.DataFrame({"duration_minutes": [30.0, 60.0, 90.0]}),
        pd.DataFrame({"cost": [10.0, 20.0, None], "duration_minutes": [10, 20, 30]}))
    assert stats["avg_treatment_cost"] == 15.0
    assert stats["median_visit_duration"] == 60.0
    assert stats["std_treatment_duration"] == pytest.approx(10.0)


def test_frequent_visitors():
    df = pd.DataFrame({"patient_id": [1, 1, 1, 2],
                       "start_time": pd.to_datetime(["2026-01-01", "2026-01-05", "2026-01-09", "2026-01-02"])})
    out = du.find_frequent_visitors(df, threshold=2)
    assert out["patient_id"].tolist() == [1] and out["visits"].tolist() == [3]


# ---- end-to-end (no database) -------------------------------------------
def test_pipeline_without_db(tmp_path):
    (tmp_path / "p.csv").write_text(
        "patient_id,patient_name,dob,gender\n"
        "1,john doe,1990-01-01,M\n"
        "1,dup person,1980-01-01,F\n"
        "2,,invalid_date,\n")
    (tmp_path / "a.csv").write_text(
        "appointment_id,patient_id,doctor_id,start_time,end_time\n"
        "1,1,5,2026-01-01 09:00:00,2026-01-01 10:00:00\n"
        "2,1,5,2026-01-01 09:30:00,2026-01-01 11:00:00\n"
        "3,9,5,2026-01-02 09:00:00,2026-01-02 10:00:00\n"      # orphan patient
        "4,,5,2026-01-03 09:00:00,2026-01-03 10:00:00\n"       # null patient
        "5,2,6,2026-01-04 09:00:00,invalid_timestamp\n")       # bad end time
    (tmp_path / "t.csv").write_text(
        "treatment_id,appointment_id,treatment_type,duration_minutes,cost\n"
        "1,1,surgery,30,100.5\n"
        "2,2,Therapy,,\n"
        "3,99,Therapy,10,5\n")                                  # orphan appointment
    etl = HealthcareETL(tmp_path / "p.csv", tmp_path / "a.csv", tmp_path / "t.csv",
                        report_path=tmp_path / "report.md")
    etl.extract()
    etl.validate()
    etl.transform()
    etl.enrich()

    assert etl.metrics["rows_extracted"] == 11
    assert etl.metrics["duplicate_patients"] == 1
    assert etl.metrics["orphan_appointments"] == 1
    assert etl.metrics["orphan_treatments"] == 1
    assert len(etl.patients_df) == 2 and len(etl.appointments_df) == 2 and len(etl.treatments_df) == 2
    assert etl.patients_df.loc[etl.patients_df.patient_id == 2, "patient_name"].iloc[0] == "Unknown"
    assert sorted(etl.treatments_df["treatment_type"].astype(str)) == ["Surgery", "Therapy"]
    assert len(etl.overlapping_appointments) == 2
    assert etl.metrics["rows_transformed"] == 6
    assert (tmp_path / "report.md").exists()
