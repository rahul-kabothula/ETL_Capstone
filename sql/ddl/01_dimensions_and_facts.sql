-- Star schema: dim_patient, dim_doctor, dim_appointment, fact_treatment

CREATE TABLE IF NOT EXISTS dim_patient (
    patient_id    INTEGER      PRIMARY KEY,
    patient_name  VARCHAR(150),
    dob           DATE,
    gender        CHAR(1)      CHECK (gender IN ('M', 'F')),
    created_at    TIMESTAMP    NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMP    NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS dim_doctor (
    doctor_id   INTEGER    PRIMARY KEY,
    created_at  TIMESTAMP  NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS dim_appointment (
    appointment_id    INTEGER    PRIMARY KEY,
    patient_id        INTEGER    NOT NULL REFERENCES dim_patient (patient_id),
    doctor_id         INTEGER    NOT NULL REFERENCES dim_doctor (doctor_id),
    start_time        TIMESTAMP  NOT NULL,
    end_time          TIMESTAMP  NOT NULL,
    duration_minutes  NUMERIC(8, 2) NOT NULL,
    created_at        TIMESTAMP  NOT NULL DEFAULT NOW(),
    updated_at        TIMESTAMP  NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_appointment_natural_key UNIQUE (patient_id, doctor_id, start_time),
    CONSTRAINT ck_appointment_times CHECK (end_time > start_time),
    CONSTRAINT ck_appointment_duration CHECK (duration_minutes > 0)
);

-- Fact table partitioned by month on visit_date.
-- The partition key must be part of the primary key.
CREATE TABLE IF NOT EXISTS fact_treatment (
    treatment_id      INTEGER      NOT NULL,
    appointment_id    INTEGER      NOT NULL REFERENCES dim_appointment (appointment_id),
    patient_id        INTEGER      NOT NULL REFERENCES dim_patient (patient_id),
    doctor_id         INTEGER      NOT NULL REFERENCES dim_doctor (doctor_id),
    treatment_type    VARCHAR(50)  NOT NULL,
    duration_minutes  INTEGER,
    cost              NUMERIC(12, 2),
    visit_date        DATE         NOT NULL,
    created_at        TIMESTAMP    NOT NULL DEFAULT NOW(),
    PRIMARY KEY (treatment_id, visit_date),
    CONSTRAINT ck_treatment_duration CHECK (duration_minutes IS NULL OR duration_minutes > 0),
    CONSTRAINT ck_treatment_cost CHECK (cost IS NULL OR cost >= 0)
) PARTITION BY RANGE (visit_date);

-- Monthly partitions: 36 months back to 12 months ahead of the current month.
-- Existing partitions are skipped, so this is safe to re-run.
DO $$
DECLARE
    month_start DATE;
    month_end   DATE;
    part_name   TEXT;
BEGIN
    FOR month_start IN
        SELECT generate_series(
            (date_trunc('month', CURRENT_DATE) - INTERVAL '36 months')::DATE,
            (date_trunc('month', CURRENT_DATE) + INTERVAL '12 months')::DATE,
            INTERVAL '1 month'
        )::DATE
    LOOP
        month_end := (month_start + INTERVAL '1 month')::DATE;
        part_name := 'fact_treatment_' || to_char(month_start, 'YYYY_MM');
        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS %I PARTITION OF fact_treatment FOR VALUES FROM (%L) TO (%L)',
            part_name, month_start, month_end
        );
    END LOOP;
END
$$;
