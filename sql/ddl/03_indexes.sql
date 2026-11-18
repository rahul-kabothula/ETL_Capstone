-- Indexes on frequently queried keys (indexes on fact_treatment propagate to partitions).

CREATE INDEX IF NOT EXISTS idx_appointment_patient_id ON dim_appointment (patient_id);
CREATE INDEX IF NOT EXISTS idx_appointment_doctor_id  ON dim_appointment (doctor_id);
CREATE INDEX IF NOT EXISTS idx_appointment_start_time ON dim_appointment (start_time);

CREATE INDEX IF NOT EXISTS idx_fact_treatment_id             ON fact_treatment (treatment_id);
CREATE INDEX IF NOT EXISTS idx_fact_treatment_appointment_id ON fact_treatment (appointment_id);
CREATE INDEX IF NOT EXISTS idx_fact_treatment_patient_id     ON fact_treatment (patient_id);
CREATE INDEX IF NOT EXISTS idx_fact_treatment_doctor_id      ON fact_treatment (doctor_id);
CREATE INDEX IF NOT EXISTS idx_fact_treatment_type           ON fact_treatment (treatment_type);

CREATE INDEX IF NOT EXISTS idx_audit_run_id ON etl_audit_log (run_id);
