-- Upsert dim_appointment. Orphans and invalid durations are already removed in pandas;
-- the foreign keys and CHECK constraints on the table enforce the same rules in the database.
INSERT INTO dim_appointment (appointment_id, patient_id, doctor_id, start_time, end_time, duration_minutes)
VALUES (%(appointment_id)s, %(patient_id)s, %(doctor_id)s, %(start_time)s, %(end_time)s, %(duration_minutes)s)
ON CONFLICT (appointment_id) DO UPDATE
SET patient_id       = EXCLUDED.patient_id,
    doctor_id        = EXCLUDED.doctor_id,
    start_time       = EXCLUDED.start_time,
    end_time         = EXCLUDED.end_time,
    duration_minutes = EXCLUDED.duration_minutes,
    updated_at       = NOW();
