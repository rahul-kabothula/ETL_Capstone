-- Upsert fact_treatment. visit_date (the partition key) is the appointment start date,
-- derived in pandas when treatments are merged with appointments.
INSERT INTO fact_treatment (
    treatment_id, appointment_id, patient_id, doctor_id,
    treatment_type, duration_minutes, cost, visit_date
)
VALUES (
    %(treatment_id)s, %(appointment_id)s, %(patient_id)s, %(doctor_id)s,
    %(treatment_type)s, %(duration_minutes)s, %(cost)s, %(visit_date)s
)
ON CONFLICT (treatment_id, visit_date) DO UPDATE
SET appointment_id   = EXCLUDED.appointment_id,
    patient_id       = EXCLUDED.patient_id,
    doctor_id        = EXCLUDED.doctor_id,
    treatment_type   = EXCLUDED.treatment_type,
    duration_minutes = EXCLUDED.duration_minutes,
    cost             = EXCLUDED.cost;
