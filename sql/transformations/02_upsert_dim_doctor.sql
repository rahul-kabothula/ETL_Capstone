-- Upsert dim_doctor. One row per distinct doctor_id found in the cleaned appointments.
INSERT INTO dim_doctor (doctor_id)
VALUES (%(doctor_id)s)
ON CONFLICT (doctor_id) DO NOTHING;
