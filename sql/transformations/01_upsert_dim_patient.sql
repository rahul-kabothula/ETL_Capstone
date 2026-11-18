-- Upsert dim_patient. Parameters are bound from the cleaned pandas DataFrame, one row per patient.
INSERT INTO dim_patient (patient_id, patient_name, dob, gender)
VALUES (%(patient_id)s, %(patient_name)s, %(dob)s, %(gender)s)
ON CONFLICT (patient_id) DO UPDATE
SET patient_name = EXCLUDED.patient_name,
    dob          = EXCLUDED.dob,
    gender       = EXCLUDED.gender,
    updated_at   = NOW();
