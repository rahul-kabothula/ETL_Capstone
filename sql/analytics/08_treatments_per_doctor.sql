-- Treatments per doctor
SELECT d.doctor_id,
       COUNT(f.treatment_id)       AS treatment_count,
       COUNT(DISTINCT f.patient_id) AS unique_patients,
       COALESCE(SUM(f.cost), 0)    AS total_cost
FROM dim_doctor d
LEFT JOIN fact_treatment f ON f.doctor_id = d.doctor_id
GROUP BY d.doctor_id
ORDER BY treatment_count DESC;
