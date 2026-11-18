-- Total treatment cost per patient
SELECT p.patient_id,
       p.patient_name,
       COUNT(f.treatment_id)       AS treatment_count,
       COALESCE(SUM(f.cost), 0)    AS total_cost
FROM dim_patient p
LEFT JOIN fact_treatment f ON f.patient_id = p.patient_id
GROUP BY p.patient_id, p.patient_name
ORDER BY total_cost DESC;
