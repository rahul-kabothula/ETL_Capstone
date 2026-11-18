-- Average visits per patient (patients with at least one visit)
SELECT COUNT(*)                          AS patients_with_visits,
       SUM(visits)                       AS total_visits,
       ROUND(AVG(visits), 2)             AS avg_visits_per_patient
FROM (
    SELECT patient_id, COUNT(*) AS visits
    FROM dim_appointment
    GROUP BY patient_id
) v;
