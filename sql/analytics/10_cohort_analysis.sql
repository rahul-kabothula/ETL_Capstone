-- Cohort analysis: patients grouped by signup month vs treatment frequency.
-- No signup date exists in the source data, so the month of a patient's first appointment is used as signup month.
WITH signup AS (
    SELECT patient_id,
           date_trunc('month', MIN(start_time))::DATE AS signup_month
    FROM dim_appointment
    GROUP BY patient_id
),
patient_stats AS (
    SELECT s.patient_id,
           s.signup_month,
           COUNT(DISTINCT a.appointment_id) AS visits,
           COUNT(f.treatment_id)            AS treatments
    FROM signup s
    JOIN dim_appointment a ON a.patient_id = s.patient_id
    LEFT JOIN fact_treatment f ON f.appointment_id = a.appointment_id
    GROUP BY s.patient_id, s.signup_month
)
SELECT signup_month,
       COUNT(*)                         AS patients,
       SUM(treatments)                  AS total_treatments,
       ROUND(AVG(visits), 2)            AS avg_visits_per_patient,
       ROUND(AVG(treatments), 2)        AS avg_treatments_per_patient
FROM patient_stats
GROUP BY signup_month
ORDER BY signup_month;
