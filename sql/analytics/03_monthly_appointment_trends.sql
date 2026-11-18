-- Monthly appointment trends with month-over-month change
WITH monthly AS (
    SELECT date_trunc('month', start_time)::DATE AS month,
           COUNT(*)                              AS appointment_count,
           COUNT(DISTINCT patient_id)            AS unique_patients
    FROM dim_appointment
    GROUP BY 1
)
SELECT month,
       appointment_count,
       unique_patients,
       appointment_count - LAG(appointment_count) OVER (ORDER BY month) AS mom_change
FROM monthly
ORDER BY month;
