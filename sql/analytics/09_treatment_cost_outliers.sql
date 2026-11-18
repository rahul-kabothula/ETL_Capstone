-- Treatment cost outliers: top 1% by cost
WITH cutoff AS (
    SELECT percentile_cont(0.99) WITHIN GROUP (ORDER BY cost) AS p99
    FROM fact_treatment
    WHERE cost IS NOT NULL
)
SELECT f.treatment_id,
       f.appointment_id,
       f.patient_id,
       f.doctor_id,
       f.treatment_type,
       f.cost,
       c.p99 AS p99_cutoff
FROM fact_treatment f
CROSS JOIN cutoff c
WHERE f.cost >= c.p99
ORDER BY f.cost DESC;
