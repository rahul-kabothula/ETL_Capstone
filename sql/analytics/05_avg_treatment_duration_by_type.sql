-- Average treatment duration by treatment type
SELECT treatment_type,
       COUNT(duration_minutes)               AS treatments_with_duration,
       ROUND(AVG(duration_minutes), 2)       AS avg_duration_minutes,
       MIN(duration_minutes)                 AS min_duration_minutes,
       MAX(duration_minutes)                 AS max_duration_minutes
FROM fact_treatment
GROUP BY treatment_type
ORDER BY avg_duration_minutes DESC;
