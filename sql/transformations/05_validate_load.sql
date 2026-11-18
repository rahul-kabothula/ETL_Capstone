-- Post-load validation. Every check should return violations = 0.
SELECT 'orphan_treatments' AS check_name, COUNT(*) AS violations
FROM fact_treatment f
LEFT JOIN dim_appointment a ON a.appointment_id = f.appointment_id
WHERE a.appointment_id IS NULL

UNION ALL

SELECT 'duplicate_appointment_ids', COUNT(*)
FROM (
    SELECT appointment_id
    FROM dim_appointment
    GROUP BY appointment_id
    HAVING COUNT(*) > 1
) d

UNION ALL

SELECT 'invalid_appointment_durations', COUNT(*)
FROM dim_appointment
WHERE end_time <= start_time
   OR duration_minutes <= 0;
