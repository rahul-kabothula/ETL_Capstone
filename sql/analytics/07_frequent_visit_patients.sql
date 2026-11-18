-- Patients with frequent visits: more than :threshold appointments in a calendar month
SELECT patient_id,
       date_trunc('month', start_time)::DATE AS month,
       COUNT(*)                              AS visits
FROM dim_appointment
GROUP BY patient_id, date_trunc('month', start_time)
HAVING COUNT(*) > :threshold
ORDER BY visits DESC, patient_id, month;
