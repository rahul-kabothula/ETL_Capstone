-- Patients with overlapping appointments
SELECT a1.patient_id,
       a1.appointment_id AS appointment_id_1,
       a1.start_time     AS start_time_1,
       a1.end_time       AS end_time_1,
       a2.appointment_id AS appointment_id_2,
       a2.start_time     AS start_time_2,
       a2.end_time       AS end_time_2
FROM dim_appointment a1
JOIN dim_appointment a2
  ON a1.patient_id     = a2.patient_id
 AND a1.appointment_id < a2.appointment_id
 AND a1.start_time     < a2.end_time
 AND a2.start_time     < a1.end_time
ORDER BY a1.patient_id, a1.start_time;
