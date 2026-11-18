# Data Quality Report

Run ID: `3563926a-3d09-4273-ae01-bd7087438f03`

## patients

- Rows: 1100
- Corrupt lines skipped: 0
- Extra columns: none

| Column | Missing values | Invalid values (wrong type / date) |
|---|---|---|
| patient_id | 0 | 0 |
| patient_name | 61 | 0 |
| dob | 0 | 62 |
| gender | 326 | 0 |

## appointments

- Rows: 5500
- Corrupt lines skipped: 0
- Extra columns: none
- Appointments with end <= start: 0

| Column | Missing values | Invalid values (wrong type / date) |
|---|---|---|
| appointment_id | 0 | 0 |
| patient_id | 264 | 0 |
| doctor_id | 0 | 0 |
| start_time | 0 | 0 |
| end_time | 0 | 245 |

## treatments

- Rows: 7500
- Corrupt lines skipped: 0
- Extra columns: none

| Column | Missing values | Invalid values (wrong type / date) |
|---|---|---|
| treatment_id | 0 | 0 |
| appointment_id | 0 | 0 |
| treatment_type | 0 | 0 |
| duration_minutes | 341 | 0 |
| cost | 341 | 0 |

## Duplicates (repeated primary keys)

- patients: 28
- appointments: 172
- treatments: 225

## Orphans

- appointments missing patient: 589
- treatments missing appointment: 341
