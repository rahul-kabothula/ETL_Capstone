CREATE TABLE IF NOT EXISTS etl_audit_log (
    audit_id              SERIAL     PRIMARY KEY,
    run_id                VARCHAR(36) NOT NULL,
    run_timestamp         TIMESTAMP  NOT NULL DEFAULT NOW(),
    rows_extracted        INTEGER    NOT NULL DEFAULT 0,
    rows_transformed      INTEGER    NOT NULL DEFAULT 0,
    rows_loaded           INTEGER    NOT NULL DEFAULT 0,
    invalid_rows          INTEGER    NOT NULL DEFAULT 0,
    duplicate_patients    INTEGER    NOT NULL DEFAULT 0,
    duplicate_appointments INTEGER   NOT NULL DEFAULT 0,
    duplicate_treatments  INTEGER    NOT NULL DEFAULT 0,
    orphan_appointments   INTEGER    NOT NULL DEFAULT 0,
    orphan_treatments     INTEGER    NOT NULL DEFAULT 0,
    status                VARCHAR(20) NOT NULL DEFAULT 'SUCCESS'
);
