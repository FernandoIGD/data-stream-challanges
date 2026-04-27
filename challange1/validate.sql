-- Reset target tables so the script is fully re-runnable.
TRUNCATE TABLE `dataengineer-494617.hr_poc.hired_employees`;
TRUNCATE TABLE `dataengineer-494617.hr_poc.departments`;
TRUNCATE TABLE `dataengineer-494617.hr_poc.jobs`;
TRUNCATE TABLE `dataengineer-494617.hr_poc.load_reject`;


-- hired_employees: valid rows
INSERT INTO `dataengineer-494617.hr_poc.hired_employees`
  (id, name, datetime, department_id, job_id)
SELECT
  CAST(id AS INT64),
  name,
  CAST(datetime AS TIMESTAMP),
  CAST(department_id AS INT64),
  CAST(job_id AS INT64)
FROM `dataengineer-494617.hr_poc.hired_employees_staging`
WHERE SAFE_CAST(id AS INT64)            IS NOT NULL
  AND name                              IS NOT NULL
  AND SAFE_CAST(datetime AS TIMESTAMP)  IS NOT NULL
  AND SAFE_CAST(department_id AS INT64) IS NOT NULL
  AND SAFE_CAST(job_id AS INT64)        IS NOT NULL;

-- hired_employees: invalid rows
INSERT INTO `dataengineer-494617.hr_poc.load_reject`
  (table_name, row_data, rejection_reason, rejected_at, source)
SELECT
  'hired_employees',
  TO_JSON(STRUCT(id, name, datetime, department_id, job_id)),
  CONCAT('missing or invalid: ', ARRAY_TO_STRING([
    IF(SAFE_CAST(id AS INT64)            IS NULL, 'id',            NULL),
    IF(name                              IS NULL, 'name',          NULL),
    IF(SAFE_CAST(datetime AS TIMESTAMP)  IS NULL, 'datetime',      NULL),
    IF(SAFE_CAST(department_id AS INT64) IS NULL, 'department_id', NULL),
    IF(SAFE_CAST(job_id AS INT64)        IS NULL, 'job_id',        NULL)
  ], ', ')),
  CURRENT_TIMESTAMP(),
  'csv_load'
FROM `dataengineer-494617.hr_poc.hired_employees_staging`
WHERE NOT (
      SAFE_CAST(id AS INT64)            IS NOT NULL
  AND name                              IS NOT NULL
  AND SAFE_CAST(datetime AS TIMESTAMP)  IS NOT NULL
  AND SAFE_CAST(department_id AS INT64) IS NOT NULL
  AND SAFE_CAST(job_id AS INT64)        IS NOT NULL
);


-- departments: valid rows
INSERT INTO `dataengineer-494617.hr_poc.departments`
  (id, department)
SELECT
  CAST(id AS INT64),
  department
FROM `dataengineer-494617.hr_poc.departments_staging`
WHERE SAFE_CAST(id AS INT64) IS NOT NULL
  AND department             IS NOT NULL;

-- departments: invalid rows
INSERT INTO `dataengineer-494617.hr_poc.load_reject`
  (table_name, row_data, rejection_reason, rejected_at, source)
SELECT
  'departments',
  TO_JSON(STRUCT(id, department)),
  CONCAT('missing or invalid: ', ARRAY_TO_STRING([
    IF(SAFE_CAST(id AS INT64) IS NULL, 'id',         NULL),
    IF(department             IS NULL, 'department', NULL)
  ], ', ')),
  CURRENT_TIMESTAMP(),
  'csv_load'
FROM `dataengineer-494617.hr_poc.departments_staging`
WHERE NOT (
      SAFE_CAST(id AS INT64) IS NOT NULL
  AND department             IS NOT NULL
);


-- jobs: valid rows
INSERT INTO `dataengineer-494617.hr_poc.jobs`
  (id, job)
SELECT
  CAST(id AS INT64),
  job
FROM `dataengineer-494617.hr_poc.jobs_staging`
WHERE SAFE_CAST(id AS INT64) IS NOT NULL
  AND job                    IS NOT NULL;

-- jobs: invalid rows
INSERT INTO `dataengineer-494617.hr_poc.load_reject`
  (table_name, row_data, rejection_reason, rejected_at, source)
SELECT
  'jobs',
  TO_JSON(STRUCT(id, job)),
  CONCAT('missing or invalid: ', ARRAY_TO_STRING([
    IF(SAFE_CAST(id AS INT64) IS NULL, 'id',  NULL),
    IF(job                    IS NULL, 'job', NULL)
  ], ', ')),
  CURRENT_TIMESTAMP(),
  'csv_load'
FROM `dataengineer-494617.hr_poc.jobs_staging`
WHERE NOT (
      SAFE_CAST(id AS INT64) IS NOT NULL
  AND job                    IS NOT NULL
);
