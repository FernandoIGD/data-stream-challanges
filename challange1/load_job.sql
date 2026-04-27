LOAD DATA OVERWRITE `dataengineer-494617.hr_poc.hired_employees_staging`
(id STRING, name STRING, datetime STRING, department_id STRING, job_id STRING)
FROM FILES (
  format = 'CSV',
  uris = ['gs://hr-poc-data/raw/hired_employees.csv'],
  field_delimiter = ',',
  skip_leading_rows = 0,
  null_marker = "",
  max_bad_records = 0
);

LOAD DATA OVERWRITE `dataengineer-494617.hr_poc.departments_staging`
(id STRING, department STRING)
FROM FILES (
  format = 'CSV',
  uris = ['gs://hr-poc-data/raw/departments.csv'],
  field_delimiter = ',',
  skip_leading_rows = 0,
  null_marker = "",
  max_bad_records = 0
);

LOAD DATA OVERWRITE `dataengineer-494617.hr_poc.jobs_staging`
(id STRING, job STRING)
FROM FILES (
  format = 'CSV',
  uris = ['gs://hr-poc-data/raw/jobs.csv'],
  field_delimiter = ',',
  skip_leading_rows = 0,
  null_marker = "",
  max_bad_records = 0
);
