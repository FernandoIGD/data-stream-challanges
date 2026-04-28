CREATE TABLE IF NOT EXISTS `dataengineer-494617.hr_poc.hired_employees`(
  id INT64 NOT NULL,
  name STRING NOT NULL,
  datetime timestamp NOT NULL,
  department_id INT64 NOT NULL,
  job_id INT64 NOT NULL
);

CREATE TABLE IF NOT EXISTS `dataengineer-494617.hr_poc.departments`(
  id INT64 NOT NULL,
  department STRING NOT NULL
);

CREATE TABLE IF NOT EXISTS `dataengineer-494617.hr_poc.jobs`(
  id INT64 NOT NULL,
  job STRING NOT NULL
);


-- Staging tables
CREATE TABLE IF NOT EXISTS `dataengineer-494617.hr_poc.hired_employees_staging`(
  id STRING,
  name STRING,
  datetime STRING,
  department_id STRING,
  job_id STRING
);

CREATE TABLE IF NOT EXISTS `dataengineer-494617.hr_poc.departments_staging`(
  id STRING,
  department STRING
);

CREATE TABLE IF NOT EXISTS `dataengineer-494617.hr_poc.jobs_staging`(
  id STRING,
  job STRING
);


-- Reject table
CREATE TABLE IF NOT EXISTS `dataengineer-494617.hr_poc.load_reject`(
  table_name STRING,
  row_data JSON,
  rejection_reason STRING,
  rejected_at TIMESTAMP,
  source STRING
);