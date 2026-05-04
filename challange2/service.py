from google.cloud import bigquery

_client = bigquery.Client()

def get_employees_for_each_job() -> list[dict]:
    '''
    This function returns a list of dictionaries, where each dictionary contains the department, 
    job, and the number of employees hired in each quarter of 2021. The data is retrieved from 
    the BigQuery database using a SQL query that joins the hired_employees, departments, and jobs tables. 
    The query counts the number of employees hired in each quarter and groups the results by department and job.

    Args:
        None
    Returns:
        list[dict]: A list of dictionaries, where each dictionary contains the department, job,
        and the number of employees hired in each quarter of 2021.
    '''
    query = """
            SELECT d.department, j.job,
            COUNTIF(e.datetime >= '2021-01-01' AND e.datetime < '2021-04-01') AS q1,
            COUNTIF(e.datetime >= '2021-04-01' AND e.datetime < '2021-07-01') AS q2,
            COUNTIF(e.datetime >= '2021-07-01' AND e.datetime < '2021-10-01') AS q3,
            COUNTIF(e.datetime >= '2021-10-01' AND e.datetime < '2022-01-01') AS q4
            FROM `dataengineer-494617.hr_poc.hired_employees` e LEFT JOIN
            `dataengineer-494617.hr_poc.departments` d ON e.department_id = d.id LEFT JOIN
            `dataengineer-494617.hr_poc.jobs` j ON e.job_id = j.id
            WHERE e.datetime >= '2021-01-01' AND e.datetime < '2022-01-01'
            GROUP BY d.department, j.job
            ORDER BY d.department, j.job
            """
    return [dict(row) for row in _client.query(query).result()]
    
def list_ids_names_numbers() -> list[dict]:
    '''
    This function returns a list of dictionaries, where each dictionary contains the department id,
    department name, and the number of employees hired in 2021 that are greater than the mean of hired employees 
    for all departments. The data is retrieved from the BigQuery database using a SQL query that joins the 
    hired_employees and departments tables.

    Args:
        None
    Returns:
        list[dict]: A list of dictionaries, where each dictionary contains the department id,
        department name, and the number of employees hired in 2021.
    '''
    query = """
            WITH table1 AS (
            SELECT COUNT(id) as emp_hired FROM `dataengineer-494617.hr_poc.hired_employees`
            WHERE datetime >= '2021-01-01' AND datetime < '2022-01-01'
            ),
            table2 AS (
            SELECT COUNT(id) as total_dep FROM `dataengineer-494617.hr_poc.departments`
            ),
            table3 AS (
            SELECT (i.emp_hired / j.total_dep) as avg_hired FROM table1 i, table2 j
            )

            SELECT d.id, d.department, COUNT(e.id) as hired 
            FROM `dataengineer-494617.hr_poc.hired_employees` e JOIN
            `dataengineer-494617.hr_poc.departments` d ON e.department_id = d.id
            WHERE e.datetime >= '2021-01-01' AND e.datetime < '2022-01-01'
            GROUP BY d.id, d.department HAVING hired > (SELECT avg_hired FROM table3)
            ORDER BY hired DESC
            """

    return [dict(row) for row in _client.query(query).result()]