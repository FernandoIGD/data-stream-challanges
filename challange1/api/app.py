from fastapi import FastAPI, HTTPException
from pydantic import ValidationError
from models import HiredEmployeesBatch, HiredEmployeesRow, DepartmentRow, DepartmentsBatch, JobRow, JobsBatch
from bq_writer import append_hired_employees, append_departments, append_jobs, hired_employees_to_proto, departments_to_proto, jobs_to_proto
from rejects import write_rejects
from restore import restore_table, latest_run_id

app = FastAPI()

ALLOWED_TABLES = {"hired_employees", "departments", "jobs"}

@app.post("/insert/hired_employees")
def insert_hired_employees(batch: HiredEmployeesBatch):
    accepted: list[HiredEmployeesRow] = []
    rejected: list[dict] = []
    for raw in batch.rows:
        try:
            row = HiredEmployeesRow.model_validate(raw)
            accepted.append(row)
        except ValidationError as e:    
            rejected.append({"row": raw, "errors": e.errors()})

    serialized_rows = [hired_employees_to_proto(row) for row in accepted]
    append_hired_employees(serialized_rows) 
    write_rejects("hired_employees", rejected, source="api_insert")   
    
    return {
        "accepted": len(accepted),
        "rejected": len(rejected),
        "rejects": rejected,
    }

@app.post("/insert/departments")
def insert_departments(batch: DepartmentsBatch):
    accepted: list[DepartmentRow] = []
    rejected: list[dict] = []
    for raw in batch.rows:
        try:
            row = DepartmentRow.model_validate(raw)
            accepted.append(row)
        except ValidationError as e:
            rejected.append({"row": raw, "errors": e.errors()})

    serialized_rows = [departments_to_proto(row) for row in accepted]
    append_departments(serialized_rows)
    write_rejects("departments", rejected, source="api_insert")

    return {
        "accepted": len(accepted),
        "rejected": len(rejected),
        "rejects": rejected,
    }

@app.post("/insert/jobs")
def insert_jobs(batch: JobsBatch):
    accepted: list[JobRow] = []
    rejected: list[dict] = []
    for raw in batch.rows:
        try:
            row = JobRow.model_validate(raw)
            accepted.append(row)
        except ValidationError as e:
            rejected.append({"row": raw, "errors": e.errors()})

    serialized_rows = [jobs_to_proto(row) for row in accepted]
    append_jobs(serialized_rows)
    write_rejects("jobs", rejected, source="api_insert")

    return {
        "accepted": len(accepted),
        "rejected": len(rejected),
        "rejects": rejected,
    }

@app.post("/restore/{table}")
def restore(table: str, run_id: str | None = None):
    if table not in ALLOWED_TABLES:
        raise HTTPException(status_code=400, detail=f"unknown table: {table}")
    resolved_run_id = run_id or latest_run_id(table)
    rows = restore_table(table, resolved_run_id)
    return {
        "table": table,
        "run_id": resolved_run_id,
        "rows_loaded": rows,
    }
