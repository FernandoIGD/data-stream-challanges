from fastapi import FastAPI
from pydantic import ValidationError
from models import HiredEmployeesBatch, HiredEmployeesRow, DepartmentRow, DepartmentsBatch, JobRow, JobsBatch
from bq_writer import append_hired_employees, append_departments, append_jobs, hired_employees_to_proto, departments_to_proto, jobs_to_proto

app = FastAPI()

@app.post("/insert/hired_employees")
async def insert_hired_employees(batch: HiredEmployeesBatch):
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
    
    return {
        "accepted": len(accepted),
        "rejected": len(rejected),
        "rejects": rejected,
    }

@app.post("/insert/departments")
async def insert_departments(batch: DepartmentsBatch):
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

    return {
        "accepted": len(accepted),
        "rejected": len(rejected),
        "rejects": rejected,
    }

@app.post("/insert/jobs")
async def insert_jobs(batch: JobsBatch):
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

    return {
        "accepted": len(accepted),
        "rejected": len(rejected),
        "rejects": rejected,
    }
