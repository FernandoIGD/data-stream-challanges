import os
from datetime import datetime , timezone
from hired_employees_pb2 import HiredEmployees
from departments_pb2 import Departments
from jobs_pb2 import Jobs
from models import HiredEmployeesRow, DepartmentRow, JobRow
from google.cloud import bigquery_storage_v1
from google.cloud.bigquery_storage_v1 import types, writer
from google.protobuf import descriptor_pb2

PROJECT_ID = os.environ["GCP_PROJECT_ID"]
DATASET_ID = os.environ.get("BQ_DATASET_ID", "hr_poc")

_write_client = bigquery_storage_v1.BigQueryWriteClient()

def _to_micros(dt: datetime) -> int:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1_000_000)

def hired_employees_to_proto(row: HiredEmployeesRow) -> bytes:
    '''
    Converts a HiredEmployeesRow Pydantic model instance to a serialized protobuf message.

    Args:
        row: An instance of HiredEmployeesRow containing the data to be converted.
    Returns:
        A bytes object representing the serialized protobuf message of type HiredEmployees.
    '''
    return HiredEmployees(
        id=row.id,
        name=row.name,
        datetime=_to_micros(row.datetime_),
        department_id=row.department_id,
        job_id=row.job_id,
    ).SerializeToString()

def departments_to_proto (row: DepartmentRow) -> bytes:
    '''
    Converts a DepartmentRow Pydantic model instance to a serialized protobuf message.

    Args:
        row: An instance of DepartmentRow containing the data to be converted.
    Returns:
        A bytes object representing the serialized protobuf message of type Departments.
    '''
    return Departments(
        id=row.id,
        department=row.department,
    ).SerializeToString()

def jobs_to_proto (row: JobRow) -> bytes:
    '''
    Converts a JobRow Pydantic model instance to a serialized protobuf message.

    Args:
        row: An instance of JobRow containing the data to be converted.
    Returns:
        A bytes object representing the serialized protobuf message of type Jobs.
    '''
    return Jobs(
        id=row.id,
        job=row.job,
    ).SerializeToString()

def _proto_schema(message_class) -> types.ProtoSchema:
    '''
    Converts a protobuf message class to a BigQuery Storage API ProtoSchema.

    Args:
        message_class: A protobuf message class (e.g., HiredEmployees).
    Returns:
        A types.ProtoSchema object representing the schema of the message class.
    '''
    descriptor = descriptor_pb2.DescriptorProto()
    message_class.DESCRIPTOR.CopyToProto(descriptor)
    return types.ProtoSchema(proto_descriptor=descriptor)


def _append_rows(table_name: str, message_class, serialized_rows: list[bytes]) -> None:
    '''
    Appends serialized protobuf rows to the specified BigQuery table using the Storage Write API.

    Args:
        table_name: The name of the BigQuery table to which the rows will be appended (e.g., "hired_employees", "departments", "jobs").
        message_class: The protobuf message class corresponding to the rows being appended (e.g., HiredEmployees, Departments, Jobs).
        serialized_rows: A list of bytes, where each byte string is a serialized protobuf message of the specified message_class.
    Returns:
        None. The function sends the rows to BigQuery and does not return any value.
    '''
    if not serialized_rows:
        return
    
    stream_name = (
        _write_client.table_path(PROJECT_ID, DATASET_ID, table_name)
        +"/_default"    
    )

    request_template = types.AppendRowsRequest(
        write_stream=stream_name,
        proto_rows=types.AppendRowsRequest.ProtoData(
            writer_schema = _proto_schema(message_class),
        ),
    )

    stream = writer.AppendRowsStream(_write_client, request_template)
    try:
        rows = types.ProtoRows(serialized_rows=serialized_rows)
        proto_rows = types.AppendRowsRequest.ProtoData(rows=rows)
        batch = types.AppendRowsRequest(proto_rows=proto_rows)
        future = stream.send(batch)
        future.result()

    finally:
        stream.close()


def append_hired_employees(serialized_rows: list[bytes]) -> None:
    _append_rows("hired_employees", HiredEmployees, serialized_rows)

def append_departments(serialized_rows: list[bytes]) -> None:
    _append_rows("departments", Departments, serialized_rows)

def append_jobs(serialized_rows: list[bytes]) -> None:
    _append_rows("jobs", Jobs, serialized_rows)