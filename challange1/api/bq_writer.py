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
    return HiredEmployees(
        id=row.id,
        name=row.name,
        datetime=_to_micros(row.datetime_),
        department_id=row.department_id,
        job_id=row.job_id,
    ).SerializeToString()

def departments_to_proto (row: DepartmentRow) -> bytes:
    return Departments(
        id=row.id,
        department=row.department,
    ).SerializeToString()

def jobs_to_proto (row: JobRow) -> bytes:
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


def append_hired_employees(serialized_rows: list[bytes]):
    '''
    Appends serialized HiredEmployees rows to the BigQuery table using the Storage Write API.

    Args:
        serialized_rows: A list of bytes, where each byte string is a serialized HiredEmployees protobuf message.
    Returns:
        None. The function sends the rows to BigQuery and does not return any value.
    '''
    if not serialized_rows:
        return
    
    stream_name = (
        _write_client.table_path(PROJECT_ID, DATASET_ID, "hired_employees")
        +"/_default"
    )

    request_template = types.AppendRowsRequest(
        write_stream=stream_name,
        proto_rows=types.AppendRowsRequest.ProtoData(
            writer_schema= _proto_schema(HiredEmployees),
        ),
    )

    stream = writer.AppendRowsStream(_write_client, request_template)
    try:
        batch = types.AppendRowsRequest(
            proto_rows=types.AppendRowsRequest.ProtoData(
                rows=types.ProtoRows(serialized_rows=serialized_rows),
            ),
        )
        future = stream.send(batch)
        future.result()
    finally:
        stream.close()