import os
import json
from datetime import datetime, timezone

from google.cloud import bigquery

PROJECT_ID = os.environ["GCP_PROJECT_ID"]
DATASET_ID = os.environ.get("BQ_DATASET_ID", "hr_poc")
REJECTS_TABLE = f"{PROJECT_ID}.{DATASET_ID}.load_reject"

_bq_client = bigquery.Client(project=PROJECT_ID)

def _summarize_errors(errors: list[dict]) -> str:
    '''
    Summarizes a list of validation errors into a human-readable string.

    Args:
        errors: A list of dictionaries, where each dictionary represents a validation error 
            with keys such as 'loc' (location of the error) and 'msg' (error message).
    Returns:
        A string summarizing the validation errors in a readable format.
    '''
    parts = []
    for err in errors:
        loc = '.'.join(str(x) for x in err.get('loc', ()))
        msg = err.get('msg', '?')
        parts.append(f'{loc}: {msg}' if loc else msg)
    return '; '.join(parts)


def write_rejects(table_name: str, rejected: list[dict], source: str) -> None:
    '''
    Writes rejected rows to a BigQuery table for later analysis.

    Args:
        table_name: The name of the table for which the rows were rejected 
            (e.g., "hired_employees", "departments", "jobs").
        rejected: A list of dictionaries, where each dictionary contains the original row data 
            and the associated validation errors.
        source: A string indicating the source of the data (e.g., "api", "batch_job") 
            to help with categorization in the rejects table.
    Returns:
        None. This function performs a side effect by inserting rows into a BigQuery table.
    '''
    if not rejected:
        return
    
    now = datetime.now(timezone.utc).isoformat()
    payload = [
        {
            "table_name": table_name,
            "row_data": json.dumps({"row": item["row"], "errors": item["errors"]}),
            "rejection_reason": _summarize_errors(item["errors"]),
            "rejected_at": now,
            "source": source,
        }
        for item in rejected
    ]

    errors = _bq_client.insert_rows_json(REJECTS_TABLE, payload)
    if errors:
        raise RuntimeError(f'insert_rows_json failed: {errors}')