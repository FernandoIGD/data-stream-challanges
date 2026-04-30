import os
from google.cloud import bigquery, storage

PROJECT_ID = os.environ["GCP_PROJECT_ID"]
DATASET_ID = os.environ["BQ_DATASET_ID"]
BUCKET = os.environ["BACKUP_BUCKET"]

_bq_client = bigquery.Client(project=PROJECT_ID)
_gcs_client = storage.Client(project=PROJECT_ID)


def latest_run_id(table_name: str) -> str:
    '''
    Lists the available backups for the given table and returns the latest run_id.
    The run_id is expected to be in the format "YYYY-MM-DDTHH-MM-SSZ", 
    which allows for lexicographical sorting to determine the latest backup.

    Args:
        table_name (str): The name of the table to find backups for.
    Returns:
        str: The latest run_id available for the given table.
    Raises:
        ValueError: If no backups are found for the given table.
    '''
    prefix = f"backups/{table_name}/"
    iterator = _gcs_client.list_blobs(BUCKET, prefix=prefix, delimiter="/")
    list(iterator)
    runs = sorted(p.rstrip("/").split("/")[-1] for p in iterator.prefixes)
    if not runs:
        raise ValueError(f"no backups found for {table_name}")
    return runs[-1]


def restore_table(table_name: str, run_id: str) -> int:
    '''
    Restores a BigQuery table from a backup stored in GCS. It loads the Avro files
    from the specified backup run into the corresponding BigQuery table, replacing its contents.

    Args:
        table_name (str): The name of the table to restore.
        run_id (str): The identifier of the backup run to restore from.
    Returns:
        int: The number of rows loaded into the table after restoration.
    '''
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"
    source_uri = f"gs://{BUCKET}/backups/{table_name}/{run_id}/*.avro"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.AVRO,
        use_avro_logical_types=True,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    load_job = _bq_client.load_table_from_uri(
        source_uris=[source_uri],
        destination=table_ref,
        job_config=job_config,
    )
    load_job.result()
    return load_job.output_rows
