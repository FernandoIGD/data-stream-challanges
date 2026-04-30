import os
from datetime import datetime, timezone
from google.cloud import bigquery

if __name__ == "__main__":
    PROJECT_ID = os.environ["GCP_PROJECT_ID"]
    DATASET_ID = os.environ["BQ_DATASET_ID"]
    BUCKET = os.environ["BACKUP_BUCKET"]

    TABLES = ['hired_employees', 'departments', 'jobs']

    client = bigquery.Client(project=PROJECT_ID)
    run_id = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%SZ")

    for table in TABLES:
        table_ref = f'{PROJECT_ID}.{DATASET_ID}.{table}'
        destination_uri = f'gs://{BUCKET}/backups/{table}/{run_id}/*.avro'

        job_config = bigquery.ExtractJobConfig(
            destination_format = bigquery.DestinationFormat.AVRO,
            use_avro_logical_types = True,
        )

        print(f'[backup] extracting {table_ref} -> {destination_uri}', flush = True)

        extract_job = client.extract_table(
            source=table_ref,
            destination_uris=[destination_uri],
            job_config=job_config,
        )
        extract_job.result()

        print(f'[backup] done: {table}', flush = True)

    print(f'[backup] all tables backed up under run_id={run_id}', flush = True)