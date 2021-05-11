import json

from google.cloud import bigquery
from google.oauth2 import service_account

from ..aws.aws_secret import get_secret_value

secret_val = get_secret_value(secret_id="gcs-mplus-data-cleaner-credential")
credentials = service_account.Credentials.from_service_account_info(
    json.loads(secret_val["google-key"])
)
bq_client = bigquery.Client(credentials=credentials)

write_disposition = {
    "append": bigquery.WriteDisposition.WRITE_APPEND,
    "truncate": bigquery.WriteDisposition.WRITE_TRUNCATE,
}

source_format = {
    "csv": bigquery.SourceFormat.CSV,
    "json": bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
}

table = bq_client.get_table(
    table="malaysia-stock-research.malaysia_derivatives.eod_data"
)
schema = table.schema


def load_data(source_uri, target_destination, source_type, load_type):
    """Load data into the corresponding table
    Args:
        dataset (str)
        table_name (str)
        source_type (str)
        load_type (str)
    """
    job_config = bigquery.LoadJobConfig(
        source_format=source_format[source_type],
        schema=schema,
        write_disposition=write_disposition[load_type],
        skip_leading_rows=1,
    )

    load_job = bq_client.load_table_from_uri(
        source_uris=source_uri, destination=target_destination, job_config=job_config
    )
    load_job.result()
