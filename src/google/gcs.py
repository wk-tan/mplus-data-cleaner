import json

from google.cloud import storage
from google.oauth2 import service_account

from ..aws.aws_secret import get_secret_value

secret_val = get_secret_value(secret_id="gcs-mplus-data-cleaner-credential")
credentials = service_account.Credentials.from_service_account_info(
    json.loads(secret_val["google-key"])
)
storage_client = storage.Client(credentials=credentials)


def upload_blob(bucket, blob_name, data):
    """Upload string to destination
    Args:
        bucket (str)
        blob_name (str): filename
        data (str)
    """
    bucket = storage_client.bucket(bucket_name=bucket)
    blob = bucket.blob(blob_name=blob_name)

    blob.upload_from_string(data, "text/csv", num_retries=3)
