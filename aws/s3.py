import os

import boto3


def create_s3_session():
    """WIP: Create S3 session
    Returns:
        [s3 session]: S3 session
    """
    # need to load access and secret key from env (docker env)
    # AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
    # AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY")
    # session = boto3.Session(
    #     aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY
    # )
    session = boto3.Session()
    s3 = session.client("s3", region_name="ap-southeast-1")
    return s3
