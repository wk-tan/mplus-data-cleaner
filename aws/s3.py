import boto3


def create_s3_session():
    """Create S3 session
    Returns:
        [s3 session]: S3 session
    """
    session = boto3.Session()
    s3 = session.client("s3", region_name="ap-southeast-1")
    return s3
