from .aws_session import create_session

session = create_session()
s3 = session.client("s3", region_name="ap-southeast-1")


def get_object(bucket, key):
    reponse = s3.get_object(Bucket=bucket, Key=key)
    data = reponse["Body"].read()

    return data


def put_object(bucket, key, body):
    response = s3.put_object(Bucket=bucket, Key=key, Body=body)

    return response


def list_objects_v2(bucket, prefix):
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    data = response.get("Contents")
    if data is None:
        data = []
    return data


def download_file(bucket, download_path):
    tmp = download_path.split("/")[-1]
    s3.download_file(bucket, download_path, tmp)

    return f"download_path={download_path}"


def upload_file(bucket, key, upload_path):
    s3.upload_file(key, bucket, upload_path)

    return f"upload_path={bucket}/{upload_path}"
