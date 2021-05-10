import json

from .aws_session import create_session

session = create_session()
secrets_manager = session.client("secretsmanager", region_name="ap-southeast-1")


def get_secret_value(secret_id):
    """
    Get secret dict using boto3's secretmanager
    Args:
        secret_id (str): define your secret id
    Returns:
        dict: str
    """
    secret_value = json.loads(
        secrets_manager.get_secret_value(SecretId=secret_id)["SecretString"]
    )

    return secret_value
