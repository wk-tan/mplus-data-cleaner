import boto3


def create_session():
    """Create session
    Returns:
        [session]: session
    """
    session = boto3.Session()
    return session
