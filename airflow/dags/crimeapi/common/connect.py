import boto3

def create_aws_conn(resource: str, access_key: str, secret_access_key: str, region: str):
    """
    Connect to aws client
    """
    client = boto3.resource(
        resource,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_access_key,
        region_name=region
    )

    return client