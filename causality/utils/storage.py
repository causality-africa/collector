import os
import tempfile

import boto3
from botocore.client import Config


def download_from_backblaze(bucket_name: str, remote_path: str) -> str:
    """
    Download a file from Backblaze B2 to a local path.
    """
    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".csv")
    local_path = temp_file.name
    temp_file.close()

    s3 = boto3.client(
        "s3",
        endpoint_url="https://s3.us-east-005.backblazeb2.com",
        aws_access_key_id=os.environ.get("B2_ACCESS_KEY_ID"),
        aws_secret_access_key=os.environ.get("B2_SECRET_ACCESS_KEY"),
        config=Config(signature_version="s3v4"),
    )

    s3.download_file(bucket_name, remote_path, local_path)
    return local_path
