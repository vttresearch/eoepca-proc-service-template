import os
import sys
from urllib.parse import urlparse

import boto3  # noqa: F401
import botocore
from botocore.exceptions import ClientError
from loguru import logger
from pystac.stac_io import DefaultStacIO
from s3 import S3Settings


class CustomStacIO(DefaultStacIO):
    """Custom STAC IO class that uses boto3 to read from S3."""

    def __init__(self):
        self.session = botocore.session.Session()

    def read_text(self, source, *args, **kwargs):
        parsed = urlparse(source)
        if parsed.scheme == "s3":
            # read the user settings file from the environment variable
            s3_settings = S3Settings()
            s3_settings.set_s3_environment(source)

            s3_client = self.session.create_client(
                service_name="s3",
                region_name=os.environ.get("AWS_REGION"),
                use_ssl=True,
                endpoint_url=os.environ.get("AWS_S3_ENDPOINT"),
                aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
                aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
            )

            bucket = parsed.netloc
            key = parsed.path[1:]

            try:
                return (
                    s3_client.get_object(Bucket=bucket, Key=key)["Body"]
                    .read()
                    .decode("utf-8")
                )
            except ClientError as ex:
                if ex.response["Error"]["Code"] == "NoSuchKey":
                    logger.error(f"Error reading {source}: {ex}")
                    sys.exit(1)

        else:
            return super().read_text(source, *args, **kwargs)
