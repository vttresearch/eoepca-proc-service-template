import os
import sys
from urllib.parse import urlparse

import boto3  # noqa: F401
import botocore
from botocore.exceptions import ClientError
from loguru import logger
from pystac.stac_io import DefaultStacIO
from datetime import datetime
import pystac 

class CustomStacIO(DefaultStacIO):
    """Custom STAC IO class that uses boto3 to read from S3."""

    def __init__(self):
        self.session = botocore.session.Session()

    def read_text(self, source, *args, **kwargs):
        parsed = urlparse(source)
        if parsed.scheme == "s3":
            # read the user settings file from the environment variable
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
                print(s3_client.get_object(Bucket=bucket, Key=key))
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


class ResultCollection:
    """Create a STAC Collection for the results."""

    def __init__(self, date: str,**kwargs):

        self.date = date    
        self.kwargs = kwargs
        self.start_time = datetime.strptime(
            f"{self.date}T00:00:00", "%Y-%m-%dT%H:%M:%S"
        )
        self.end_time = datetime.strptime(f"{self.date}T23:59:59", "%Y-%m-%dT%H:%M:%S")

        self.stac_extensions = []

    def _temporal_extent(self):
        """Create a temporal extent for the collection."""
        dates = [self.start_time, self.end_time]

        return pystac.TemporalExtent(intervals=[[min(dates), max(dates)]])

    @staticmethod
    def _spatial_extent():
        """Create a spatial extent for the collection."""
        return pystac.SpatialExtent([[-180, -90, 180, 90]])

    def _extent(self):
        """Create an extent for the collection."""
        return pystac.Extent(
            spatial=self._spatial_extent(), temporal=self._temporal_extent()
        )

    @staticmethod
    def _human_date(date):
        """Format a date to a human readable string."""
        return datetime.strftime(date, "%Y-%m-%d")

    @staticmethod
    def _iso_date(date):
        """Format a date to an ISO string."""
        return datetime.strftime(date, "%Y-%m-%dT%H:%M:%S")

    @staticmethod
    def _short_date(date):
        """Format a date to an short string."""
        return datetime.strftime(date, "%Y-%m-%d")

    def init_collection(self):
        """Initialize a STAC Collection."""
        return pystac.Collection(
            id=self.kwargs.get("identifier", "processing-results"),
            description=self.kwargs.get("description", "Processing results"),
            extent=self._extent(),
            title=self.kwargs.get("description", "Processing results"),
            #href=f"./collection-{self.kwargs.get('identifier', 'processing-results')}.json",
            stac_extensions=self.stac_extensions,
            keywords=self.kwargs.get("keywords", ["eoepca"]),
            license="proprietary",
        )