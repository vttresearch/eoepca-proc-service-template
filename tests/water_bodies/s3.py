
import os
import re


class S3Settings:
    """class for reading JSON files, matching regular expressions,
    and setting environment variables for an S3 service"""

    def __init__(self):

        self.settings = {
                            "S3": {
                                "Services": {
                                    "minio": {
                                        "UrlPattern": "s3:\/\/processingresults\/.*",
                                        "Region": "RegionOne",
                                        "AuthenticationRegion": "RegionOne",
                                        "AccessKey": "minio-admin",
                                        "SecretKey": "minio-secret-password",
                                        "ServiceURL": "http://localhost:9000",
                                        #"ForcePathStyle": "true"
                                    }
                                }
                            }
                        }

    @staticmethod
    def match_regex(regex, string):
        """match a regular expression to a string and return the match object"""
        return re.search(regex, string)

    @staticmethod
    def set_s3_vars(s3_service):
        """set environment variables for an S3 service"""
        os.environ["AWS_ACCESS_KEY_ID"] = s3_service["AccessKey"]
        os.environ["AWS_SECRET_ACCESS_KEY"] = s3_service["SecretKey"]
        os.environ["AWS_DEFAULT_REGION"] = s3_service["Region"]
        os.environ["AWS_REGION"] = s3_service["Region"]
        os.environ["AWS_S3_ENDPOINT"] = s3_service["ServiceURL"]

    def set_s3_environment(self, url):
        """set environment variables for an S3 service based on a given URL"""
        for _, s3_service in self.settings["S3"]["Services"].items():
            if self.match_regex(s3_service["UrlPattern"], url):
                self.set_s3_vars(s3_service)
                break
