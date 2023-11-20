# see https://zoo-project.github.io/workshops/2014/first_service.html#f1
import pathlib

try:
    import zoo
except ImportError:

    class ZooStub(object):
        def __init__(self):
            self.SERVICE_SUCCEEDED = 3
            self.SERVICE_FAILED = 4

        def update_status(self, conf, progress):
            print(f"Status {progress}")

        def _(self, message):
            print(f"invoked _ with {message}")

    zoo = ZooStub()

#import base64
#import importlib
import json
import os
import sys
from urllib.parse import urlparse

import boto3  # noqa: F401
import botocore
import yaml
#import subprocess
from botocore.exceptions import ClientError
from loguru import logger
from pystac import Asset, Collection, read_file
#import requests
from pystac.stac_io import DefaultStacIO, StacIO
#import re
from s3 import S3Settings
from zoo_calrissian_runner import ExecutionHandler, ZooCalrissianRunner

# from stac import CustomStacIO

class CustomStacIO(DefaultStacIO):
    """Custom STAC IO class that uses boto3 to read from S3."""

    def __init__(self):
        self.session = botocore.session.Session()

    def read_text(self, source, *args, **kwargs):
        parsed = urlparse(source)
        logger.info(f"parsed {parsed}")
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
            logger.info(f"bucket {bucket}")
            logger.info(f"key {key}")
            try:
                logger.info(f"content")
                content = s3_client.get_object(Bucket=bucket, Key=key)["Body"].read()
                logger.info(f"content {content}")
                return (
                    content.read().decode("utf-8")
                )
            except ClientError as ex:
                if ex.response["Error"]["Code"] == "NoSuchKey":
                    logger.error(f"Error reading {source}: {ex}")
                    sys.exit(1)

        else:
            return super().read_text(source, *args, **kwargs)


StacIO.set_default(CustomStacIO)

class EoepcaCalrissianRunnerExecutionHandler(ExecutionHandler):

    def pre_execution_hook(self):

        logger.info("Pre execution hook")
        logger.info(self.conf["auth_env"])

    def post_execution_hook(self, log, output, usage_report, tool_logs):

        logger.info(output)

        logger.info(self.conf["auth_env"])
        StacIO.set_default(CustomStacIO)

        logger.info(f"STAC Catalog URI: {output['StacCatalogUri']}")

        logger.info(StacIO.default())
        try:
            cat = read_file(output["StacCatalogUri"])
            cat.describe()
        except Exception as e:
            logger.info(f"Exception: {e}")
        logger.info(os.environ["AWS_S3_ENDPOINT"] )
        logger.info("Post execution hook")

    @staticmethod
    def local_get_file(fileName):
        """
        Read and load the contents of a yaml file

        :param yaml file to load
        """
        try:
            with open(fileName, 'r') as file:
                data = yaml.safe_load(file)
            return data
        # if file does not exist
        except FileNotFoundError:
            return {}
        # if file is empty
        except yaml.YAMLError:
            return {}
        # if file is not yaml
        except yaml.scanner.ScannerError:
            return {}

    def get_pod_env_vars(self):

        logger.info("get_pod_env_vars")

        return self.conf.get("pod_env_vars", {})

    def get_pod_node_selector(self):

        logger.info("get_pod_node_selector")

        return self.conf.get("pod_node_selector", {})

    def get_secrets(self):

        logger.info("get_secrets")

        return self.local_get_file('/assets/pod_imagePullSecrets.yaml')

    def get_additional_parameters(self):

        logger.info("get_additional_parameters")

        return self.conf.get("additional_parameters", {})

    def handle_outputs(self, log, output, usage_report, tool_logs):
        """
        Handle the output files of the execution.

        :param log: The application log file of the execution.
        :param output: The output file of the execution.
        :param usage_report: The metrics file.
        :param tool_logs: A list of paths to individual workflow step logs.

        """

        logger.info("handle_outputs")

        # link element to add to the statusInfo
        servicesLogs = [
            {
                "url": f"{self.conf['main']['tmpUrl']}/"
                f"{self.conf['lenv']['Identifier']}-{self.conf['lenv']['usid']}/"
                f"{os.path.basename(tool_log)}",
                "title": f"Tool log {os.path.basename(tool_log)}",
                "rel": "related",
            }
            for tool_log in tool_logs
        ]
        for i in range(len(servicesLogs)):
            okeys=["url","title","rel"]
            keys=["url","title","rel"]
            if i>0:
                for j in range(len(keys)):
                    keys[j]=keys[j]+"_"+str(i)
            if "service_logs" not in self.conf:
                self.conf["service_logs"]={}
            for j in range(len(keys)):
                self.conf["service_logs"][keys[j]]=servicesLogs[i][okeys[j]]

        self.conf["service_logs"]["length"]=str(len(servicesLogs))


def {{cookiecutter.workflow_id |replace("-", "_")  }}(conf, inputs, outputs): # noqa

    with open(
        os.path.join(
            pathlib.Path(os.path.realpath(__file__)).parent.absolute(),
            "app-package.cwl",
        ),
        "r",
    ) as stream:
        cwl = yaml.safe_load(stream)

    execution_handler = EoepcaCalrissianRunnerExecutionHandler(conf=conf)

    runner = ZooCalrissianRunner(
        cwl=cwl,
        conf=conf,
        inputs=inputs,
        outputs=outputs,
        execution_handler=execution_handler,
    )

    # we are changing the working directory to store the outputs
    # in a directory dedicated to this execution
    working_dir=os.path.join(conf["main"]["tmpPath"], runner.get_namespace_name())
    os.makedirs(
            working_dir,
            mode=0o777,
            exist_ok=True,
    )
    os.chdir(working_dir)

    exit_status = runner.execute()

    if exit_status == zoo.SERVICE_SUCCEEDED:
        out = {"StacCatalogUri": runner.outputs.outputs["stac"]["value"]["StacCatalogUri"] }
        json_out_string= json.dumps(out, indent=4)
        outputs["stac"]["value"]=json_out_string
        return zoo.SERVICE_SUCCEEDED

    else:
        conf["lenv"]["message"] = zoo._("Execution failed")
        return zoo.SERVICE_FAILED
