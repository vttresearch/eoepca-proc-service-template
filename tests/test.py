import os
import unittest

import requests
from cookiecutter.main import cookiecutter
from dotenv import load_dotenv
from loguru import logger

load_dotenv()


class TestExecutionHandler(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # UMA
        # Client (e.g. Portal) authenticates user and calls the ADES
        client_id = os.getenv("CLIENT_ID") 
        client_secret = os.getenv(
            "CLIENT_SECRET"
        )

        # Get Token Endpoint from OIDC Configuration
        # Get OIDC Configuration
        oidc_config_endpoint = os.getenv(
            "OIDC_ENDPOINT"
        )
        
        headers = {"accept": "application/json"}
        oidc_config = requests.get(oidc_config_endpoint, headers=headers).json()

        # Extract the token endpoint
        token_endpoint = oidc_config["token_endpoint"]
        logger.info(f"Token Endpoint: {token_endpoint}")
        # Get Tokens for User eric

        username = os.getenv("USER_NAME")  # "eric"
        password = os.getenv("PASSWORD")  # "defaultPWD"

        logger.info(f"User: {username}")
        logger.info(f"Password: {password}")

        headers = {"cache-control": "no-cache"}
        data = {
            "scope": "openid user_name is_operator",
            "grant_type": "password",
            "username": username,
            "password": password,
            "client_id": client_id,
            "client_secret": client_secret,
        }
        token_response = requests.post(
            token_endpoint, headers=headers, data=data
        ).json()

        id_token = token_response["id_token"]
        token_response["access_token"]
        token_response["refresh_token"]

        cls.conf = {}
        cls.conf["auth_env"] = {"jwt": id_token}
        cls.conf["lenv"] = {"message": ""}
        cls.conf["lenv"] = {
            "Identifier": "water-bodies",
            "usid": "dummy-uid",
        }
        cls.conf["tmpPath"] = "/tmp"
        cls.conf["main"] = {
            "tmpPath": "/tmp",
            "tmpUrl": "http://localhost:8080",
        }

        cls.conf["additional_parameters"] = {
            "STAGEOUT_AWS_ACCESS_KEY_ID": os.getenv(
                "AWS_SECRET_ACCESS_KEY", "minio-admin"
            ),
            "STAGEOUT_AWS_SECRET_ACCESS_KEY": os.getenv(
                "AWS_REGION", "minio-secret-password"
            ),
            "STAGEOUT_AWS_REGION": os.getenv("AWS_ACCESS_KEY_ID", "RegionOne"),
            "STAGEOUT_AWS_SERVICEURL": os.getenv(
                "AWS_SERVICE_URL", "http://s3-service.zoo.svc.cluster.local:9000"
            ),
            "STAGEOUT_OUTPUT": "processingresults",
            "process": "water-bodies-run",
        }

        cls.service_name = "water_bodies"
        cls.workflow_id = "water-bodies"

        cookiecutter_values = {
            "service_name": cls.service_name,
            "workflow_id": cls.workflow_id,
        }

        os.environ[
            "WRAPPER_STAGE_IN"
        ] = f"{os.path.dirname(__file__)}/assets/stagein.yaml"
        os.environ[
            "WRAPPER_STAGE_OUT"
        ] = f"{os.path.dirname(__file__)}/assets/stageout.yaml"
        os.environ["WRAPPER_MAIN"] = f"{os.path.dirname(__file__)}/assets/main.yaml"
        os.environ["WRAPPER_RULES"] = f"{os.path.dirname(__file__)}/assets/rules.yaml"

        os.environ["DEFAULT_VOLUME_SIZE"] = "10000"
        os.environ["STORAGE_CLASS"] = "standard"

        template_folder = f"{os.path.dirname(__file__)}/.."

        service_tmp_folder = "tests/"

        cookiecutter(
            template_folder,
            extra_context=cookiecutter_values,
            output_dir=service_tmp_folder,
            no_input=True,
            overwrite_if_exists=True,
            # config_file=self.cookiecutter_configuration_file
        )

        # noqa

        cls.inputs = {
            "aoi": {"value": "-121.399,39.834,-120.74,40.472"},
            "bands": {"value": ["green", "nir"]},
            "epsg": {"value": "EPSG:4326"},
            "stac_items": {
                "value": [
                    "https://earth-search.aws.element84.com/v1/collections/sentinel-2-l2a/items/S2A_10TFK_20210708_0_L2A",  # noqa
                ]
            },
        }

        cls.outputs = {}
        cls.outputs["stac"] = {"value": ""}

    def test_runner(self):
        from tests.water_bodies.service import water_bodies

        water_bodies(self.conf, self.inputs, self.outputs)
