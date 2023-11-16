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

import json
import os

import yaml
from zoo_calrissian_runner import ExecutionHandler, ZooCalrissianRunner


class EoepcaCalrissianRunnerExecutionHandler(ExecutionHandler):
    def local_get_file(self, fileName):
        """
        Read and load a yaml file

        :param fileName the yaml file to load
        """
        try:
            with open(fileName, "r") as file:
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
        return self.conf.get("pod_env_vars", {})

    def get_pod_node_selector(self):
        return self.conf.get("pod_node_selector", {})

    def get_secrets(self):
        return self.local_get_file("/assets/pod_imagePullSecrets.yaml")

    def get_additional_parameters(self):
        return self.conf.get("additional_parameters", {})

    def handle_outputs(self, log, output, usage_report, tool_logs):
        """
        Handle the output files of the execution.

        :param log: The application log file of the execution.
        :param output: The output file of the execution.
        :param usage_report: The metrics file.
        :param tool_logs: A list of paths to individual workflow step logs.

        """
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
            okeys = ["url", "title", "rel"]
            keys = ["url", "title", "rel"]
            if i > 0:
                for j in range(len(keys)):
                    keys[j] = keys[j] + "_" + str(i)
            if "service_logs" not in self.conf:
                self.conf["service_logs"] = {}
            for j in range(len(keys)):
                self.conf["service_logs"][keys[j]] = servicesLogs[i][okeys[j]]
        self.conf["service_logs"]["length"] = str(len(servicesLogs))


def water_bodies(conf, inputs, outputs):  # noqa
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
    working_dir = os.path.join(conf["main"]["tmpPath"], runner.get_namespace_name())
    os.makedirs(
        working_dir,
        mode=0o777,
        exist_ok=True,
    )
    os.chdir(working_dir)

    exit_status = runner.execute()

    if exit_status == zoo.SERVICE_SUCCEEDED:
        out = {
            "StacCatalogUri": runner.outputs.outputs["stac"]["value"]["StacCatalogUri"]
        }
        json_out_string = json.dumps(out, indent=4)
        outputs["stac"]["value"] = json_out_string
        return zoo.SERVICE_SUCCEEDED

    else:
        conf["lenv"]["message"] = zoo._("Execution failed")
        return zoo.SERVICE_FAILED
