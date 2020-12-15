from pathlib import Path
import logging
from urllib.parse import urlparse
import subprocess
import time

import ray
from ray import serve
from mlflow.deployments import BaseDeploymentClient
from mlflow.exceptions import MlflowException
from mlflow.tracking.artifact_utils import _download_artifact_from_uri
from mlflow.models import Model
from mlflow.protos.databricks_pb2 import INVALID_PARAMETER_VALUE

from .utils import (get_preferred_deployment_flavor, validate_deployment_flavor,
                    SUPPORTED_DEPLOYMENT_FLAVORS, flavor2backend, Config)


logger = logging.getLogger(__name__)


def target_help():
    # TODO
    help_string = ("\nmlflow-rayserve plugin integrates Ray Serve with mlflow deployment pipeline. "
                   "For detailed explanation and to see multiple examples, checkout the Readme at "
                   "https://github.com/RedisAI/mlflow-redisai/blob/master/README.md \n\n"

                   "Connection parameters: You can either use the URI to specify the connection "
                   "parameters or specify them as environmental variables. If connection parameters "
                   "are present in both URI and environmental variables, parameters from the "
                   "environmental variables are ignored completely. The command with formatted "
                   "URI would look like\n\n"

                   "    mlflow deployments <command> -t redisai:/<username>:<password>@<host>:<port>/<db>\n\n"

                   "If you'd like to use the default values for parameters, only specify the "
                   "target as given below \n\n"

                   "    mlflow deployments <command> -t redisai\n\n"

                   "If you are going with environmental variables instead of URI parameters, the "
                   "expected keys are \n\n"

                   "    * REDIS_HOST\n"
                   "    * REDIS_PORT\n"
                   "    * REDIS_DB\n"
                   "    * REDIS_USERNAME\n"
                   "    * REDIS_PASSWORD\n\n"

                   "However, if you wish to go with default values, don't set any environmental "
                   "variables\n\n"
                   "Model configuration: The ``--config`` or ``-C`` option of ``create`` and "
                   "``update`` API enables you to pass arguments specific to RedisAI deployment. "
                   "The possible config options are\n\n"

                   "    * batchsize: Batch size for auto-batching\n"
                   "    * tag: Tag a deployment with a version number or a given name\n"
                   "    * device: CPU or GPU. if multiple GPUs are available, specify that too\n\n")
    return help_string


def run_local(name, model_uri, flavor=None, config=None):
    pass # TODO

class MLFlowBackend:
    def __init__(self, model_uri):
        self.model = mlflow.pyfunc.load_model(model_uri=model_uri)
    async def __call__(self, starlette_request):
        df = await starlette_request.body() # TODO: Does this correctly use Ray Serve pandas.df optimization?
        return self.model.predict(df)

class RedisAIPlugin(BaseDeploymentClient):

    def __init__(self, uri):
        super().__init__(uri)
        ray.init()
        self.client = serve.start()

    def create_deployment(self, name, model_uri, flavor=None, config=None):
        self.client.create_backend(name, MLFlowBackend, model_uri, config=config)
        self.client.create_endpoint(name, backend=name)
        #TODO: check flavor
        #TODO: raise MLflow exception as necessary
        #TODO: env
        return {'name': name, 'config': config} # , 'flavor': flavor}

    def delete_deployment(self, name):
        self.client.delete_backend(name)
        self.client.delete_endpoint(name)
        logger.info("Deleted model with name: {}".format(name))

    def update_deployment(self, name, model_uri=None, flavor=None, config=None):
        if model_uri is None:
            self.client.update_backend_config(name, config=config)
        else:
            self.delete_deployment(name)
            self.create_deployment(name, model_uri)


    def list_deployments(self, **kwargs):
        return [{
            "name": name,
            "config": config
        } for (name, config) in self.client.list_backends().items()]

    def get_deployment(self, name):
        #TODO: raise error if name not found
        return {'name': name, 'config': self.client.list_backends()[name]}

    def predict(self, deployment_name, df):
        return ray.get(client.get_handle(deployment_name).remote(df))