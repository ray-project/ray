from pathlib import Path
import logging
from urllib.parse import urlparse
import subprocess
import time

import ray
from ray import serve
import redisai
import redis
import ml2rt
from mlflow.deployments import BaseDeploymentClient
from mlflow.exceptions import MlflowException
from mlflow.tracking.artifact_utils import _download_artifact_from_uri
from mlflow.models import Model
from mlflow.protos.databricks_pb2 import INVALID_PARAMETER_VALUE

from .utils import (get_preferred_deployment_flavor, validate_deployment_flavor,
                    SUPPORTED_DEPLOYMENT_FLAVORS, flavor2backend, Config)


logger = logging.getLogger(__name__)


def target_help():
    help_string = ("\nmlflow-redisai plugin integrates RedisAI to mlflow deployment pipeline. "
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
    """
    Run the RedisAI docker container locally and deploy the model to it. It requires
    docker to be installed in the host machine.

    Parameters
    ----------
    name : str
        Name/key for setting the model in RedisAI
    model_uri : str
        A valid mlflow model URI
    flavor : str
        Which flavor to use to deploy. If this is not provided, it will be inferred from the
        model config file
    config : dict
        Configuration dictionary parsed from user command passed as ``-C key value``
    """
    device = config.get('device', 'cpu')
    if 'gpu' in device.lower():
        commands = ['docker', 'run', '-p', '6379:6379', '--gpus', 'all', '--rm', 'redisai/redisai:latest']
    else:
        commands = ['docker', 'run', '-p', '6379:6379', '--rm', 'redisai/redisai:latest']
    proc = subprocess.Popen(commands)
    plugin = RedisAIPlugin('redisai:/localhost:6379/0')
    start_time = time.time()
    prev_num_interval = 0
    while True:
        logger.info("Launching RedisAI docker container")
        try:
            if plugin.con.ping():
                break
        except redis.exceptions.ConnectionError:
            num_interval, _ = divmod(time.time() - start_time, 10)
            if num_interval > prev_num_interval:
                prev_num_interval = num_interval
                try:
                    proc.communicate(timeout=0.1)
                except subprocess.TimeoutExpired:
                    pass
                else:
                    raise RuntimeError("Could not start the RedisAI docker container. You can "
                                       "try setting up RedisAI locally by (by following the "
                                       "documentation https://oss.redislabs.com/redisai/quickstart/)"
                                       " and call the ``create`` API with target_uri as given in "
                                       "the example command below (this will set the host as "
                                       "localhost and port as 6379)\n\n"
                                       "    mlflow deployments create -t redisai -m <modeluri> ...\n\n")
            time.sleep(0.2)
    plugin.create_deployment(name, model_uri, flavor, config)
    logger.info("RedisAI docker container is up and the model has been deployed. "
                "Don't forget to stop the container once you are done using it.")


class RedisAIPlugin(BaseDeploymentClient):

    def __init__(self, uri):
        super().__init__(uri)
        server_config = Config()
        path = urlparse(uri).path
        if path:
            uri = f"redis:/{path}"
            self.con = redisai.Client.from_url(uri)
        else:
            self.con = redisai.Client(**server_config)

    def create_deployment(self, name, model_uri, flavor=None, config=None):
        device = config.get('device', 'CPU')
        autobatch_size = config.get('batchsize')
        tag = config.get('tag')
        path = Path(_download_artifact_from_uri(model_uri))
        model_config = path / 'MLmodel'
        if not model_config.exists():
            raise MlflowException(
                message=(
                    "Failed to find MLmodel configuration within the specified model's"
                    " root directory."),
                error_code=INVALID_PARAMETER_VALUE)
        model_config = Model.load(model_config)

        if flavor is None:
            flavor = get_preferred_deployment_flavor(model_config)
        else:
            validate_deployment_flavor(model_config, flavor)
        logger.info("Using the {} flavor for deployment!".format(flavor))

        if flavor == 'tensorflow':
            # TODO: test this for tf1.x and tf2.x
            tags = model_config.flavors[flavor]['meta_graph_tags']
            signaturedef = model_config.flavors[flavor]['signature_def_key']
            model_dir = path / model_config.flavors[flavor]['saved_model_dir']
            model, inputs, outputs = ml2rt.load_model(model_dir, tags, signaturedef)
        else:
            model_path = None
            for file in path.iterdir():
                if file.suffix == '.pt':
                    model_path = file
            if model_path is None:
                raise RuntimeError("Model file does not have a valid suffix. Expected ``.pt``")
            model = ml2rt.load_model(model_path)
            inputs = outputs = None
        backend = flavor2backend[flavor]
        self.con.modelset(name, backend, device, model, inputs=inputs, outputs=outputs, batch=autobatch_size, tag=tag)
        return {'name': name, 'flavor': flavor}

    def delete_deployment(self, name):
        self.con.modeldel(name)
        logger.info("Deleted model with key: {}".format(name))

    def update_deployment(self, name, model_uri=None, flavor=None, config=None):
        try:
            self.con.modelget(name, meta_only=True)
        except redis.exceptions.ConnectionError:
            raise MlflowException("Model doesn't exist. If you trying to create new "
                                  "deployment, use ``create_deployment``")
        else:
            ret = self.create_deployment(name, model_uri, flavor, config=config)
        return {'flavor': ret['flavor']}

    def list_deployments(self, **kwargs):
        return self.con.modelscan()

    def get_deployment(self, name):
        return self.con.modelget(name, meta_only=True)

    def predict(self, deployment_name, df):
        nparray = df.to_numpy()
        self.con.tensorset('array', nparray)
        # TODO: manage multiple inputs and multiple outputs
        self.con.modelrun(deployment_name, inputs=['array'], outputs=['output'])
        return self.con.tensorget('output')