import logging

import ray
from ray import serve
from mlflow.deployments import BaseDeploymentClient

import mlflow.pyfunc

logger = logging.getLogger(__name__)

def target_help():
    # TODO: Improve
    help_string = ("The mlflow-ray-serve plugin integrates Ray Serve "
                       "with the MLFlow deployment API. ")
    return help_string

def run_local(name, model_uri, flavor=None, config=None):
    pass  # TODO


class MLFlowBackend:
    def __init__(self, model_uri):
        self.model = mlflow.pyfunc.load_model(model_uri=model_uri)

    async def __call__(self, request):
        # TODO: Does this correctly use Ray Serve pandas.df optimization?
        df = request.data
        return self.model.predict(df)


class RayServePlugin(BaseDeploymentClient):
    # TODO: raise MLflow exceptions as necessary

    def __init__(self, uri):
        super().__init__(uri)
        ray.init()
        self.client = serve.start()

    def help():
        return target_help()

    def create_deployment(self, name, model_uri, flavor=None, config=None):
        self.client.create_backend(
            name, MLFlowBackend, model_uri, config=config)
        self.client.create_endpoint(name, backend=name)
        # TODO: validate flavor
        # TODO: conda env integration
        return {"name": name, "config": config}

    def delete_deployment(self, name):
        self.client.delete_endpoint(name)
        self.client.delete_backend(name)
        logger.info("Deleted model with name: {}".format(name))

    def update_deployment(self, name, model_uri=None, flavor=None,
                          config=None):
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
        # TODO: raise error if name not found
        return {"name": name, "config": self.client.list_backends()[name]}

    def predict(self, deployment_name, df):
        return ray.get(self.client.get_handle(deployment_name).remote(df))
