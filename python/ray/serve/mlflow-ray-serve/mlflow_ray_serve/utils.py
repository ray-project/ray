import logging
import os
from mlflow.exceptions import MlflowException
from mlflow.protos.databricks_pb2 import RESOURCE_DOES_NOT_EXIST, INVALID_PARAMETER_VALUE


logger = logging.getLogger(__name__)


SUPPORTED_DEPLOYMENT_FLAVORS = ['torchscript', 'tensorflow']
flavor2backend = {
    'torchscript': 'torch',
    'tensorflow': 'tf'}


class Config(dict):
    def __init__(self):
        super().__init__()
        self['host'] = os.environ.get('REDIS_HOST')
        self['port'] = os.environ.get('REDIS_PORT')
        self['username'] = os.environ.get('REDIS_USERNAME')
        self['password'] = os.environ.get('REDIS_PASSWORD')
        self['db'] = os.environ.get('REDIS_DB')


def validate_deployment_flavor(model_config, flavor):
    """
    Checks that the specified flavor is a supported deployment flavor
    and is contained in the specified model. If one of these conditions
    is not met, an exception is thrown.

    :param model_config: An MLflow Model object
    :param flavor: The deployment flavor to validate
    """
    if flavor not in SUPPORTED_DEPLOYMENT_FLAVORS:
        raise MlflowException(
            message=(
                "The specified flavor: `{flavor_name}` is not supported for deployment."
                " Please use one of the supported flavors: {supported_flavor_names}".format(
                    flavor_name=flavor,
                    supported_flavor_names=SUPPORTED_DEPLOYMENT_FLAVORS)),
            error_code=INVALID_PARAMETER_VALUE)
    elif flavor not in model_config.flavors:
        raise MlflowException(
            message=("The specified model does not contain the specified deployment flavor:"
                     " `{flavor_name}`. Please use one of the following deployment flavors"
                     " that the model contains: {model_flavors}".format(
                        flavor_name=flavor, model_flavors=model_config.flavors.keys())),
            error_code=RESOURCE_DOES_NOT_EXIST)


def get_preferred_deployment_flavor(model_config):
    """
    Obtains the flavor that MLflow would prefer to use when deploying the model on RedisAI.
    If the model does not contain any supported flavors for deployment, an exception
    will be thrown.

    :param model_config: An MLflow model object
    :return: The name of the preferred deployment flavor for the specified model
    """
    # TODO: add onnx & TFlite
    possible_flavors = set(SUPPORTED_DEPLOYMENT_FLAVORS).intersection(model_config.flavors)
    if len(possible_flavors) == 1:
        return possible_flavors.pop()
    elif len(possible_flavors) > 1:
        flavor = possible_flavors.pop()
        logger.info("Found more than one possible flavors, using "
                    "the first: {}".format(flavor))
        return flavor
    else:
        raise MlflowException(
            message=(
                "The specified model does not contain any of the supported flavors for"
                " deployment. The model contains the following flavors: {model_flavors}."
                " Supported flavors: {supported_flavors}".format(
                    model_flavors=model_config.flavors.keys(),
                    supported_flavors=SUPPORTED_DEPLOYMENT_FLAVORS)),
            error_code=RESOURCE_DOES_NOT_EXIST)