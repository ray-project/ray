from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from ray import Language
from ray.remote_function import RemoteFunction
from ray import PythonFunctionDescriptor, JavaFunctionDescriptor
from ray.actor import ActorClass

__all__ = [
    "java_function", "java_actor_class", "python_function",
    "python_actor_class"
]


def java_function(class_name, function_name, signature=""):
    return RemoteFunction(
        Language.JAVA,
        JavaFunctionDescriptor(class_name, function_name, signature),
        None,  # num_cpus,
        None,  # num_gpus,
        None,  # memory,
        None,  # object_store_memory,
        None,  # resources,
        None,  # num_return_vals,
        None,  # max_calls,
        None)  # max_retries)


def java_actor_class(class_name, signature=""):
    return ActorClass._ray_from_function_descriptor(
        Language.JAVA,
        JavaFunctionDescriptor(class_name, "<init>", signature),
        0,  # max_reconstructions,
        None,  # num_cpus,
        None,  # num_gpus,
        None,  # memory,
        None,  # object_store_memory,
        None)  # resources,


def python_function(module_name, function_name):
    return RemoteFunction(
        Language.PYTHON,
        PythonFunctionDescriptor(module_name, function_name),
        None,  # num_cpus,
        None,  # num_gpus,
        None,  # memory,
        None,  # object_store_memory,
        None,  # resources,
        None,  # num_return_vals,
        None,  # max_calls,
        None)  # max_retries)


def python_actor_class(module_name, class_name):
    return ActorClass._ray_from_function_descriptor(
        Language.PYTHON,
        PythonFunctionDescriptor(module_name, "__init__", class_name),
        0,  # max_reconstructions,
        None,  # num_cpus,
        None,  # num_gpus,
        None,  # memory,
        None,  # object_store_memory,
        None)  # resources,
