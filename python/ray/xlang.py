from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import ray
from ray import Language
from ray.remote_function import RemoteFunction
from ray.actor import ActorClass

__all__ = ['java_function', 'java_actor_class', 'python_function', 'python_actor_class']


def java_function(class_name, function_name):
    # Java Function Descriptor: [class name, function name, signature]
    return RemoteFunction(
            Language.JAVA, [class_name.encode('ascii'), function_name.encode('ascii'), b''],
            None,  # num_cpus,
            None,  # num_gpus,
            None,  # memory,
            None,  # object_store_memory,
            None,  # resources,
            None,  # num_return_vals,
            None,  # max_calls,
            None)  # max_retries)


def java_actor_class(class_name):
    # Java Function Descriptor: [class name, function name, signature]
    return ActorClass._ray_from_descriptor_list(
            Language.JAVA, [class_name.encode('ascii'), b'<init>', b''],
            0,  # max_reconstructions,
            None,  # num_cpus,
            None,  # num_gpus,
            None,  # memory,
            None,  # object_store_memory,
            None)  # resources,


def python_function(module_name, function_name):
    # Python Function Descriptor: [module name, class name, function name]
    return RemoteFunction(
            Language.PYTHON, [module_name.encode('ascii'), b'', function_name.encode('ascii')],
            None,  # num_cpus,
            None,  # num_gpus,
            None,  # memory,
            None,  # object_store_memory,
            None,  # resources,
            None,  # num_return_vals,
            None,  # max_calls,
            None)  # max_retries)


def python_actor_class(module_name, class_name):
    # Python Function Descriptor: [module name, class name, function name]
    return ActorClass._ray_from_descriptor_list(
            Language.PYTHON, [module_name.encode('ascii'), class_name.encode('ascii'), b'__init__'],
            0,  # max_reconstructions,
            None,  # num_cpus,
            None,  # num_gpus,
            None,  # memory,
            None,  # object_store_memory,
            None)  # resources,
