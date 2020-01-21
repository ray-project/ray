from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from ray import Language
from ray import PythonFunctionDescriptor, JavaFunctionDescriptor
import ray.signature

__all__ = [
    "java_function", "java_actor_class", "python_function",
    "python_actor_class"
]


def format_args(worker, language, function_signature, args, kwargs):
    """Format args for various languages.

    Args:
        worker: The global worker instance.
        language: Target language.
        function_signature: The function signature for cross language.
        args: The arguments for cross language.
        kwargs: The keyword arguments for cross language.

    Returns:
        List of args and kwargs (if supported).
    """
    if not worker.load_code_from_local:
        raise Exception("Cross language feature needs "
                        "--load-code-from-local to be set.")
    if language == Language.PYTHON:
        # When cross language calling Python from Python,
        # the args and kwargs needs to be compatible with
        # ray.signature.recover_args.
        return ray.signature.flatten_args(function_signature, args, kwargs)
    else:
        if kwargs:
            raise Exception("Cross language remote functions "
                            "does not support kwargs.")
        return args


def get_function_descriptor_for_actor_method(
        language, actor_creation_function_descriptor, method_name):
    """Get function descriptor for cross language actor method call.

    Args:
        language: Target language.
        actor_creation_function_descriptor:
            The function signature for actor creation.
        method_name: The name of actor method.

    Returns:
        Function descriptor for cross language actor method call.
    """
    if language == Language.PYTHON:
        return PythonFunctionDescriptor(
            actor_creation_function_descriptor.module_name, method_name,
            actor_creation_function_descriptor.class_name)
    elif language == Language.JAVA:
        return JavaFunctionDescriptor(
            actor_creation_function_descriptor.class_name,
            method_name,
            # Currently not support call actor method with signature.
            "")
    else:
        raise NotImplementedError("Cross language remote actor method "
                                  "not support language {}".format(language))


def java_function(class_name, function_name):
    from ray.remote_function import RemoteFunction
    return RemoteFunction(
        Language.JAVA,
        JavaFunctionDescriptor(class_name, function_name, ""),
        None,  # num_cpus,
        None,  # num_gpus,
        None,  # memory,
        None,  # object_store_memory,
        None,  # resources,
        None,  # num_return_vals,
        None,  # max_calls,
        None)  # max_retries)


def java_actor_class(class_name):
    from ray.actor import ActorClass
    return ActorClass._ray_from_function_descriptor(
        Language.JAVA,
        JavaFunctionDescriptor(class_name, "<init>", ""),
        0,  # max_reconstructions,
        None,  # num_cpus,
        None,  # num_gpus,
        None,  # memory,
        None,  # object_store_memory,
        None)  # resources,


def python_function(module_name, function_name):
    from ray.remote_function import RemoteFunction
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
    from ray.actor import ActorClass
    return ActorClass._ray_from_function_descriptor(
        Language.PYTHON,
        PythonFunctionDescriptor(module_name, "__init__", class_name),
        0,  # max_reconstructions,
        None,  # num_cpus,
        None,  # num_gpus,
        None,  # memory,
        None,  # object_store_memory,
        None)  # resources,
