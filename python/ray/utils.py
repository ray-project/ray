from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import binascii
import functools
import hashlib
import inspect
import numpy as np
import os
import sys
import threading
import uuid

import ray.gcs_utils
import ray.local_scheduler
import ray.ray_constants as ray_constants


def _random_string():
    id_hash = hashlib.sha1()
    id_hash.update(uuid.uuid4().bytes)
    id_bytes = id_hash.digest()
    assert len(id_bytes) == ray_constants.ID_SIZE
    return id_bytes


def is_cython(obj):
    """Check if an object is a Cython function or method"""

    # TODO(suo): We could split these into two functions, one for Cython
    # functions and another for Cython methods.
    # TODO(suo): There doesn't appear to be a Cython function 'type' we can
    # check against via isinstance. Please correct me if I'm wrong.
    def check_cython(x):
        return type(x).__name__ == "cython_function_or_method"

    # Check if function or method, respectively
    return check_cython(obj) or \
        (hasattr(obj, "__func__") and check_cython(obj.__func__))


def get_methods(cls):
    def pred(x):
        return inspect.isfunction(x) or inspect.ismethod(x) or is_cython(x)

    return inspect.getmembers(cls, predicate=pred)


def is_classmethod(f):
    """Returns whether the given method is a classmethod."""

    return hasattr(f, "__self__") and f.__self__ is not None


def compute_actor_method_function_id(class_name, attr):
    """Get the function ID corresponding to an actor method.

    Args:
        class_name (str): The class name of the actor.
        attr (str): The attribute name of the method.

    Returns:
        Function ID corresponding to the method.
    """
    function_id_hash = hashlib.sha1()
    function_id_hash.update(class_name.encode("ascii"))
    function_id_hash.update(attr.encode("ascii"))
    function_id = function_id_hash.digest()
    assert len(function_id) == ray_constants.ID_SIZE
    return ray.ObjectID(function_id)


def random_string():
    """Generate a random string to use as an ID.

    Note that users may seed numpy, which could cause this function to generate
    duplicate IDs. Therefore, we need to seed numpy ourselves, but we can't
    interfere with the state of the user's random number generator, so we
    extract the state of the random number generator and reset it after we are
    done.

    TODO(rkn): If we want to later guarantee that these are generated in a
    deterministic manner, then we will need to make some changes here.

    Returns:
        A random byte string of length ray_constants.ID_SIZE.
    """
    # Get the state of the numpy random number generator.
    numpy_state = np.random.get_state()
    # Try to use true randomness.
    np.random.seed(None)
    # Generate the random ID.
    random_id = np.random.bytes(ray_constants.ID_SIZE)
    # Reset the state of the numpy random number generator.
    np.random.set_state(numpy_state)
    return random_id


def decode(byte_str):
    """Make this unicode in Python 3, otherwise leave it as bytes."""
    if not isinstance(byte_str, bytes):
        raise ValueError("The argument must be a bytes object.")
    if sys.version_info >= (3, 0):
        return byte_str.decode("ascii")
    else:
        return byte_str


def binary_to_object_id(binary_object_id):
    return ray.ObjectID(binary_object_id)


def binary_to_hex(identifier):
    hex_identifier = binascii.hexlify(identifier)
    if sys.version_info >= (3, 0):
        hex_identifier = hex_identifier.decode()
    return hex_identifier


def hex_to_binary(hex_identifier):
    return binascii.unhexlify(hex_identifier)


def get_cuda_visible_devices():
    """Get the device IDs in the CUDA_VISIBLE_DEVICES environment variable.

    Returns:
        if CUDA_VISIBLE_DEVICES is set, this returns a list of integers with
            the IDs of the GPUs. If it is not set, this returns None.
    """
    gpu_ids_str = os.environ.get("CUDA_VISIBLE_DEVICES", None)

    if gpu_ids_str is None:
        return None

    if gpu_ids_str == "":
        return []

    return [int(i) for i in gpu_ids_str.split(",")]


def set_cuda_visible_devices(gpu_ids):
    """Set the CUDA_VISIBLE_DEVICES environment variable.

    Args:
        gpu_ids: This is a list of integers representing GPU IDs.
    """
    os.environ["CUDA_VISIBLE_DEVICES"] = ",".join([str(i) for i in gpu_ids])


def resources_from_resource_arguments(default_num_cpus, default_num_gpus,
                                      default_resources, runtime_num_cpus,
                                      runtime_num_gpus, runtime_resources):
    """Determine a task's resource requirements.

    Args:
        default_num_cpus: The default number of CPUs required by this function
            or actor method.
        default_num_gpus: The default number of GPUs required by this function
            or actor method.
        default_resources: The default custom resources required by this
            function or actor method.
        runtime_num_cpus: The number of CPUs requested when the task was
            invoked.
        runtime_num_gpus: The number of GPUs requested when the task was
            invoked.
        runtime_resources: The custom resources requested when the task was
            invoked.

    Returns:
        A dictionary of the resource requirements for the task.
    """
    if runtime_resources is not None:
        resources = runtime_resources.copy()
    elif default_resources is not None:
        resources = default_resources.copy()
    else:
        resources = {}

    if "CPU" in resources or "GPU" in resources:
        raise ValueError("The resources dictionary must not "
                         "contain the key 'CPU' or 'GPU'")

    assert default_num_cpus is not None
    resources["CPU"] = (default_num_cpus
                        if runtime_num_cpus is None else runtime_num_cpus)

    if runtime_num_gpus is not None:
        resources["GPU"] = runtime_num_gpus
    elif default_num_gpus is not None:
        resources["GPU"] = default_num_gpus

    return resources


def check_oversized_pickle(pickled, name, obj_type, worker):
    """Send a warning message if the pickled object is too large.

    Args:
        pickled: the pickled object.
        name: name of the pickled object.
        obj_type: type of the pickled object, can be 'function',
            'remote function', 'actor', or 'object'.
        worker: the worker used to send warning message.
    """
    length = len(pickled)
    if length <= ray_constants.PICKLE_OBJECT_WARNING_SIZE:
        return
    warning_message = (
        "Warning: The {} {} has size {} when pickled. "
        "It will be stored in Redis, which could cause memory issues. "
        "This may mean that its definition uses a large array or other object."
    ).format(obj_type, name, length)
    worker.logger.push_error_to_driver(
        ray_constants.PICKLING_LARGE_OBJECT_PUSH_ERROR,
        warning_message,
        driver_id=worker.task_driver_id.id())


class _ThreadSafeProxy(object):
    """This class is used to create a thread-safe proxy for a given object.
        Every method call will be guarded with a lock.

    Attributes:
        orig_obj (object): the original object.
        lock (threading.Lock): the lock object.
        _wrapper_cache (dict): a cache from original object's methods to
            the proxy methods.
    """

    def __init__(self, orig_obj, lock):
        self.orig_obj = orig_obj
        self.lock = lock
        self._wrapper_cache = {}

    def __getattr__(self, attr):
        orig_attr = getattr(self.orig_obj, attr)
        if not callable(orig_attr):
            # If the original attr is a field, just return it.
            return orig_attr
        else:
            # If the orginal attr is a method,
            # return a wrapper that guards the original method with a lock.
            wrapper = self._wrapper_cache.get(attr)
            if wrapper is None:

                @functools.wraps(orig_attr)
                def _wrapper(*args, **kwargs):
                    with self.lock:
                        return orig_attr(*args, **kwargs)

                self._wrapper_cache[attr] = _wrapper
                wrapper = _wrapper
            return wrapper


def thread_safe_client(client, lock=None):
    """Create a thread-safe proxy which locks every method call
    for the given client.

    Args:
        client: the client object to be guarded.
        lock: the lock object that will be used to lock client's methods.
            If None, a new lock will be used.

    Returns:
        A thread-safe proxy for the given client.
    """
    if lock is None:
        lock = threading.Lock()
    return _ThreadSafeProxy(client, lock)
