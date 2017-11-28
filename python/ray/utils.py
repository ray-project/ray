from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import binascii
import collections
import json
import numpy as np
import redis
import sys

import ray.local_scheduler

ERROR_KEY_PREFIX = b"Error:"
DRIVER_ID_LENGTH = 20


def _random_string():
    return np.random.bytes(20)


def push_error_to_driver(redis_client, error_type, message, driver_id=None,
                         data=None):
    """Push an error message to the driver to be printed in the background.

    Args:
        redis_client: The redis client to use.
        error_type (str): The type of the error.
        message (str): The message that will be printed in the background
            on the driver.
        driver_id: The ID of the driver to push the error message to. If this
            is None, then the message will be pushed to all drivers.
        data: This should be a dictionary mapping strings to strings. It
            will be serialized with json and stored in Redis.
    """
    if driver_id is None:
        driver_id = DRIVER_ID_LENGTH * b"\x00"
    error_key = ERROR_KEY_PREFIX + driver_id + b":" + _random_string()
    data = {} if data is None else data
    redis_client.hmset(error_key, {"type": error_type,
                                   "message": message,
                                   "data": data})
    redis_client.rpush("ErrorKeys", error_key)


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
        A random byte string of length 20.
    """
    # Get the state of the numpy random number generator.
    numpy_state = np.random.get_state()
    # Try to use true randomness.
    np.random.seed(None)
    # Generate the random ID.
    random_id = np.random.bytes(20)
    # Reset the state of the numpy random number generator.
    np.random.set_state(numpy_state)
    return random_id


def decode(byte_str):
    """Make this unicode in Python 3, otherwise leave it as bytes."""
    if sys.version_info >= (3, 0):
        return byte_str.decode("ascii")
    else:
        return byte_str


def binary_to_object_id(binary_object_id):
    return ray.local_scheduler.ObjectID(binary_object_id)


def binary_to_hex(identifier):
    hex_identifier = binascii.hexlify(identifier)
    if sys.version_info >= (3, 0):
        hex_identifier = hex_identifier.decode()
    return hex_identifier


def hex_to_binary(hex_identifier):
    return binascii.unhexlify(hex_identifier)


FunctionProperties = collections.namedtuple("FunctionProperties",
                                            ["num_return_vals",
                                             "num_cpus",
                                             "num_gpus",
                                             "num_custom_resource",
                                             "max_calls"])
"""FunctionProperties: A named tuple storing remote functions information."""


def attempt_to_reserve_gpus(num_gpus, driver_id, local_scheduler,
                            redis_client):
    """Attempt to acquire GPUs on a particular local scheduler for an actor.

    Args:
        num_gpus: The number of GPUs to acquire.
        driver_id: The ID of the driver responsible for creating the actor.
        local_scheduler: Information about the local scheduler.
        redis_client: The redis client to use for interacting with Redis.

    Returns:
        True if the GPUs were successfully reserved and false otherwise.
    """
    assert num_gpus != 0
    local_scheduler_id = local_scheduler["DBClientID"]
    local_scheduler_total_gpus = int(local_scheduler["NumGPUs"])

    success = False

    # Attempt to acquire GPU IDs atomically.
    with redis_client.pipeline() as pipe:
        while True:
            try:
                # If this key is changed before the transaction below (the
                # multi/exec block), then the transaction will not take place.
                pipe.watch(local_scheduler_id)

                # Figure out which GPUs are currently in use.
                result = redis_client.hget(local_scheduler_id, "gpus_in_use")
                gpus_in_use = dict() if result is None else json.loads(
                    result.decode("ascii"))
                num_gpus_in_use = 0
                for key in gpus_in_use:
                    num_gpus_in_use += gpus_in_use[key]
                assert num_gpus_in_use <= local_scheduler_total_gpus

                pipe.multi()

                if local_scheduler_total_gpus - num_gpus_in_use >= num_gpus:
                    # There are enough available GPUs, so try to reserve some.
                    # We use the hex driver ID in hex as a dictionary key so
                    # that the dictionary is JSON serializable.
                    driver_id_hex = binary_to_hex(driver_id)
                    if driver_id_hex not in gpus_in_use:
                        gpus_in_use[driver_id_hex] = 0
                    gpus_in_use[driver_id_hex] += num_gpus

                    # Stick the updated GPU IDs back in Redis
                    pipe.hset(local_scheduler_id, "gpus_in_use",
                              json.dumps(gpus_in_use))
                    success = True

                pipe.execute()
                # If a WatchError is not raised, then the operations should
                # have gone through atomically.
                break
            except redis.WatchError:
                # Another client must have changed the watched key between the
                # time we started WATCHing it and the pipeline's execution. We
                # should just retry.
                success = False
                continue

    return success


def release_gpus_in_use(driver_id, local_scheduler_id, gpu_ids, redis_client):
    """Release the GPUs that a given worker was using.

    Note that this does not affect the local scheduler's bookkeeping. It only
    affects the GPU allocations which are recorded in the primary Redis shard,
    which are redundant with the local scheduler bookkeeping.

    Args:
        driver_id: The ID of the driver that is releasing some GPUs.
        local_scheduler_id: The ID of the local scheduler that owns the GPUs
            being released.
        gpu_ids: The IDs of the GPUs being released.
        redis_client: A client for the primary Redis shard.
    """
    # Attempt to release GPU IDs atomically.
    with redis_client.pipeline() as pipe:
        while True:
            try:
                # If this key is changed before the transaction below (the
                # multi/exec block), then the transaction will not take place.
                pipe.watch(local_scheduler_id)

                # Figure out which GPUs are currently in use.
                result = redis_client.hget(local_scheduler_id, "gpus_in_use")
                gpus_in_use = dict() if result is None else json.loads(
                    result.decode("ascii"))

                assert driver_id in gpus_in_use
                assert gpus_in_use[driver_id] >= len(gpu_ids)

                gpus_in_use[driver_id] -= len(gpu_ids)

                pipe.multi()

                pipe.hset(local_scheduler_id, "gpus_in_use",
                          json.dumps(gpus_in_use))

                pipe.execute()
                # If a WatchError is not raised, then the operations should
                # have gone through atomically.
                break
            except redis.WatchError:
                # Another client must have changed the watched key between the
                # time we started WATCHing it and the pipeline's execution. We
                # should just retry.
                continue


def select_local_scheduler(driver_id, local_schedulers, num_gpus,
                           redis_client):
    """Select a local scheduler to assign this actor to.

    Args:
        driver_id: The ID of the driver who the actor is for.
        local_schedulers: A list of dictionaries of information about the local
            schedulers.
        num_gpus (int): The number of GPUs that must be reserved for this
            actor.
        redis_client: The Redis client to use for interacting with Redis.

    Returns:
        The ID of the local scheduler that has been chosen.

    Raises:
        Exception: An exception is raised if no local scheduler can be found
            with sufficient resources.
    """
    local_scheduler_id = None
    # Loop through all of the local schedulers in a random order.
    local_schedulers = np.random.permutation(local_schedulers)
    for local_scheduler in local_schedulers:
        if local_scheduler["NumCPUs"] < 1:
            continue
        if local_scheduler["NumGPUs"] < num_gpus:
            continue
        if num_gpus == 0:
            local_scheduler_id = hex_to_binary(local_scheduler["DBClientID"])
            break
        else:
            # Try to reserve enough GPUs on this local scheduler.
            success = attempt_to_reserve_gpus(num_gpus, driver_id,
                                              local_scheduler, redis_client)
            if success:
                local_scheduler_id = hex_to_binary(
                                         local_scheduler["DBClientID"])
                break

    if local_scheduler_id is None:
        raise Exception("Could not find a node with enough GPUs or other "
                        "resources to create this actor. The local scheduler "
                        "information is {}.".format(local_schedulers))

    return local_scheduler_id


def publish_actor_creation(actor_id, driver_id, local_scheduler_id,
                           reconstruct, redis_client):
    """Publish a notification that an actor should be created.

    This broadcast will be received by all of the local schedulers. The local
    scheduler whose ID is being broadcast will create the actor. Any other
    local schedulers that have already created the actor will kill it. All
    local schedulers will update their internal data structures to redirect
    tasks for this actor to the new local scheduler.

    Args:
        actor_id: The ID of the actor involved.
        driver_id: The ID of the driver responsible for the actor.
        local_scheduler_id: The ID of the local scheduler that is suposed to
            create the actor.
        reconstruct: True if the actor should be created in "reconstruct" mode.
        redis_client: The client used to interact with Redis.
    """
    reconstruct_bit = b"1" if reconstruct else b"0"
    # Really we should encode this message as a flatbuffer object. However,
    # we're having trouble getting that to work. It almost works, but in Python
    # 2.7, builder.CreateString fails on byte strings that contain characters
    # outside range(128).
    redis_client.publish("actor_notifications",
                         actor_id + driver_id + local_scheduler_id +
                         reconstruct_bit)
