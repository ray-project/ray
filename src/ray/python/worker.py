import logging

import ray
import pyarrow
import pyarrow.plasma as plasma
from ray.utils import random_string


logger = logging.getLogger(__name__)

# The default return value to put in the object store.
RETURN_VALUE = 0

class Worker(object):

    total_task_count = 0

    def __init__(self, raylet_socket_name, object_store_socket_name,
                 is_worker):
        # Connect to the Raylet and object store.
        self.node_manager_client = ray.local_scheduler.LocalSchedulerClient(
            raylet_socket_name, random_string(), is_worker)
        self.plasma_client = plasma.connect(object_store_socket_name, "", 0)
        self.serialization_context = pyarrow.default_serialization_context()
        self.raylet_socket_name = raylet_socket_name
        self.object_store_socket_name = object_store_socket_name

    def main_loop(self):
        while True:
            self.get_task()

    def get(self, object_ids, timeout_ms=-1):
        for object_id in object_ids:
            self.node_manager_client.reconstruct_object(object_id.id())
        plasma_ids = [plasma.ObjectID(argument.id()) for argument in
                      object_ids]
        values = self.plasma_client.get(plasma_ids, timeout_ms,
                                        self.serialization_context)
        logger.debug(values)
        logger.debug("[WORKER] values=", values)
        assert(all(value[0] == RETURN_VALUE for value in values))
        return values

    def get_task(self):
        print("[WORKER] waiting for task")
        logger.debug("[WORKER] waiting for task")
        task = self.node_manager_client.get_task()
        #logger.debug("Worker assigned", task.task_id(),
        print("[WORKER] Worker assigned", task.task_id(),
                     "arguments", [ray.utils.binary_to_hex(argument.id()) for
                                   argument in task.arguments()])

        # Get the arguments. NOTE(swang): This will hang forever if the
        # arguments have been evicted.
        arguments = self.get(task.arguments())

        for object_id in task.returns():
            self.plasma_client.put((RETURN_VALUE,self.raylet_socket_name),
                                   plasma.ObjectID(object_id.id()))
            objval = self.plasma_client.get([plasma.ObjectID(object_id.id())])
            print("[WORKER.GETTASK]: value put matches value get: ", objval)
            assert(all([o[0] == RETURN_VALUE for o in objval]))

        print("Worker returned",
        #logger.debug("Worker returned",
                     [ray.utils.binary_to_hex(return_id.id()) for return_id in
                      task.returns()])

        # Release the arguments.
        del arguments
