import ray
import pyarrow
import pyarrow.plasma as plasma
from ray.utils import random_string

# The default return value to put in the object store.
RETURN_VALUE = 0

if __name__ == '__main__':
    import sys

    raylet_socket = sys.argv[1]
    store_name = sys.argv[2]
    print("Worker started, connecting to", raylet_socket, store_name)

    # Connect to the Raylet and object store.
    worker = ray.local_scheduler.LocalSchedulerClient(raylet_socket,
                                                      random_string(),
                                                      random_string(), True, 0)
    plasma_client = plasma.connect(store_name, "", 0)
    serialization_context = pyarrow.default_serialization_context()

    # Get tasks in a loop.
    while True:
        print("Worker waiting for task")
        task = worker.get_task()
        print("Worker assigned", task.task_id(),
              "arguments", [ray.utils.binary_to_hex(argument.id()) for argument
                            in task.arguments()])

        # Get the arguments. NOTE(swang): This will hang forever if the
        # arguments have been evicted.
        argument_ids = [plasma.ObjectID(argument.id()) for argument in
                        task.arguments()]
        arguments = plasma_client.get(argument_ids, 0, serialization_context)
        assert(all(argument == RETURN_VALUE for argument in arguments))

        for object_id in task.returns():
            plasma_client.put(RETURN_VALUE, plasma.ObjectID(object_id.id()))
        print("Worker returned", [ray.utils.binary_to_hex(return_id.id()) for
                                  return_id in task.returns()])

        # Release the arguments.
        del arguments
