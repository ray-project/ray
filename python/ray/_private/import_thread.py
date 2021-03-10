from collections import defaultdict
import threading
import traceback

import redis

import ray
from ray import ray_constants
from ray import cloudpickle as pickle
from ray import profiling

import logging

logger = logging.getLogger(__name__)


class ImportThread:
    """A thread used to import exports from the driver or other workers.

    Attributes:
        worker: the worker object in this process.
        mode: worker mode
        redis_client: the redis client used to query exports.
        threads_stopped (threading.Event): A threading event used to signal to
            the thread that it should exit.
        imported_collision_identifiers: This is a dictionary mapping collision
            identifiers for the exported remote functions and actor classes to
            the number of times that collision identifier has appeared. This is
            used to provide good error messages when the same function or class
            is exported many times.
    """

    def __init__(self, worker, mode, threads_stopped):
        self.worker = worker
        self.mode = mode
        self.redis_client = worker.redis_client
        self.threads_stopped = threads_stopped
        self.imported_collision_identifiers = defaultdict(int)

    def start(self):
        """Start the import thread."""
        self.t = threading.Thread(target=self._run, name="ray_import_thread")
        # Making the thread a daemon causes it to exit
        # when the main thread exits.
        self.t.daemon = True
        self.t.start()

    def join_import_thread(self):
        """Wait for the thread to exit."""
        self.t.join()

    def _run(self):
        import_pubsub_client = self.redis_client.pubsub()
        # Exports that are published after the call to
        # import_pubsub_client.subscribe and before the call to
        # import_pubsub_client.listen will still be processed in the loop.
        import_pubsub_client.subscribe("__keyspace@0__:Exports")
        # Keep track of the number of imports that we've imported.
        num_imported = 0

        try:
            # Get the exports that occurred before the call to subscribe.
            export_keys = self.redis_client.lrange("Exports", 0, -1)
            for key in export_keys:
                num_imported += 1
                self._process_key(key)

            while True:
                # Exit if we received a signal that we should stop.
                if self.threads_stopped.is_set():
                    return

                msg = import_pubsub_client.get_message()
                if msg is None:
                    self.threads_stopped.wait(timeout=0.01)
                    continue

                if msg["type"] == "subscribe":
                    continue
                assert msg["data"] == b"rpush"
                num_imports = self.redis_client.llen("Exports")
                assert num_imports >= num_imported
                for i in range(num_imported, num_imports):
                    num_imported += 1
                    key = self.redis_client.lindex("Exports", i)
                    self._process_key(key)
        except (OSError, redis.exceptions.ConnectionError) as e:
            logger.error(f"ImportThread: {e}")
        finally:
            # Close the pubsub client to avoid leaking file descriptors.
            import_pubsub_client.close()

    def _get_import_info_for_collision_detection(self, key):
        """Retrieve the collision identifier, type, and name of the import."""
        if key.startswith(b"RemoteFunction"):
            collision_identifier, function_name = (self.redis_client.hmget(
                key, ["collision_identifier", "function_name"]))
            return (collision_identifier,
                    ray._private.utils.decode(function_name),
                    "remote function")
        elif key.startswith(b"ActorClass"):
            collision_identifier, class_name = self.redis_client.hmget(
                key, ["collision_identifier", "class_name"])
            return collision_identifier, ray._private.utils.decode(
                class_name), "actor"

    def _process_key(self, key):
        """Process the given export key from redis."""
        if self.mode != ray.WORKER_MODE:
            # If the same remote function or actor definition appears to be
            # exported many times, then print a warning. We only issue this
            # warning from the driver so that it is only triggered once instead
            # of many times. TODO(rkn): We may want to push this to the driver
            # through Redis so that it can be displayed in the dashboard more
            # easily.
            if (key.startswith(b"RemoteFunction")
                    or key.startswith(b"ActorClass")):
                collision_identifier, name, import_type = (
                    self._get_import_info_for_collision_detection(key))
                self.imported_collision_identifiers[collision_identifier] += 1
                if (self.imported_collision_identifiers[collision_identifier]
                        == ray_constants.DUPLICATE_REMOTE_FUNCTION_THRESHOLD):
                    logger.warning(
                        "The %s '%s' has been exported %s times. It's "
                        "possible that this warning is accidental, but this "
                        "may indicate that the same remote function is being "
                        "defined repeatedly from within many tasks and "
                        "exported to all of the workers. This can be a "
                        "performance issue and can be resolved by defining "
                        "the remote function on the driver instead. See "
                        "https://github.com/ray-project/ray/issues/6240 for "
                        "more discussion.", import_type, name,
                        ray_constants.DUPLICATE_REMOTE_FUNCTION_THRESHOLD)

        if key.startswith(b"RemoteFunction"):
            # TODO (Alex): There's a race condition here if the worker is
            # shutdown before the function finished registering (because core
            # worker's global worker is unset before shutdown and is needed
            # for profiling).
            # with profiling.profile("register_remote_function"):
            (self.worker.function_actor_manager.
             fetch_and_register_remote_function(key))
        elif key.startswith(b"FunctionsToRun"):
            with profiling.profile("fetch_and_run_function"):
                self.fetch_and_execute_function_to_run(key)
        elif key.startswith(b"ActorClass"):
            # Keep track of the fact that this actor class has been
            # exported so that we know it is safe to turn this worker
            # into an actor of that class.
            self.worker.function_actor_manager.imported_actor_classes.add(key)
        # TODO(rkn): We may need to bring back the case of
        # fetching actor classes here.
        else:
            assert False, "This code should be unreachable."

    def fetch_and_execute_function_to_run(self, key):
        """Run on arbitrary function on the worker."""
        (job_id, serialized_function,
         run_on_other_drivers) = self.redis_client.hmget(
             key, ["job_id", "function", "run_on_other_drivers"])

        if (ray._private.utils.decode(run_on_other_drivers) == "False"
                and self.worker.mode == ray.SCRIPT_MODE
                and job_id != self.worker.current_job_id.binary()):
            return

        try:
            # FunctionActorManager may call pickle.loads at the same time.
            # Importing the same module in different threads causes deadlock.
            with self.worker.function_actor_manager.lock:
                # Deserialize the function.
                function = pickle.loads(serialized_function)
            # Run the function.
            function({"worker": self.worker})
        except Exception:
            # If an exception was thrown when the function was run, we record
            # the traceback and notify the scheduler of the failure.
            traceback_str = traceback.format_exc()
            # Log the error message.
            ray._private.utils.push_error_to_driver(
                self.worker,
                ray_constants.FUNCTION_TO_RUN_PUSH_ERROR,
                traceback_str,
                job_id=ray.JobID(job_id))
