from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import time

import pyarrow
import pyarrow.plasma as plasma
# TODO: Cleanup imports after implement `serialization` module.
import ray.cloudpickle as pickle
from ray.dataflow.exceptions import RayTaskError
import ray.plasma
from ray.serialization import CloudPickleError, RayNotDictionarySerializable
from ray.services import logger
import ray.ray_constants as ray_constants
from ray.utils import thread_safe_client
import ray.worker

class ObjectStoreClient(object):
    def __init__(self, worker, memcopy_threads=12):
        """Data client for getting & putting objects.

        Args:
            worker: The worker instance.
            memcopy_threads (int):
                The number of threads Plasma should use when putting an object
                in the object store.
        """
        self.worker = worker
        self.memcopy_threads = memcopy_threads
        # A dictionary that maps from driver id to SerializationContext
        # TODO: clean up the SerializationContext once the job finished.
        self.serialization_context_map = {}

    def connect(self, store_socket_name, manager_socket_name, release_delay):
        """Create a connection to plasma client.

        Args:
            store_socket_name (str):
                Name of the socket the plasma store is listening at.
            manager_socket_name (str):
                Name of the socket the plasma manager is listening at.
            release_delay (int):
                The maximum number of objects that the client will keep and
                delay releasing (for caching reasons).

        """

        if not self.use_raylet:
            self.plasma_client = thread_safe_client(
                plasma.connect(
                    store_socket_name, manager_socket_name, release_delay))
        else:
            self.plasma_client = thread_safe_client(
                plasma.connect(store_socket_name, "", release_delay))

    def disconnect(self):
        self.plasma_client.disconnect()

    def get_serialization_context(self, driver_id):
        """Get the SerializationContext of the driver that this worker is processing.

        Args:
            driver_id: The ID of the driver that indicates which driver to get
                the serialization context for.

        Returns:
            The serialization context of the given driver.
        """

        # TODO: `serialization` module should have a class maintaining
        # serialization context info.
        if driver_id not in self.serialization_context_map:
            ray.worker._initialize_serialization(driver_id)
        return self.serialization_context_map[driver_id]

    def clear(self):
        self.serialization_context_map.clear()

    @property
    def mode(self):
        return self.worker.mode

    @property
    def lock(self):
        return self.worker.lock

    @property
    def state_lock(self):
        return self.worker.state_lock

    @property
    def use_raylet(self):
        return self.worker.use_raylet

    @property
    def task_driver_id(self):
        return self.worker.task_driver_id

    @property
    def local_scheduler_client(self):
        return self.worker.local_scheduler_client

    @property
    def worker_fetch_request_size(self):
        return ray._config.worker_fetch_request_size()

    @property
    def worker_get_request_size(self):
        return ray._config.worker_get_request_size()

    def store_and_register(self, object_id, value, depth=100):
        """Store an object and attempt to register its class if needed.

        Args:
            object_id: The ID of the object to store.
            value: The value to put in the object store.
            depth: The maximum number of classes to recursively register.

        Raises:
            Exception: An exception is raised if the attempt to store the
                object fails. This can happen if there is already an object
                with the same ID in the object store or if the object store is
                full.
        """
        counter = 0
        while True:
            if counter == depth:
                raise Exception("Ray exceeded the maximum number of classes "
                                "that it will recursively serialize when "
                                "attempting to serialize an object of "
                                "type {}.".format(type(value)))
            counter += 1
            try:
                self.plasma_client.put(
                    value,
                    object_id=pyarrow.plasma.ObjectID(object_id.id()),
                    memcopy_threads=self.memcopy_threads,
                    serialization_context=self.get_serialization_context(
                        self.task_driver_id))
                break
            except pyarrow.SerializationCallbackError as e:
                # TODO(suquark): Move these fallbacks into `serialization`
                # module
                example_object = e.example_object
                try:
                    ray.worker.register_custom_serializer(
                        type(example_object), use_dict=True)
                    warning_message = ("WARNING: Serializing objects of type "
                                       "{} by expanding them as dictionaries "
                                       "of their fields. This behavior may "
                                       "be incorrect in some cases.".format(
                        type(example_object)))
                    logger.warning(warning_message)
                except (RayNotDictionarySerializable, CloudPickleError,
                        pickle.pickle.PicklingError, Exception):
                    # We also handle generic exceptions here because
                    # cloudpickle can fail with many different types of errors.
                    try:
                        ray.worker.register_custom_serializer(
                            type(example_object), use_pickle=True)
                        warning_message = ("WARNING: Falling back to "
                                           "serializing objects of type {} by "
                                           "using pickle. This may be "
                                           "inefficient.".format(
                            type(example_object)))
                        logger.warning(warning_message)
                    except CloudPickleError:
                        ray.worker.register_custom_serializer(
                            type(example_object), use_pickle=True, local=True)
                        warning_message = ("WARNING: Pickling the class {} "
                                           "failed, so we are using pickle "
                                           "and only registering the class "
                                           "locally.".format(
                            type(example_object)))
                        logger.warning(warning_message)

    def put_object(self, object_id, value):
        """Put value in the local object store with object id objectid.

        This assumes that the value for objectid has not yet been placed in the
        local object store.

        Args:
            object_id (object_id.ObjectID): The object ID of the value to be
                put.
            value: The value to put in the object store.

        Raises:
            Exception: An exception is raised if the attempt to store the
                object fails. This can happen if there is already an object
                with the same ID in the object store or if the object store is
                full.
        """
        # Make sure that the value is not an object ID.
        if isinstance(value, ray.ObjectID):
            raise Exception("Calling 'put' on an ObjectID is not allowed "
                            "(similarly, returning an ObjectID from a remote "
                            "function is not allowed). If you really want to "
                            "do this, you can wrap the ObjectID in a list and "
                            "call 'put' on it (or return it).")

        # Serialize and put the object in the object store.
        try:
            self.store_and_register(object_id, value)
        except pyarrow.PlasmaObjectExists:
            # The object already exists in the object store, so there is no
            # need to add it again. TODO(rkn): We need to compare the hashes
            # and make sure that the objects are in fact the same. We also
            # should return an error code to the caller instead of printing a
            # message.
            logger.info(
                "The object with ID {} already exists in the object store."
                    .format(object_id))

    def retrieve_and_deserialize(self, object_ids, timeout, error_timeout=10):
        start_time = time.time()
        # Only send the warning once.
        warning_sent = False
        while True:
            try:
                # We divide very large get requests into smaller get requests
                # so that a single get request doesn't block the store for a
                # long time, if the store is blocked, it can block the manager
                # as well as a consequence.
                results = []
                for i in range(0, len(object_ids),
                               self.worker_get_request_size):
                    results += self.plasma_client.get(
                        object_ids[i:(i + self.worker_get_request_size)],
                        timeout,
                        self.get_serialization_context(self.task_driver_id))
                return results
            except pyarrow.lib.ArrowInvalid:
                # TODO(ekl): the local scheduler could include relevant
                # metadata in the task kill case for a better error message
                invalid_error = RayTaskError(
                    "<unknown>", None,
                    "Invalid return value: likely worker died or was killed "
                    "while executing the task.")
                return [invalid_error] * len(object_ids)
            except pyarrow.DeserializationCallbackError:
                # Wait a little bit for the import thread to import the class.
                # If we currently have the worker lock, we need to release it
                # so that the import thread can acquire it.
                if self.mode == WORKER_MODE:
                    self.lock.release()
                time.sleep(0.01)
                if self.mode == WORKER_MODE:
                    self.lock.acquire()

                if time.time() - start_time > error_timeout:
                    warning_message = ("This worker or driver is waiting to "
                                       "receive a class definition so that it "
                                       "can deserialize an object from the "
                                       "object store. This may be fine, or it "
                                       "may be a bug.")
                    if not warning_sent:
                        ray.utils.push_error_to_driver(
                            self,
                            ray_constants.WAIT_FOR_CLASS_PUSH_ERROR,
                            warning_message,
                            driver_id=self.task_driver_id.id())
                    warning_sent = True

    def get_object(self, object_ids):
        """Get the value or values in the object store associated with the IDs.

        Return the values from the local object store for object_ids. This will
        block until all the values for object_ids have been written to the
        local object store.

        Args:
            object_ids (List[object_id.ObjectID]): A list of the object IDs
                whose values should be retrieved.
        """
        # Make sure that the values are object IDs.
        for object_id in object_ids:
            if not isinstance(object_id, ray.ObjectID):
                raise Exception("Attempting to call `get` on the value {}, "
                                "which is not an ObjectID.".format(object_id))
        # Do an initial fetch for remote objects. We divide the fetch into
        # smaller fetches so as to not block the manager for a prolonged period
        # of time in a single call.
        plain_object_ids = [
            plasma.ObjectID(object_id.id()) for object_id in object_ids
        ]
        for i in range(0, len(object_ids), self.worker_fetch_request_size):
            if not self.use_raylet:
                self.plasma_client.fetch(plain_object_ids[i:(
                        i + self.worker_fetch_request_size)])
            else:
                self.local_scheduler_client.reconstruct_objects(
                    object_ids[i:(i + self.worker_fetch_request_size)], True)

        # Get the objects. We initially try to get the objects immediately.
        final_results = self.retrieve_and_deserialize(plain_object_ids, 0)
        # Construct a dictionary mapping object IDs that we haven't gotten yet
        # to their original index in the object_ids argument.
        unready_ids = {
            plain_object_ids[i].binary(): i
            for (i, val) in enumerate(final_results)
            if val is plasma.ObjectNotAvailable
        }

        if len(unready_ids) > 0:
            with self.state_lock:
                # Try reconstructing any objects we haven't gotten yet. Try to
                # get them until at least get_timeout_milliseconds
                # milliseconds passes, then repeat.
                while len(unready_ids) > 0:
                    object_ids_to_fetch = [
                        plasma.ObjectID(unready_id)
                        for unready_id in unready_ids.keys()
                    ]
                    ray_object_ids_to_fetch = [
                        ray.ObjectID(unready_id)
                        for unready_id in unready_ids.keys()
                    ]
                    fetch_request_size = self.worker_fetch_request_size
                    for i in range(0, len(object_ids_to_fetch),
                                   fetch_request_size):
                        if not self.use_raylet:
                            for unready_id in ray_object_ids_to_fetch[i:(
                                    i + fetch_request_size)]:
                                (self.local_scheduler_client.
                                 reconstruct_objects([unready_id], False))
                            # Do another fetch for objects that aren't
                            # available locally yet, in case they were evicted
                            # since the last fetch. We divide the fetch into
                            # smaller fetches so as to not block the manager
                            # for a prolonged period of time in a single call.
                            # This is only necessary for legacy ray since
                            # reconstruction and fetch are implemented by
                            # different processes.
                            self.plasma_client.fetch(object_ids_to_fetch[i:(
                                    i + fetch_request_size)])
                        else:
                            self.local_scheduler_client.reconstruct_objects(
                                ray_object_ids_to_fetch[i:(
                                        i + fetch_request_size)], False)
                    results = self.retrieve_and_deserialize(
                        object_ids_to_fetch,
                        max([
                            ray._config.get_timeout_milliseconds(),
                            int(0.01 * len(unready_ids))
                        ]))
                    # Remove any entries for objects we received during this
                    # iteration so we don't retrieve the same object twice.
                    for i, val in enumerate(results):
                        if val is not plasma.ObjectNotAvailable:
                            object_id = object_ids_to_fetch[i].binary()
                            index = unready_ids[object_id]
                            final_results[index] = val
                            unready_ids.pop(object_id)

                # If there were objects that we weren't able to get locally,
                # let the local scheduler know that we're now unblocked.
                self.local_scheduler_client.notify_unblocked()

        assert len(final_results) == len(object_ids)
        return final_results
