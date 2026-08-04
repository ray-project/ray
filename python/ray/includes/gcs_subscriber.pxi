import random

from libcpp.memory cimport shared_ptr
from libcpp.string cimport string as c_string
from libcpp.vector cimport vector as c_vector
from libcpp.utility cimport move

from ray.includes.common cimport(
    CPythonGcsSubscriber,
    CErrorTableData,
    CLogBatch,
    PythonGetLogBatchLines,
    RAY_ERROR_INFO_CHANNEL,
    RAY_LOG_CHANNEL,
)

cdef class _GcsSubscriber:
    """Cython wrapper class of C++ `ray::pubsub::PythonGcsSubscriber`."""
    cdef:
        shared_ptr[CPythonGcsSubscriber] inner

    def _construct(self, address, channel, worker_id):
        cdef:
            c_worker_id = worker_id or b""
        # subscriber_id needs to match the binary format of a random
        # SubscriberID / UniqueID, which is 28 (kUniqueIDSize) random bytes.
        subscriber_id = bytes(bytearray(random.getrandbits(8) for _ in range(28)))
        gcs_address, gcs_port = parse_address(address)
        self.inner.reset(new CPythonGcsSubscriber(
            gcs_address, int(gcs_port), channel, subscriber_id, c_worker_id))

    def subscribe(self):
        """Registers a subscription for the subscriber's channel type.

        Before the registration, published messages in the channel will not be
        saved for the subscriber.
        """
        with nogil:
            check_status(self.inner.get().Subscribe())

    @property
    def last_batch_size(self):
        """Batch size of the result from last poll.

        Used to indicate whether the subscriber can keep up.
        """
        return self.inner.get().last_batch_size()

    def close(self):
        """Closes the subscriber and its active subscription."""
        with nogil:
            check_status(self.inner.get().Close())


cdef class GcsErrorSubscriber(_GcsSubscriber):
    """Subscriber to error info. Thread safe.

    Usage example:
        subscriber = GcsErrorSubscriber()
        # Subscribe to the error channel.
        subscriber.subscribe()
        ...
        while running:
            error_id, error_data = subscriber.poll()
            ......
        # Unsubscribe from the error channels.
        subscriber.close()
    """

    def __init__(self, address, worker_id=None):
        self._construct(address, RAY_ERROR_INFO_CHANNEL, worker_id)

    def poll(self, timeout=None):
        """Polls for new error messages.

        Returns:
            A tuple of error message ID and dict describing the error,
            or None, None if polling times out or subscriber closed.
        """
        cdef:
            CErrorTableData error_data
            c_string key_id
            int64_t timeout_ms = round(1000 * timeout) if timeout else -1

        with nogil:
            check_status(self.inner.get().PollError(&key_id, timeout_ms, &error_data))

        if key_id == b"":
            return None, None

        return (bytes(key_id), {
            "job_id": error_data.job_id(),
            "type": error_data.type().decode(),
            "error_message": error_data.error_message().decode(),
            "timestamp": error_data.timestamp(),
        })


cdef class GcsLogSubscriber(_GcsSubscriber):
    """Subscriber to logs. Thread safe.

    Usage example:
        subscriber = GcsLogSubscriber()
        # Subscribe to the log channel.
        subscriber.subscribe()
        ...
        while running:
            log = subscriber.poll()
            ......
        # Unsubscribe from the log channel.
        subscriber.close()
    """

    def __init__(self, address, worker_id=None):
        self._construct(address, RAY_LOG_CHANNEL, worker_id)

    def poll(self, timeout=None):
        """Polls for new log messages.

        Returns:
            A dict containing a batch of log lines and their metadata.
        """
        cdef:
            CLogBatch log_batch
            c_string key_id
            int64_t timeout_ms = round(1000 * timeout) if timeout else -1
            c_vector[c_string] c_log_lines
            c_string c_log_line

        with nogil:
            check_status(self.inner.get().PollLogs(&key_id, timeout_ms, &log_batch))

        result = {
            "ip": log_batch.ip().decode(),
            "pid": log_batch.pid().decode(),
            "job": log_batch.job_id().decode(),
            "is_err": log_batch.is_error(),
            "actor_name": log_batch.actor_name().decode(),
            "task_name": log_batch.task_name().decode(),
        }

        with nogil:
            c_log_lines = PythonGetLogBatchLines(move(log_batch))

        log_lines = []
        for c_log_line in c_log_lines:
            log_lines.append(c_log_line.decode())

        result["lines"] = log_lines
        return result
