from typing import Optional, Tuple, TypedDict, Union

class GcsErrorPollDict(TypedDict):
    job_id: bytes
    type: str
    error_message: str
    timestamp: float

class GcsLogPollDict(TypedDict):
    ip: str
    pid: str
    job: str
    is_err: bool
    lines: list[str]
    actor_name: str
    task_name: str

class _GcsSubscriber:
    """Cython wrapper class of C++ `ray::pubsub::PythonGcsSubscriber`."""
    def _construct(self, address: str, channel: int, worker_id: Union[str,bytes]): ...

    def subscribe(self):
        """Registers a subscription for the subscriber's channel type.

        Before the registration, published messages in the channel will not be
        saved for the subscriber.
        """
        ...

    @property
    def last_batch_size(self) -> int:
        """Batch size of the result from last poll.

        Used to indicate whether the subscriber can keep up.
        """
        ...

    def close(self):
        """Closes the subscriber and its active subscription."""
        ...

class GcsErrorSubscriber(_GcsSubscriber):
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

    def __init__(self, address: str, worker_id: Optional[Union[str,bytes]]=None): ...

    def poll(self, timeout: Optional[float]=None) -> Union[Tuple[None,None],Tuple[bytes,GcsErrorPollDict]]:
        """Polls for new error messages.

        Returns:
            A tuple of error message ID and dict describing the error,
            or None, None if polling times out or subscriber closed.
        """
        ...


class GcsLogSubscriber(_GcsSubscriber):
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

    def __init__(self, address: str, worker_id: Optional[Union[str,bytes]]=None): ...

    def poll(self, timeout: Optional[float]=None) -> GcsLogPollDict:
        """Polls for new log messages.

        Returns:
            A dict containing a batch of log lines and their metadata.
        """
        ...
