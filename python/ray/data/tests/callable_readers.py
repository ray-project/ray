"""Callable classes for test_callable_read_task.py.

These are defined in a separate module to enable Ray serialization.
"""
import pyarrow as pa

import ray


@ray.remote
class InitTracker:
    """Ray actor to track initialization calls across distributed workers."""

    def __init__(self):
        self.init_calls = []

    def record_init(self, connection_string: str):
        """Record an initialization call."""
        self.init_calls.append({"connection_string": connection_string})

    def get_calls(self):
        """Get all recorded initialization calls."""
        return self.init_calls

    def reset(self):
        """Reset the recorded calls."""
        self.init_calls = []


class CallableReaderWithInit:
    """A callable reader class that tracks initialization.

    This class is used to test per-actor initialization with ActorPoolStrategy.
    """

    def __init__(self, connection_string: str, tracker_handle=None):
        """Initialize the reader.

        This should be called once per ReadTask constructor args set (e.g. per actor group),
        and the instance should be reused.

        Args:
            connection_string: Connection string (simulating a database connection)
            tracker_handle: Ray actor handle for tracking initialization calls
        """
        self.connection_string = connection_string
        self.tracker_handle = tracker_handle

        # Track initialization via Ray actor
        if tracker_handle is not None:
            ray.get(tracker_handle.record_init.remote(connection_string))

    def __call__(self, partition_id: int):
        """Execute the read operation.

        This is called each time the ReadTask is executed.

        Args:
            partition_id: The partition ID for this read task.
        """
        return [
            pa.Table.from_pydict(
                {
                    "partition_id": [partition_id],
                    "connection_string": [self.connection_string],
                    "data": [f"data_from_partition_{partition_id}"],
                }
            )
        ]
