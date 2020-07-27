from abc import ABC, abstractmethod

from ray.streaming.datastream import StreamSource
from ray.streaming.function import LocalFileSourceFunction
from ray.streaming.function import CollectionSourceFunction
from ray.streaming.function import SourceFunction
from ray.streaming.runtime.gateway_client import GatewayClient


class StreamingContext:
    """
    Main entry point for ray streaming functionality.
    A StreamingContext is also a wrapper of java
    `io.ray.streaming.api.context.StreamingContext`
    """

    class Builder:
        def __init__(self):
            self._options = {}

        def option(self, key=None, value=None, conf=None):
            """
            Sets a config option. Options set using this method are
            automatically propagated to :class:`StreamingContext`'s own
            configuration.

            Args:
                key: a key name string for configuration property
                value: a value string for configuration property
                conf: multi key-value pairs as a dict

            Returns:
                self
            """
            if key is not None:
                assert value is not None
                self._options[key] = str(value)
            if conf is not None:
                for k, v in conf.items():
                    self._options[k] = v
            return self

        def build(self):
            """
            Creates a StreamingContext based on the options set in this
            builder.
            """
            ctx = StreamingContext()
            ctx._gateway_client.with_config(self._options)
            return ctx

    def __init__(self):
        self.__gateway_client = GatewayClient()
        self._j_ctx = self._gateway_client.create_streaming_context()

    def source(self, source_func: SourceFunction):
        """Create an input data stream with a SourceFunction

        Args:
            source_func: the SourceFunction used to create the data stream

        Returns:
            The data stream constructed from the source_func
        """
        return StreamSource.build_source(self, source_func)

    def from_values(self, *values):
        """Creates a data stream from values

        Args:
            values: The elements to create the data stream from.

        Returns:
            The data stream representing the given values
        """
        return self.from_collection(values)

    def from_collection(self, values):
        """Creates a data stream from the given non-empty collection.

        Args:
            values: The collection of elements to create the data stream from.

        Returns:
            The data stream representing the given collection.
        """
        assert values, "values shouldn't be None or empty"
        func = CollectionSourceFunction(values)
        return self.source(func)

    def read_text_file(self, filename: str):
        """Reads the given file line-by-line and creates a data stream that
         contains a string with the contents of each such line."""
        func = LocalFileSourceFunction(filename)
        return self.source(func)

    def submit(self, job_name: str):
        """Submit job for execution.

        Args:
            job_name: name of the job

        Returns:
            An JobSubmissionResult future
        """
        self._gateway_client.execute(job_name)
        # TODO return a JobSubmissionResult future

    def execute(self, job_name: str):
        """Execute the job. This method will block until job finished.

        Args:
            job_name: name of the job
        """
        # TODO support block to job finish
        # job_submit_result = self.submit(job_name)
        # job_submit_result.wait_finish()
        raise Exception("Unsupported")

    @property
    def _gateway_client(self):
        return self.__gateway_client


class RuntimeContext(ABC):
    @abstractmethod
    def get_task_id(self):
        """
        Returns:
            Task id of the parallel task.
        """
        pass

    @abstractmethod
    def get_task_index(self):
        """
        Gets the index of this parallel subtask. The index starts from 0
        and goes up to  parallelism-1 (parallelism as returned by
        `get_parallelism()`).

        Returns:
            The index of the parallel subtask.
        """
        pass

    @abstractmethod
    def get_parallelism(self):
        """
        Returns:
            The parallelism with which the parallel task runs.
        """
        pass

    @abstractmethod
    def get_config(self):
        """
        Returns:
            The config with which the parallel task runs.
        """
        pass

    @abstractmethod
    def get_job_config(self):
        """
        Returns:
            The job config.
        """
        pass


class RuntimeContextImpl(RuntimeContext):
    def __init__(self, task_id, task_index, parallelism, **kargs):
        self.task_id = task_id
        self.task_index = task_index
        self.parallelism = parallelism
        self.config = kargs.get("config", {})
        self.job_config = kargs.get("job_config", {})

    def get_task_id(self):
        return self.task_id

    def get_task_index(self):
        return self.task_index

    def get_parallelism(self):
        return self.parallelism

    def get_config(self):
        return self.config

    def get_job_config(self):
        return self.job_config
