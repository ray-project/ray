import inspect

from ray.serve.constants import ASYNC_CONCURRENCY


def _callable_accepts_batch(func_or_class):
    if inspect.isfunction(func_or_class):
        return hasattr(func_or_class, "_serve_accept_batch")
    elif inspect.isclass(func_or_class):
        return hasattr(func_or_class.__call__, "_serve_accept_batch")


def _callable_is_blocking(func_or_class):
    if inspect.isfunction(func_or_class):
        return not inspect.iscoroutinefunction(func_or_class)
    elif inspect.isclass(func_or_class):
        return not inspect.iscoroutinefunction(func_or_class.__call__)


class BackendConfig:
    def __init__(self, config_dict, accepts_batches=False, is_blocking=True):
        assert isinstance(config_dict, dict)
        # Make a copy so that we don't modify the input dict.
        config_dict = config_dict.copy()

        self.accepts_batches = accepts_batches
        self.is_blocking = is_blocking
        self.num_replicas = config_dict.pop("num_replicas", 1)
        self.max_batch_size = config_dict.pop("max_batch_size", None)
        self.batch_wait_timeout = config_dict.pop("batch_wait_timeout", 0)
        self.max_concurrent_queries = config_dict.pop("max_concurrent_queries",
                                                      None)

        if self.max_concurrent_queries is None:
            # Model serving mode: if the servable is blocking and the wait
            # timeout is default zero seconds, then we keep the existing
            # behavior to allow at most max batch size queries.
            if self.is_blocking and self.batch_wait_timeout == 0:
                if self.max_batch_size:
                    self.max_concurrent_queries = 2 * self.max_batch_size
                else:
                    self.max_concurrent_queries = 8

            # Pipeline/async mode: if the servable is not blocking,
            # router should just keep pushing queries to the worker
            # replicas until a high limit.
            if not self.is_blocking:
                self.max_concurrent_queries = ASYNC_CONCURRENCY

            # Batch inference mode: user specifies non zero timeout to wait for
            # full batch. We will use 2*max_batch_size to perform double
            # buffering to keep the replica busy.
            if self.max_batch_size is not None and self.batch_wait_timeout > 0:
                self.max_concurrent_queries = 2 * self.max_batch_size

        if len(config_dict) != 0:
            raise ValueError("Unknown options in backend config: {}".format(
                list(config_dict.keys())))

        self._validate()

    def update(self, config_dict):
        """Updates this BackendConfig with options set in the passed config.

        Unspecified keys will remain the same.
        """
        if "num_replicas" in config_dict:
            self.num_replicas = config_dict.pop("num_replicas")
        if "max_batch_size" in config_dict:
            self.max_batch_size = config_dict.pop("max_batch_size")
        if "max_concurrent_queries" in config_dict:
            self.max_concurrent_queries = config_dict.pop(
                "max_concurrent_queries")

        if len(config_dict) != 0:
            raise ValueError("Unknown options in backend config: {}".format(
                list(config_dict.keys())))

        self._validate()

    def _validate(self):
        if not isinstance(self.num_replicas, int):
            raise TypeError("num_replicas must be an int.")
        elif self.num_replicas < 1:
            raise ValueError("num_replicas must be >= 1.")

        if self.max_batch_size is not None:
            if not isinstance(self.max_batch_size, int):
                raise TypeError("max_batch_size must be an integer.")
            elif self.max_batch_size < 1:
                raise ValueError("max_batch_size must be >= 1.")

            if not self.accepts_batches and self.max_batch_size > 1:
                raise ValueError(
                    "max_batch_size is set in config but the function or "
                    "method does not accept batching. Please use "
                    "@serve.accept_batch to explicitly mark the function or "
                    "method as batchable and takes in list as arguments.")


class ReplicaConfig:
    def __init__(self, func_or_class, *actor_init_args,
                 ray_actor_options=None):
        self.func_or_class = func_or_class
        self.accepts_batches = _callable_accepts_batch(func_or_class)
        self.is_blocking = _callable_is_blocking(func_or_class)
        self.actor_init_args = list(actor_init_args)
        if ray_actor_options is None:
            self.ray_actor_options = {}
        else:
            self.ray_actor_options = ray_actor_options

        self.resource_dict = {}
        self._validate()

    def _validate(self):
        # Validate that func_or_class is a function or class.
        if inspect.isfunction(self.func_or_class):
            if len(self.actor_init_args) != 0:
                raise ValueError(
                    "actor_init_args not supported for function backend.")
        elif not inspect.isclass(self.func_or_class):
            raise TypeError(
                "Backend must be a function or class, it is {}.".format(
                    type(self.func_or_class)))

        if not isinstance(self.ray_actor_options, dict):
            raise TypeError("ray_actor_options must be a dictionary.")
        elif "detached" in self.ray_actor_options:
            raise ValueError(
                "Specifying detached in actor_init_args is not allowed.")
        elif "name" in self.ray_actor_options:
            raise ValueError(
                "Specifying name in actor_init_args is not allowed.")
        elif "max_restarts" in self.ray_actor_options:
            raise ValueError("Specifying max_restarts in "
                             "actor_init_args is not allowed.")
        else:
            num_cpus = self.ray_actor_options.get("num_cpus", 0)
            if not isinstance(num_cpus, (int, float)):
                raise TypeError(
                    "num_cpus in ray_actor_options must be an int or a float.")
            elif num_cpus < 0:
                raise ValueError("num_cpus in ray_actor_options must be >= 0.")
            self.resource_dict["CPU"] = num_cpus

            num_gpus = self.ray_actor_options.get("num_gpus", 0)
            if not isinstance(num_gpus, (int, float)):
                raise TypeError(
                    "num_gpus in ray_actor_options must be an int or a float.")
            elif num_gpus < 0:
                raise ValueError("num_gpus in ray_actor_options must be >= 0.")
            self.resource_dict["GPU"] = num_gpus

            memory = self.ray_actor_options.get("memory", 0)
            if not isinstance(memory, (int, float)):
                raise TypeError(
                    "memory in ray_actor_options must be an int or a float.")
            elif memory < 0:
                raise ValueError("num_gpus in ray_actor_options must be >= 0.")
            self.resource_dict["memory"] = memory

            object_store_memory = self.ray_actor_options.get(
                "object_store_memory", 0)
            if not isinstance(object_store_memory, (int, float)):
                raise TypeError(
                    "object_store_memory in ray_actor_options must be "
                    "an int or a float.")
            elif object_store_memory < 0:
                raise ValueError(
                    "object_store_memory in ray_actor_options must be >= 0.")
            self.resource_dict["object_store_memory"] = object_store_memory

            custom_resources = self.ray_actor_options.get("resources", {})
            if not isinstance(custom_resources, dict):
                raise TypeError(
                    "resources in ray_actor_options must be a dictionary.")
            self.resource_dict.update(custom_resources)
