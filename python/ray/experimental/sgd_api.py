# Underlying Actor Group API

# Basically what Horovod-ray is doing right now.

class BaseWorker:
    executable = None

    def set_env_var(self, key: str, value: str):
        """Set an environment variable with the provided values."""
        if value is not None:
            value = str(value)
            os.environ[key] = value

    def set_env_vars(self, keys: List[str], values: List[str]):
        """Sets multiple env vars with the provided values"""
        assert len(keys) == len(values)
        for key, value in zip(keys, values):
            self.set_env_var(key, value)

    def get_node_ip(self):
        """Returns the IP address of the node that this Ray actor is on."""
        return ray.services.get_node_ip_address()

    def start_cls(self, actor_cls, *init_args, **init_kwargs):
        self.cls = actor_cls(*init_args, **init_kwargs)

    def execute(self, fn: Callable):
        """Execute the provided function and return the result."""
        return fn(self.cls)

# This is similar to _ExecutorDriver in Horovod-ray
class ActorGroup:
    """Base driver for executing Ray calls."""

    def __init__(self,
                 num_workers: Optional[int] = None,
                 cpus_per_worker: int = 1,
                 gpus_per_worker: Optional[int] = None,
                 placement_group: Union[str, PlacementGroup]):

        self.workers = []

        self.pg_strategy = placement_group
        ...

    def _start_executables(self, actor_cls, *init_args, **init_kwargs):
        """
        Starts the actors with the given class.
        """
        ray.get(w.start_executable.remote(actor_cls, *init_args,
                                          **init_kwargs) for w in self.workers)

    def _create_placement_group(self, pg_strategy):
        ...

    def _start_workers(self, pg):
        self.workers = [ray.remote(BaseWorker).options(...)remote() for _ in\
            range(self.num_workers)]

    def start(self,
              actor_cls: type = None,
              *init_args: Optional[List] = None,
              **init_kwargs: Optional[Dict] = None):

        if isinstance(self.pg, PlacementGroup):
            self.pg = self.pg_strategy
        else:
            self.pg = self._create_placement_group(self.pg_strategy)

        self._start_workers(self.pg)

        self._start_executables(actor_cls, *init_args,
                                **init_kwargs)

    def execute(self, fn: Callable[["executable_cls"], Any]) -> List[Any]:
        """Executes the provided function on all workers.
        Args:
            fn: Target function to be invoked on every object.
        Returns:
            Deserialized return values from the target function.
        """
        return ray.get([worker.execute.remote(fn) for worker in self.workers])

    def run(self,
            fn: Callable[[Any], Any],
            *args: Optional[List] = None,
            **kwargs: Optional[Dict] = None) -> List[Any]:
        """Executes the provided function on all workers.
        Args:
            fn: Target function that can be executed with arbitrary
                args and keyword arguments.
            args: List of arguments to be passed into the target function.
            kwargs: Dictionary of keyword arguments to be
                passed into the target function.
        Returns:
            Deserialized return values from the target function.
        """
        return ray.get(self.run_remote(fn, args, kwargs))

    def run_remote(self,
                   fn: Callable[[Any], Any],
                   *args: Optional[List] = None,
                   **kwargs: Optional[Dict] = None) -> List[Any]:
        """Executes the provided function on all workers.
        Args:
            fn: Target function that can be executed with arbitrary
                args and keyword arguments.
            args: List of arguments to be passed into the target function.
            kwargs: Dictionary of keyword arguments to be
                passed into the target function.
        Returns:
            list: List of ObjectRefs that you can run `ray.get` on to
                retrieve values.
        """
        return [
            worker.execute.remote(lambda w: fn(args, kwargs))
            for worker in self.workers
        ]

    def execute_single(self,
                       fn: Callable[["executable_cls"], Any]) -> List[Any]:
        """Executes the provided function on the rank 0 worker (chief).
        Args:
            fn: Target function to be invoked on the chief object.
        Returns:
            Deserialized return values from the target function.
        """
        return ray.get(self.workers[0].execute.remote(fn))

    def set_env_vars(env_vars: Dict):
        ray.get(w.set_env_vars.remote(env_vars) for w in self.workers]


    def shutdown(self):
        """Destroys the provided workers."""
        for worker in self.workers:
            del worker

        remove_placement_group(self.pg)


# usage
class Foo:
    def __init__(self, counter):
        self.counter = counter

    def increment(self):
        self.counter += 1

actor_group = ActorGroup(num_workers=2)
actor_group.start(Foo, counter=0)
for _ in range(5):
    actor_group.run(lambda w: w.increment())

assert actor_group.run(lambda w: w.counter) == [5, 5]


# SGD Backends.

# Framework specific actor groups. Allows you to do framework specific
# implementations

# Torch: Setup process groups
# Horovod: Create Coordinator, NIC detection, ...

class TorchActorGroup(ActorGroup):

    def start(self, executable_cls, ...):
        super().start(executable_cls, ...)
        self.run(lambda _: setup_torch_process_group())

class TFActorGroup(TrainingGroup):
    ...

# HorovodExecutor can live in the Horovod repo, replacing the current
# abstractions.
# We will still maintain backwards compatibility.
class HorovodActorGroup(TrainingGroup):
    ...

# These actor groups can be used directly.

# Start the Ray cluster or attach to an existing Ray cluster
ray.init()

# Start num_workers actors on the cluster
executor = HorovodActorGroup(
    setting, num_workers=num_workers, use_gpu=True)

# This will launch `num_workers` actors on the Ray Cluster.
executor.start()

# Using the stateless `run` method, a function can take in any args or kwargs
def simple_fn():
    hvd.init()
    print("hvd rank", hvd.rank())
    return hvd.rank()

# Execute the function on all workers at once
result = executor.run(simple_fn)
# Check that the rank of all workers is unique
assert len(set(result)) == hosts * num_slots

executor.shutdown()

# Or you can use a higher level API.
# Framework-agnostic Trainer

# 2 APIs

# 1. Current `TrainingOperator` class API

# 2. function API

class Trainer:
    def __init__(self, train_fn_or_cls, num_workers,
                 num_cpus_per_worker, num_gpus_per_worker, backend):
        """
        train_fn_or_cls: Either a training function or a class of
            type `TrainingOperator`
        num_workers: Either an integer of num_workers or "auto" to
            create as many workers as resources available
        num_cpus_per_worker: Number of CPUs per worker
        num_gpus_per_worker: Number of GPUs per worker
        backend: torch, tensorflow, horovod, distml
        config: If using training function, config will get passed directly to
        the training function. If using TrainingOperator class, then it
        doesn't do anything.
        """
        if backend == "horovod":
            self.actor_group = HorovodActorGroup()
        ...
        self.train_fn_or_cls = train_fn_or_cls
        self.actor_group.start()
        if not callable(self.train_fn_or_cls):
            self.actor_group.start(self.train_fn_or_cls)
        else:
            # Stateless if using function API
            self.actor_group.start(None)

    def init_operator(self):
        """Initialization for class API usage."""
        pass

    # For function API it is a single run call
    def run(self):
        if callable(self.train_fn_or_cls):
            self.actor_group.run(lambda w: train_fn(self.config))

    # For class API, it's similar to our current TorchTrainer

    def train_epoch(self):
        if not callable(self.train_fn_or_cls):
            return self.actor_group.run(lambda w: w.train_epoch())

    def validate(self):
        if not callable(self.train_fn_or_cls):
            return self.actor_group.run(lambda w: w.validate())

    def state_dict(self):
        if not callable(self.train_fn_or_cls):
            return self.actor_group.run(lambda w: w.state_dict())

    def save_checkpoint(self):
        if not callable(self.train_fn_or_cls):
            return self.actor_group.run(lambda w: w.save_checkpoint())

# Function API usage
def train_func(config, world_rank, local_rank, device_id ):
    # Either
    state = ray.sgd.load_checkpoint()
    model, optimizer, scheduler, train_loader, val_loader = state.model, \
        state.optimizer, state.scheduler, state.train_loader, state.val_loader
    # Or
    model = Net()
    optimizer = torch.sgd()
    ...

    # These should work.
    ray.sgd.local_rank()
    ray.sgd.world_rank()

    # User sets up mixed precision here.

    model.cuda()
    DistributedDataParallel(...)

    # Potentially add DistributedSampler here
    train_loader.sampler = DistributedSampler(...)


    # eventually, optional: ray.sgd.accelerate(model, …)
    for _ in config["num_epochs"]:
        train(...)
        # optional, eventually: ray.sgd.report(...)
        # at some frequency, ray.sgd.save_checkpoint(state, files=False|True)
        validate(...)
    return model

trainer = Trainer(train_fn, num_workers=“auto”, backend=...)
result = Trainer.run(train_func, callbacks=[MLflowLogger(), ...],
                     num_epochs=10)

# Class API usage. Same as our current TorchTrainer.
trainer = Trainer(training_operator_cls, num_workers=“auto”, backend=...)
trainer.init_operator(use_ddp=True, use_fp16=False, use_dist_sampler=...)
for _ in range(5):
    trainer.train_epoch()
    trainer.validate()

# # Another alternative
# session = Trainer.run_and_report(
#     train_func | training_cls | training_operator | ptl_module)
# while session.is_running():
#     next_result = session.fetch_result(...)