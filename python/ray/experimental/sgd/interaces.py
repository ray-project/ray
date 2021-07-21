from dataclasses import dataclass

from typing import Callable


@dataclass
class WorkerArgs:
    num_workers: int
    num_cpus_per_worker: int
    num_gpus_per_worker: int
    placement_strategy: str # Assumes 1 bundle per worker.

    elastic_training: bool = False # If True, training will continue with fewer
    # workers if an actor fails. Also, if more resources are available,
    # training will continue with more workers.
    max_actor_restarts: int = 0 # Number of retries when Ray actor fails.
    # Doesn't do anything if elastic_training is True. Set to -1 for
    # unlimited restarts.
    max_failed_actors: int = 0 # If `elastic_training` is True, this
            # specifies the maximum number of failed actors with which
            # we still continue training.

class BackendConfig:
    pass

@dataclass
class TorchConfig(BackendConfig):
    address: str # The connection address to use for the process group.
    process_group_timeout: int # The timeout for process group creation.

@dataclass
class TFConfig(BackendConfig):
    pass


def setup_torch_process_group(...):
    pass

def shutdown_torch(...):
    pass

class TorchExecutor:
    def __init__(self, worker_args: WorkerArgs, backend_config: TorchConfig,
                 initialization_hook):
        self.actor_group = ActorGroup(num_workers=worker_args.num_workers,
                                  cpus_per_worker=worker_args.num_cpus_per_worker,
                                  gpus_per_worker=worker_args.num_gpus_per_worker,
                                  pg_strategy=worker_args.placement_strategy)
        self.backend_config = backend_config
        self.initialization_hook = initialization_hook

    def start(self):
        self.actor_group.start()

        self.actor_group.execute(self.initialization_hook)

        futures = []
        for i in range(len(self.actor_group.workers)):
            futures.append(self.actor_group.execute_single_async(i,
                                                  setup_torch_process_group,
                                            kwargs={
                "address": self.backend_config.address,
                "process_group_timeout": self.backend_config.process_group_timeout,
                "world_size": len(self.actor_group.workers),
                "rank": i
            }))

        ray.get(futures)

    def run(self, training_fn, config):
        """Runs the training function"""

        # Run the training function asynchronously on all workers.
        not_ready = self.actor_group.execute_async(training_fn,
                                                     args=[config])

        # While the training function has not finished on all the workers,
        # Get results and yield.
        while not_ready:
            try:
                results = self.actor_group.execute(self.fetch_next)
            except RayActorError:
                # An actor failed.
                # handle fault tolerance and elastic training here.


            yield results
            ready, not_ready = ray.wait(not_ready, timeout=0)

    def shutdown(self):
        self.actor_group.execute(shutodwn_torch)
        self.actor_group.shutdown()

class Trainer:
    def __init__(self, worker_args: WorkerArgs, backend: str, backend_config:
    BackendConfig, initialization_hook: Callable, callbacks: List[
        "sgd.Callback"]):

        if backend == "torch":
            self.executor = TorchExecutor(worker_args, backend_config,
                                          initialization_hook, callbacks)

    def run(self, training_fn, config):
        self.executor.start()
        result_generator = self.run_generator(training_fn, config)
        for results in result_generator:
            for callback in self.callbacks:
                callback.handle_results(results)
        self.executor.shutdown()


    def run_generator(self, training_fn, config):
        result_generator = self.executor.run(training_fn, config)
        yield from result_generator


    # Option 1: Read from sgd queue, call tune.report and send to tune queue.
    def to_trainable(self, training_func):
        def tune_training_func(config):
            training_func(config) # This calls sgd.report. sgd.report sends
            # results to the queue in a separate thread. we read from that
            # queue and call tune.report.
            tune.report(fetch_next())
        return tune_training_func

    # Option 2: Don't even use sgd queue, directly send to tune's queue.
    def to_trainable(self, training_func):
        # Everything is handled inside sgd.report
        pass





