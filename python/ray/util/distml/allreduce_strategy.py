import ray
from ray.util.distml.base_trainer import BaseTrainer
import ray.util.collective as col


# TODO(Hao): could make an interface class and use factory pattern to
#            set up strategies/trainers.
class AllReduceStrategy(BaseTrainer):
    def __init__(self, *args, **kwargs):
        super(AllReduceStrategy, self).__init__(*args, **kwargs)


    def _init_strategy(self):
        pass

    def _start_workers(self,
                       training_operator_cls,
                       num_cpus_per_worker,
                       num_gpus_per_worker,
                       world_size,
                       backend):
        params = dict()
        dist_params = dict()
        self.data_parallel_group = DataParallelGroup(params, dist_params, 1, 1)
        self.data_parallel_group.start_replicas(world_size)

    def train_batch(self, batches):
        # First, let each worker proceed with a train_step
        # self.data_parallel_group.train()
        rets = [replica.train_step.remote(batches[i])
                for i, replica in enumerate(self.data_parallel_group.replicas)]
        ray.get(rets)


class Replica:
    """Express the training semantics of data-parallel replica.

    This class includes some glue code between the user-provided opterator
    and Ray cluster/collective-group setup.
    """
    def __init__(self,
                 training_operator_cls):
        self.training_operator_cls = training_operator_cls

        # collective-related information
        self.group_size = None
        self.rank = None
        self.group_name = None

    def setup_operator(self):
        # figure out the signature of training_operator_cls later.
        self.training_operator = self.training_operator_cls()

    def setup_collective_group(self, rank, world_size, backend):
        self.rank = rank
        self.group_name = "123"
        col.init_collective_group(world_size, rank,
                                  backend=backend, group_name=self.group_name)
        return

    def train_step(self, batch):
        # TODO (Hao): handling data loader next.
        # TODO (Hao): change it to derive_update and apply_update.
        self.training_operator.train_step(batch)

    def shutdown(self):
        # destroy the collective group resources on this process
        col.destroy_collective_group(self.group_name)
        if not self.training_operator:
            del self.training_operator


class DataParallelGroup:
    """Spawn a group a replicas for data-parallel training."""
    def __init__(self,
                 params,
                 dist_params,
                 num_cpus_per_replica=1,
                 num_gpus_per_replica=1):
        self._params = params
        self._dist_params = dist_params
        self._num_cpus_per_replica = num_cpus_per_replica
        self._num_gpus_per_replica = num_gpus_per_replica
        self._distributed_replicas = None

    def _setup_collective_group(self, world_size):
        rets = [replica.setup_collective_group.remote(rank=i, world_size=world_size, backend="nccl")
                for i, replica in enumerate(self._distributed_replicas)]
        return rets

    def _setup_operator(self):
        setups = [replica.setup_operator.remote()
                for i, replica in enumerate(self._distributed_replicas)]
        return setups

    def start_replicas(self, num_replicas):
        assert num_replicas > 1

        # make an actor
        RemoteReplica = ray.remote(num_cpus=self._num_cpus_per_replica,
                                   num_gpus=self._num_gpus_per_replica)(Replica)

        self._distributed_replicas = [
            RemoteReplica.remote(**self._params)
            for _ in range(num_replicas)
        ]

        # setup the rank and group in each replica
        ray.get(self._setup_collective_group(
            len(self._distributed_replicas)))

        # setup the model training operator
        ray.get(self._setup_operator())

    def shutdown(self, force=False):
        pass

    def reset(self):
        pass

    @property
    def replicas(self):
        return self._distributed_replicas
