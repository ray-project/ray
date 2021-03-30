import ray
from ray.util.distml.base_trainer import BaseTrainer
import ray.util.collective as col


# TODO(Hao): could make an interface class and use factory pattern to
#            set up strategies/trainers.
class AllReduceStrategy(BaseTrainer):
    def __init__(self, *args, fusion=False, **kwargs):
        self._fusion = fusion
        super(AllReduceStrategy, self).__init__(*args, **kwargs)

    def _init_strategy(self):
        """Do initialization for the distributed strategy."""
        pass

    def train(self, *, max_retries=1, info=None):
        # train one epoch
        rets = [replica.train.remote()
                for _, replica in enumerate(self.data_parallel_group.replicas)]
        ray.get(rets)

    def train_batch(self, batches):
        # First, let each worker proceed with a train_step
        # self.data_parallel_group.train()
        rets = [replica.train_batch.remote(batches[i])
                for i, replica in enumerate(self.data_parallel_group.replicas)]
        ray.get(rets)

    def _start_workers(self, num_workers):
        """Create worker(actor), maybe need worker group to manager these workers.
           Or, send these workers to strategy to manager?

           set workers or worker group
           set worker info, record rank, backend, use_num_gpus?
        """
        # TODO (Hao): infer the per-replica batch size here...

        # so here we get two set of params that will be passed around:
        # (1) Those for setting up training logic in training_operator, including:
        # batchsize, use_tqdm, user defined operator_config.
        operator_config = self._operator_config.copy()
        params = dict(
            training_operator_cls = self.training_operator_cls,
            operator_config = operator_config
        )
        # (2) params for setting up collective group and the strategy-related things;
        # For now, we do not have many of them though.
        dist_params = dict(
            group_name="default",
        )
        # (3) other arguments that used to init the DataParallelGrup
        group_init_args = {
            "params": params,
            "dist_params": dist_params,
            "num_cpus_per_worker": self.num_cpus_per_worker,
            "num_gpus_per_worker": self.num_gpus_per_worker,
        }

        self.data_parallel_group = DataParallelGroup(**group_init_args)

        # Once the group is created, we start it.
        self.data_parallel_group.start_replicas(num_workers)


class Replica:
    """Express the training semantics of data-parallel replica.

    This class includes some glue code between the user-provided opterator
    and Ray cluster/collective-group setup.
    """
    def __init__(self,
                 training_operator_cls, operator_config):
        self.training_operator_cls = training_operator_cls
        self.operator_config = operator_config
        # collective-related information
        self.group_size = None
        self.rank = None
        self.group_name = None

    def setup_operator(self):
        # figure out the signature of training_operator_cls later.
        self.training_operator = self.training_operator_cls(self.operator_config)

    def setup_collective_group(self, rank, world_size, backend):
        self.rank = rank
        self.group_name = "123"
        col.init_collective_group(world_size, rank,
                                  backend=backend, group_name=self.group_name)
        return

    def train(self):
        for batch in self.training_operator.yield_train_loader():
            self.train_batch(batch)

    def train_batch(self, batch):
        updates = self.derive_updates(batch)
        # TODO: make the signature correct
        # updates is a list.

        ## maybe need a transform for updates to make a list.
        # update_transform = lambda x: x 
        for g in self.updates_transform(updates):
            col.allreduce(updates)
        self.apply_updates(updates)
        print("yeah!!!")

    def derive_updates(self, batch):
        # TODO (Hao): handling data loader next.
        # TODO (Hao): change it to derive_update and apply_update.
        self.training_operator.derive_updates(batch)

    def apply_updates(self, updates):
        self.training_operator.apply_updates(updates)

    def updates_transform(self, updates):
        return self.training_operator.updates_transform(updates)

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
                 num_cpus_per_worker,
                 num_gpus_per_worker):
        self._params = params
        self._dist_params = dist_params
        self._num_cpus_per_worker = num_cpus_per_worker
        self._num_gpus_per_worker = num_gpus_per_worker

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
        RemoteReplica = ray.remote(num_cpus=self._num_cpus_per_worker,
                                   num_gpus=self._num_gpus_per_worker)(Replica)

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
