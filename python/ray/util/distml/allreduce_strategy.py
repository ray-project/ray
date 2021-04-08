import ray
from ray.util.distml.base_trainer import BaseTrainer
import ray.util.collective as col

import cupy as cp
import numpy as np

tqdm = None
try:
    from tqdm import tqdm
except ImportError:
    pass

# TODO(Hao): could make an interface class and use factory pattern to
#            set up strategies/trainers.
class AllReduceStrategy(BaseTrainer):
    def __init__(self, *args, fusion=False, use_tqdm=True, **kwargs):
        self._fusion = fusion
        self._use_tqdm = use_tqdm
        super(AllReduceStrategy, self).__init__(*args, **kwargs)

    def _init_strategy(self):
        """Do initialization for the distributed strategy."""
        pass

    def train(self, *, max_retries=1, info={}):
        if self._use_tqdm:
            desc = ""
            if "epoch_idx" in info.keys():
                if "num_epochs" in info.keys():
                    desc = f"Epoch {info['epoch_idx'] + 1}/{info['num_epochs']} "
                else:
                    desc = f"Epoch {info['epoch_idx'] + 1} "

            total = self.data_parallel_group.get_train_loader_len()
            _progress_bar = tqdm(
                total=total, desc=desc, unit="batch", leave=False)
            postfix = {}
            if "val_acc" in info.keys():
                postfix.update(val_acc=info["val_acc"])

        self.data_parallel_group.start_iteration()
        for idx in range(self.data_parallel_group.get_train_loader_len()):
            metrics = self.data_parallel_group.train_batch()
            if self._use_tqdm:
                _progress_bar.n = idx + 1
                if "train_loss" in metrics:
                    postfix.update(loss=metrics["train_loss"])
                _progress_bar.set_postfix(postfix)
        return info

    # def train_batch(self, batches):
    #     # First, let each worker proceed with a train_step
    #     # self.data_parallel_group.train()
    #     rets = [replica.train_batch.remote(batches[i])
    #             for i, replica in enumerate(self.data_parallel_group.replicas)]
    #     ray.get(rets)

    def validate(self, *info):
        stats = self.data_parallel_group.validate(info=info)
        return stats[0] # validate result should be the same in all workers

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
            strategy="allreduce",
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

    def shutdown(self, force=False):
        self.data_parallel_group.shutdown(force=force)

    def save_parameters(self, checkpoint):
        self.data_parallel_group.save_parameters(checkpoint)

    def load_parameters(self, checkpoint):
        self.data_parallel_group.load_parameters(checkpoint)


class Replica:
    """Express the training semantics of data-parallel replica.

    This class includes some glue code between the user-provided opterator
    and Ray cluster/collective-group setup.
    """
    def __init__(self,
                 training_operator_cls, operator_config):
        self.training_operator_cls = training_operator_cls
        self.operator_config = operator_config

        if "use_tqdm" in operator_config.keys():
            self._use_tqdm = operator_config["use_tqdm"]
        else:
            self._use_tqdm = False

        if tqdm is None and self._use_tqdm:
            raise ValueError("tqdm must be installed to use tqdm in training.")

        # collective-related information
        self.group_size = None
        self.rank = None
        self.group_name = None

    def setup_operator(self):
        # figure out the signature of training_operator_cls later.
        self.training_operator = self.training_operator_cls(self.operator_config)

    def setup_collective_group(self, rank, world_size, backend, group_name="default"):
        self.rank = rank
        self.group_name = group_name
        self.group_size = world_size
        col.init_collective_group(world_size, rank,
                                  backend=backend, group_name=group_name)
        return

    def start_iteration(self):
        self.iterator = iter(self.training_operator.train_loader)
    
    def get_train_loader_len(self):
        return len(self.training_operator.train_loader)

    def train_batch(self):
        metrics = {}
        try:
            batch = next(self.iterator)
        except StopIteration and NameError:
            raise RuntimeError(
                "iterator has ran out. Please use `start_iteration` to update iterator")
        
        loss_val, updates = self.derive_updates(batch)
        metrics["train_loss"] = loss_val

        for g in updates:
            cg = self.training_operator.to_cupy(g)
            col.allreduce(cg)
        self.apply_updates(updates)
        return metrics

    def derive_updates(self, batch):
        # TODO (Hao): handling data loader next.
        # TODO (Hao): change it to derive_update and apply_update.
        return self.training_operator.derive_updates(batch)

    def apply_updates(self, updates):
        self.training_operator.apply_updates(updates, self.group_size)

    def updates_transform(self, updates):
        return self.training_operator.updates_transform(updates)

    def validate(self, info={}):
        return self.training_operator.validate(info)

    def shutdown(self):
        # destroy the collective group resources on this process
        col.destroy_collective_group(self.group_name)
        if self.training_operator:
            del self.training_operator
        return 1

    def save_parameters(self, checkpoint):
        self.training_operator.save_parameters(checkpoint)

    def load_parameters(self, checkpoint):
        self.training_operator.load_parameters(checkpoint)


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

    def start_iteration(self):
        rets = [replica.start_iteration.remote() 
                for _, replica in enumerate(self.replicas)]

    def get_train_loader_len(self):
        lens =  ray.get([replica.get_train_loader_len.remote() 
                         for replica in self.replicas])

        if len(set(lens)) != 1:
            raise RuntimeError("All actors should have the same dataloader len.")

        return lens[0]

    def train_batch(self):
        metrics = {}
        loss_vals = ray.get([replica.train_batch.remote()
                             for _, replica in enumerate(self.replicas)])
        train_loss_list = [d["train_loss"] for d in loss_vals]
        metrics["train_loss"] = np.mean(train_loss_list)

        return metrics

    def validate(self, info={}):
        rets = [replica.validate.remote(info=info)
                for _, replica in enumerate(self.replicas)]
        stats = ray.get(rets)
        return stats

    def shutdown(self, force=False):
        rets = [replica.shutdown.remote()
                for _, replica in enumerate(self.replicas)]
        stats = ray.get(rets)
        return stats

    def reset(self):
        pass

    @property
    def replicas(self):
        return self._distributed_replicas

    def save_parameters(self, checkpoint):
        rets = [self.replicas[0].save_parameters.remote(checkpoint)]
        ray.get(rets)

    def load_parameters(self, checkpoint):
        rets = [replica.load_parameters.remote(checkpoint)
                for _, replica in enumerate(self.replicas)]
        ray.get(rets)

    def set_parameters(self, params):
        rets = [replica.set_parameters.remote(params)
                for _, replica in enumerate(self.replicas)]
        ray.get(rets)
        
    def get_parameters(self, cpu=False):
        ret = self.replicas[0].get_parameters.remote(cpu)
        return ray.get(ret)[0]

    def get_named_parameters(self, cpu=False):
        ret = self.replicas[0].get_named_parameters.remote(cpu)
        return ray.get([ret])[0]