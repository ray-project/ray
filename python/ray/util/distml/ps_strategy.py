import ray
from ray.util.distml.base_trainer import BaseTrainer
import ray.util.collective as col

import cupy as cp
import numpy as np
import ray.util.distml.util as util
from ray.util.distml.util import ThroughoutCollection, func_timer

# TODO(HUI): use group start and end in send/recv
from cupy.cuda.nccl import groupStart, groupEnd

tqdm = None
try:
    from tqdm import tqdm
except ImportError:
    pass

import logging

class ParameterServerStrategy(BaseTrainer):
    def __init__(self, *args, fusion=False, num_workers=1, num_ps=1, use_tqdm=True, max_iteration=None, record_config=None, **kwargs):
        self.num_ps = num_ps 
        self.num_workers = num_workers
        self._fusion = fusion
        self._max_iteration = max_iteration
        self._use_record = True if record_config else False

        self.assignments = None

        self.num_cpus_per_server = 1 + 1
        self.num_gpus_per_server = 1

        self._use_tqdm = use_tqdm

        super(ParameterServerStrategy, self).__init__(*args, **kwargs)

        self._collector = ThroughoutCollection(**record_config)
        if not self._use_record:
            self._collector.disable()

    def _init_strategy(self):
        """Do initialization for the distributed strategy."""
        # All sync with worker 0
        init_weights_id = self.worker_group.get_named_parameters(cpu=True)
    
        self._round_robin_sharding()

        # set assignments to every worker
        self.worker_group.set_assignments(self.assignments)

        # all workers get synced
        for i, worker in enumerate(self.worker_group.actors):
            if i != 0:
                ray.get([worker.set_parameters.remote(init_weights_id)])

        # now spawn parameter server actors
        shard_ids = self.worker_group.split_parameters(self.assignments)

        # TODO(HUI): use scatter to send parameters
        for server_idx, server in enumerate(self.server_group.actors):
            this_shard_ref = self.worker_group.actors[0].index_shard.remote(shard_ids, server_idx)
            ray.get([server.set_params.remote(this_shard_ref)])

    def _start_workers(self, world_size):
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
        dist_params_worker = dict(
            strategy="ps",
            is_server=False,
            group_name="default",
            num_ps=self.num_ps,
            num_workers=self.num_workers,
        )

        dist_params_server = dict(
            strategy="ps",
            is_server=True,
            group_name="default",
            num_ps=self.num_ps,
            num_workers=self.num_workers,
        )

        # (3) other arguments that used to init the DataParallelGrup
        workergroup_init_args = {
            "params": params,
            "dist_params": dist_params_worker,
            "num_cpus_per_actor": self.num_cpus_per_worker,
            "num_gpus_per_actor": self.num_gpus_per_worker,
        }

        servergroup_init_args = {
            "params": params,
            "dist_params": dist_params_server,
            "num_cpus_per_actor": self.num_cpus_per_server,
            "num_gpus_per_actor": self.num_gpus_per_server,
        }

        # Should we make two groups for worker and server?
        self.worker_group = DataParallelGroup(**workergroup_init_args)
        self.server_group = DataParallelGroup(**servergroup_init_args)

        # Once the group is created, we start it.
        self.server_group.start_actors(self.num_ps)  # server at the last num_ps processes.
        self.worker_group.start_actors(self.num_workers)

        worker_rets = self.worker_group.test_connection()
        server_rets = self.server_group.test_connection()
        ray.get(worker_rets+server_rets)
        ray.get(self.worker_group.setup_operator())
        ray.get(self.server_group.setup_operator())  
        
        self.server_group.clean_redundancy()

    def shutdown(self, force=False):
        self.worker_group.shutdown(force=force)
        self.server_group.shutdown(force=force)

    def save_parameters(self, checkpoint):
        # TODO(HUI): ps save parameters.
        # First, worker rank 0 should pull the latest parameter from servers
        # Then, worker rank 0 save parameters
        self.worker_group.save_parameters(checkpoint)

    def load_parameters(self, checkpoint):
        # TODO(HUI): ps load parameters.
        # load the needed parameter in all server.
        self.server_group.load_parameters(checkpoint)

    def _round_robin_sharding(self):
        """Generate the assignment of variable to servers."""
        parameter_distribution = ray.get(self.worker_group.actors[0].params_distribution.remote())
        assignments = [0 for _ in parameter_distribution]
        loads = [0 for _ in range(self.num_ps)]
        for i, var_size in enumerate(parameter_distribution):
            min_ps_index = loads.index(min(loads))
            loads[min_ps_index] += var_size
            assignments[i] = min_ps_index
        print("Load of each ps {}".format(loads))
        self.assignments = assignments

    def train(self, *, max_retries=1, info={}):
        total = self._max_iteration if self._max_iteration else self.worker_group.get_train_loader_len()

        if self._use_tqdm:
            desc = ""
            if "epoch_idx" in info.keys():
                if "num_epochs" in info.keys():
                    desc = f"Epoch {info['epoch_idx'] + 1}/{info['num_epochs']} "
                else:
                    desc = f"Epoch {info['epoch_idx'] + 1} "

            _progress_bar = tqdm(
                total=total, desc=desc, unit="batch", leave=False)
            postfix = {}
            if "val_acc" in info.keys():
                postfix.update(val_acc=info["val_acc"])

        # TODO(HUI): Record server rank instead of using num_ps.
        # train one epoch
        self.worker_group.start_iteration()
        for idx in range(total):
            with self._collector.record("train_batch"):
                metrics = self.step()
            if self._use_tqdm:
                _progress_bar.n = idx + 1
                if "train_loss" in metrics:
                    postfix.update(loss=metrics["train_loss"])
                _progress_bar.set_postfix(postfix)
        return info

    def validate(self, *, info={}):
        with self._collector.record("validate_epoch"):
            stats = self.worker_group.validate(info=info)
        self._collector.update("validate_epoch",val_acc=stats[0]["val_accuracy"])
        self._collector.save("validate_epoch")
        return stats[0] # validate result should be the same in all workers

    @func_timer
    def step(self):
        loss_vals = []
        rets = []
        metrics = {}

        for worker_idx, worker in enumerate(self.worker_group.actors):
            groupStart()
            for server_idx, server in enumerate(self.server_group.actors):
                # every server sends its shard to the worker
                server.send_params.remote(worker_idx)
            # the worker receives shards from ps, compute loss, gradients
            # and sends these gradients to every server
            worker.compute_recv_params.remote()
            groupEnd()

        for worker_idx, worker in enumerate(self.worker_group.actors):
            groupStart()
            loss_val = worker.compute_calculate_and_send.remote()
            for server in self.server_group.actors:
                rets.append(server.update.remote(worker_idx))
            groupEnd()
            loss_vals.append(loss_val)
 
        loss_vals = ray.get(loss_vals)
        ray.get(rets)
        train_loss_list = [d["train_loss"] for d in loss_vals]
        metrics["train_loss"] = np.mean(train_loss_list)
        return metrics

    # @func_timer
    # def step(self):
    #     loss_vals = []
    #     rets = []
    #     metrics = {}

    #     for worker_idx, worker in enumerate(self.worker_group.actors):
    #         for server_idx, server in enumerate(self.server_group.actors):
    #             # every server sends its shard to the worker
    #             server.send_params.remote(worker_idx)
    #         # the worker receives shards from ps, compute loss, gradients
    #         # and sends these gradients to every server
    #         loss_val = worker.compute.remote()
    #         loss_vals.append(loss_val)
    #     loss_vals = ray.get(loss_vals)

    #     for worker_idx, worker in enumerate(self.worker_group.actors):
    #         for server in self.server_group.actors:
    #             rets.append(server.update.remote(worker_idx))
                
    #     ray.get(rets)
    #     train_loss_list = [d["train_loss"] for d in loss_vals]
    #     metrics["train_loss"] = np.mean(train_loss_list)
    #     return metrics


class PS(object):  # HUI: In server, the saved params is operator tensor, not cupy
    def __init__(self, 
                 training_operator_cls, operator_config):
        # rank should be true rank. means, rank has already plus num_worker.
        self.training_operator_cls = training_operator_cls
        self.operator_config = operator_config

        self.grad_counts = None
        self.params = dict()

    def setup_operator(self):
        # figure out the signature of training_operator_cls later.
        self.training_operator = self.training_operator_cls(self.operator_config)

    def setup_collective_group(self, rank, num_ps, num_workers, backend="nccl", group_name="default"):
        self.rank = rank
        self.num_ps = num_ps
        self.num_workers = num_workers
        self.group_name = group_name
        self.group_size = num_ps + num_workers
        self._init_grad_counts()
        # the last num_ps processes are servers.
        col.init_collective_group(num_ps + num_workers, rank,
                                  backend=backend, group_name=group_name)

    def test_connection(self):
        # hand shake with server?
        for i in range(self.num_workers):
            recv = util.zeros((1,), cpu=False)
            col.recv(recv, i, self.group_name)
            assert recv==1
        for i in range(self.num_workers):
            send = util.ones((1,), cpu=False)
            col.send(send, i, self.group_name)
        return

    def _init_grad_counts(self):
        self.grad_counts = [0] * self.num_workers

    def _init_grad_buffer(self):
        # if hasattr(self, "grad_buffer"):
        #     self.grad_buffer = {k:v*0 for k, v in self.grad_buffer.items()}
        # else:
        self.grad_buffer = {k:self.training_operator.zeros_like(v, cpu=False) 
                            for k, v in self.params.items()}

    def get_params(self):
        return self.params

    def set_params(self, params):
        # params should in GPU when calling this function.
        for k, v in params.items():
            self.params[k] = self.training_operator.asarray(v)

        # param is a dict, if needed list, should convert in operator.
        self.training_operator.reset_optimizer_for_params(self.params)
        self._init_grad_buffer()

    def apply_updates(self, grad_buffer):
        self.training_operator.apply_updates(grad_buffer, self.num_workers)
        self.params = self.training_operator.get_named_parameters(cpu=False)

    def _inc_gradients(self, gradients):
        for name, p in self.get_params().items():
            if gradients[name] is not None:
                self.grad_buffer[name] += gradients[name]
                
    def send_params(self, dst_rank):
        """ Send this param shard to the destination worker """
        for name, v in self.params.items():
            cv = self.training_operator.to_cupy(v)
            col.send(cv, dst_rank, self.group_name)

    def update(self, src_rank):
        """Receive gradients and update"""
        keys = list(self.params.keys())
        grads = dict()
        recv_list = []
        
        for key in keys:
            to_recv = self.params[key]
            recv_list.append(self.training_operator.zeros(to_recv.shape, cpu=False))

        print(f"rank {self.rank} {len(keys)}")
        for i in range(len(keys)):
            v = self.training_operator.to_cupy(recv_list[i])
            print(f"rank {self.rank} {i} {v} {v.shape}")
            col.recv(v, src_rank, self.group_name)
            print(f"rank {self.rank} {i}")
        for i in range(len(keys)):
            grads[keys[i]] = recv_list[i]

        self._inc_gradients(grads)

        print(f"rank {self.rank} {grad_counts}")
        if not self.grad_counts[src_rank]:
            self.grad_counts[src_rank] = 1
        else:
            raise RuntimeError(f"This worker {src_rank} send gradients again.")
        if sum(self.grad_counts) == self.num_workers:
            self.apply_updates(self.grad_buffer)

            self._init_grad_buffer()
            self._init_grad_counts()
        return True

    def clean_redundancy(self):
        self.training_operator.clean_redundancy()

    def shutdown(self):
        # destroy the collective group resources on this process
        col.destroy_collective_group(self.group_name)
        if self.training_operator:
            del self.training_operator
        return 1

class Worker(object):
    def __init__(self, 
                 training_operator_cls, operator_config):
        self.training_operator_cls = training_operator_cls
        self.operator_config = operator_config
        # collective-related information
        self.group_size = None
        self.rank = None
        self.group_name = None
        self.assignments = None

    def setup_operator(self):
        # figure out the signature of training_operator_cls later.
        self.training_operator = self.training_operator_cls(self.operator_config)

    def setup_collective_group(self, rank, num_ps, num_workers, backend="nccl", group_name="default"):
        self.rank = rank
        self.num_ps = num_ps
        self.num_workers = num_workers
        self.group_name = group_name
        self.group_size = num_ps + num_workers
        self.name_list = [[] for i in range(num_ps)]

        # the last num_ps processes are servers.
        col.init_collective_group(num_ps+num_workers, rank,
                                  backend=backend, group_name=group_name)
    
    def test_connection(self):
        # hand shake with server?
        for i in range(self.num_ps):
            send = util.ones((1,), cpu=False)
            col.send(send, self.num_workers + i, self.group_name)
        for i in range(self.num_ps):
            recv = util.zeros((1,), cpu=False)
            col.recv(recv, self.num_workers + i, self.group_name)
            assert recv==1
        return

    def num_params(self):
        return len(self.get_parameters())

    def params_distribution(self):
        distribution = []
        weights = self.get_named_parameters(cpu=True)
        for k, v in weights.items():
            distribution.append(self.training_operator.numel(v))
        return distribution

    def get_data_loader(self):
        return self.training_operator.train_loader

    def start_iteration(self):
        self.iterator = iter(self.training_operator.train_loader)
    
    def get_train_loader_len(self):
        if hasattr(self.training_operator.train_loader, "__len__"):
            return len(self.training_operator.train_loader)
        else:
            raise RuntimeError("traning dataloader has no attributed `__len__`."
                               "Please set `max_iteration` in AllReduceStrategy.")

    def derive_updates(self, batch):
        # TODO (Hao): handling data loader next.
        # TODO (Hao): change it to derive_update and apply_update.
        return self.training_operator.derive_updates(batch)

    def compute_gradients(self, params):
        """
        update worker parameters that received from server. compute gradients and return named gradients.
        """
        self.set_parameters(params)

        try:
            batch = next(self.iterator)
        except StopIteration and NameError:
            self.iterator = iter(self.training_operator.train_loader)
            batch = next(self.iterator)

        # different from original core ps. 
        # Here derive_updates return loss_val and graident in order.
        loss_val, grads = self.training_operator.derive_updates(batch)
        assert isinstance(grads, dict)

        return loss_val, grads

    def split_gradients(self, grad, assignments):
        # assuming messages are gradients or parameters
        # this grad is ready to be called by apply_gradients in ParameterServer
        num_shards = np.unique(np.array(assignments)).size
        shards = [dict() for i in range(num_shards)]
        for i, (k, v) in enumerate(grad.items()):
            shards[assignments[i]][k] = v
        return shards

    def split_parameters(self, assignments):
        params = self.get_named_parameters(cpu=False)
        num_shards = np.unique(np.array(assignments)).size
        shards = [dict() for i in range(num_shards)]
        for i, (k, v) in enumerate(params.items()):
            shards[assignments[i]][k] = v
        return shards

    def index_shard(self, shards, index):
        return shards[index]

    def set_parameters(self, params):
        return self.training_operator.set_parameters(params)

    def get_parameters(self, cpu):
        # HUI: we might need to convert different tensor type to a unified one.
        # GPU use cupy, CPU use numpy.
        return self.training_operator.get_parameters(cpu)

    def get_named_parameters(self, cpu):
        return self.training_operator.get_named_parameters(cpu)

    def get_gradients(self):
        # training_operator call gradients or we save gradient in replica 
        # when derive_updates.
        return self.training_operator.get_gradients()

    def set_assignments(self, assignments):
        self.assignments = assignments
        keys = list(self.get_named_parameters(cpu=False).keys())
        for i, a in enumerate(self.assignments):
            self.name_list[a].append(keys[i])

    def compute_recv_params(self):
        """Returns the loss, and send gradients to servers"""
        
        # First receive params from servers
        weights = self.get_named_parameters(cpu=False)
        # create the receive lists to group collective calls
        recv_list = []
        for i in range(self.num_ps):
            recv_list.append([])
            param_shard_keys = self.name_list[i]
            for key in param_shard_keys:
                to_recv = weights[key]
                recv_list[-1].append(
                    self.training_operator.ones(to_recv.shape, cpu=False))

        for i in range(self.num_ps):
            for j in range(len(self.name_list[i])):
                v = self.training_operator.to_cupy(recv_list[i][j])
                col.recv(v, self.num_workers+i, self.group_name)

        self.recv_list = recv_list

    def compute_calculate_and_send(self):
        recv_list = self.recv_list
        del self.recv_list
        params = dict()
        metrics = {}

        # parameter updates
        for i in range(self.num_ps):
            param_shard_keys = self.name_list[i]
            for j in range(len(param_shard_keys)):
                params[param_shard_keys[j]] = recv_list[i][j]

        loss_val, grad = self.compute_gradients(params)
        metrics["train_loss"] = loss_val
        split_grad = self.split_gradients(grad, self.assignments)
        for i in range(self.num_ps):
            this_shard = self.index_shard(split_grad, i)
            for _, v in this_shard.items():
                cv = self.training_operator.to_cupy(v)
                col.send(cv, self.num_workers+i, self.group_name)
        return metrics

    def compute(self):
        """Returns the loss, and send gradients to servers"""
        metrics = {}
        
        # First receive params from servers
        param_shards = []
        weights = self.get_named_parameters(cpu=False)
        params = dict()
        # create the receive lists to group collective calls
        recv_list = []
        for i in range(self.num_ps):
            recv_list.append([])
            param_shard_keys = self.name_list[i]
            for key in param_shard_keys:
                to_recv = weights[key]
                recv_list[-1].append(
                    self.training_operator.ones(to_recv.shape, cpu=False))

        for i in range(self.num_ps):
            for j in range(len(self.name_list[i])):
                v = self.training_operator.to_cupy(recv_list[i][j])
                col.recv(v, self.num_workers+i, self.group_name)

        # parameter updates
        for i in range(self.num_ps):
            param_shard_keys = self.name_list[i]
            for j in range(len(param_shard_keys)):
                params[param_shard_keys[j]] = recv_list[i][j]

        loss_val, grad = self.compute_gradients(params)
        metrics["train_loss"] = loss_val
        split_grad = self.split_gradients(grad, self.assignments)
        for i in range(self.num_ps):
            this_shard = self.index_shard(split_grad, i)
            for _, v in this_shard.items():
                cv = self.training_operator.to_cupy(v)
                col.send(cv, self.num_workers+i, self.group_name)
        return metrics

    def validate(self, info={}):
        return self.training_operator.validate(info)

    def shutdown(self):
        # destroy the collective group resources on this process
        col.destroy_collective_group(self.group_name)
        if self.training_operator:
            del self.training_operator
        return 1

class DataParallelGroup:
    """Spawn a group a replicas for data-parallel training."""
    def __init__(self,
                 params,
                 dist_params,
                 num_cpus_per_actor,
                 num_gpus_per_actor):
        self._params = params
        self._dist_params = dist_params
        self._num_cpus_per_actor = num_cpus_per_actor
        self._num_gpus_per_actor = num_gpus_per_actor

        self.is_server = self._dist_params["is_server"]
        self.num_ps = self._dist_params["num_ps"]
        self.num_workers = self._dist_params["num_workers"]

        self._distributed_actors = None

    def _setup_collective_group(self, num_replicas):
        if self._dist_params["strategy"] == "ps":
            num_ps = self._dist_params["num_ps"]
            num_workers = self._dist_params["num_workers"]
            is_server = self.is_server
            rets = [actor.setup_collective_group.remote(rank=i+is_server*num_workers, num_workers=num_workers, num_ps=num_ps, backend="nccl")
                    for i, actor in enumerate(self._distributed_actors)]
        else:  # this can be extend for allreduce.
            raise RuntimeError("Unrecognized strategy.")
        return rets

    def setup_operator(self):
        setups = [actor.setup_operator.remote()
                for i, actor in enumerate(self._distributed_actors)]
        return setups

    def start_actors(self, num_actors):
        if self.is_server:
            # make an actor
            RemoteActor = ray.remote(num_cpus=self._num_cpus_per_actor,
                                     num_gpus=self._num_gpus_per_actor,
                                     )(PS) # resources={"server":1}
        else:
            RemoteActor = ray.remote(num_cpus=self._num_cpus_per_actor,
                                     num_gpus=self._num_gpus_per_actor,
                                     )(Worker) #resources={"worker":1}

        self._distributed_actors = [
            RemoteActor.remote(**self._params)
            for _ in range(num_actors)
        ]

        # setup the rank and group in each replica
        ray.get(self._setup_collective_group(
            len(self._distributed_actors)))

    def test_connection(self):
        rets = [actor.test_connection.remote() 
                for _, actor in enumerate(self.actors)]
        return rets

    def set_assignments(self, assignments):
        rets = [actor.set_assignments.remote(assignments) 
                for _, actor in enumerate(self.actors)]
        return rets

    def start_iteration(self):
        rets = [actor.start_iteration.remote() 
                for _, actor in enumerate(self.actors)]

    def get_train_loader_len(self):
        lens = ray.get([actor.get_train_loader_len.remote() 
                        for actor in self.actors])

        if len(set(lens)) != 1:
            raise RuntimeError("All actors should have the same dataloader len.")

        return lens[0]

    def train(self, max_retries=1, info={}):
        rets = [actor.train.remote()
                for _, actor in enumerate(self.actors)]
        stats = ray.get(rets)
        return stats

    def validate(self, info={}):
        rets = [actor.validate.remote(info)
                for _, actor in enumerate(self.actors)]
        stats = ray.get(rets)
        return stats

    def shutdown(self, force=False):
        rets = [actor.shutdown.remote()
                for _, actor in enumerate(self.actors)]
        stats = ray.get(rets)
        return stats

    def reset(self):
        pass

    @property
    def actors(self):
        return self._distributed_actors

    def save_parameters(self, checkpoint):
        rets = [self.actors[0].save_parameters.remote(checkpoint)]
        ray.get(rets)

    def load_parameters(self, checkpoint):
        rets = [actor.load_parameters.remote(checkpoint)
                for _, actor in enumerate(self.actors)]
        ray.get(rets)

    def set_parameters(self, params):
        rets = [actor.set_parameters.remote(params)
                for _, actor in enumerate(self.actors)]
        ray.get(rets)
        
    def get_parameters(self, cpu=False):
        ret = self.actors[0].get_parameters.remote(cpu)
        return ray.get([ret])[0]

    def get_named_parameters(self, cpu=False):
        ret = self.actors[0].get_named_parameters.remote(cpu)
        return ray.get([ret])[0]

    def split_parameters(self, assignments):
        ret = self.actors[0].split_parameters.remote(assignments)
        return ray.get([ret])[0]

    def clean_redundancy(self):
        rets = [actor.clean_redundancy.remote()
                for _, actor in enumerate(self.actors)]
        ray.get(rets)