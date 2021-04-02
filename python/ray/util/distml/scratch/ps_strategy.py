import ray
from ray.util.distml.base_trainer import BaseTrainer
import ray.util.collective as col



class ParameterServerStrategy(BaseTrainer):
    def __init__(self, *args, num_ps=1, fusion=False, **kwargs):
        super(ParameterServerStrategy, self).__init__(*args, **kwargs)
        self.num_ps = num_ps
        self.num_worker = self.world_size - self.num_ps
        self._fusion = fusion

    def _init_strategy(self):
        """Do initialization for the distributed strategy."""
        pass

    def train(self, *, max_retries=1, info=None):
        # train one epoch

        stats = self.data_parallel_group.train(max_retries=1, info={})
        return stats
        # rets = [replica.train.remote()
        #         for _, replica in enumerate(self.data_parallel_group.replicas)]
        # ray.get(rets)

    # def train_batch(self, batches):
    #     # First, let each worker proceed with a train_step
    #     # self.data_parallel_group.train()
    #     rets = [replica.train_batch.remote(batches[i])
    #             for i, replica in enumerate(self.data_parallel_group.replicas)]
    #     ray.get(rets)

    def validate(self):
        stats = self.data_parallel_group.validate(info={})
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
            group_name="default",
        )
        # (3) other arguments that used to init the DataParallelGrup
        group_init_args = {
            "params": params,
            "dist_params": dist_params,
            "num_cpus_per_worker": self.num_cpus_per_worker,
            "num_gpus_per_worker": self.num_gpus_per_worker,
        }

        self.worker_group = DataParallelGroup(**group_init_args)

        # Once the group is created, we start it.
        self.worker_group.start_replicas(num_workers)

    def _start_servers(self, num_ps):
        operator_config = self._operator_config.copy()
        params = dict(
            training_operator_cls = self.training_operator_cls,
            operator_config = operator_config
        )
        dist_params = dict(
            group_name="default",
        )
        group_init_args = {
            "params": params,
            "dist_params": dist_params,
            "num_cpus_per_worker": self.num_cpus_per_worker,
            "num_gpus_per_worker": self.num_gpus_per_worker,
        }

        self.worker_group = DataParallelGroup(**group_init_args)

        # Once the group is created, we start it.
        self.data_parallel_group.start_replicas(num_workers)

    def shutdown(self, force=False):
        self.data_parallel_group.shutdown(force=force)

    def save_parameters(self, checkpoint):
        self.data_parallel_group.save_parameters(checkpoint)

    def load_parameters(self, checkpoint):
        self.data_parallel_group.load_parameters(checkpoint)


class PS(object):
    def __init__(self, workers, world_size, rank):
        self.params = dict()
        self.optimizer = None
        self.workers = workers
        self.world_size = world_size
        self.rank = rank
        self.grad_counts = 0
        collective.init_collective_group(self.world_size, self.rank, "nccl", "default")
        for i in range(len(self.workers)):
            recv = torch.zeros(1,).cuda()
            collective.recv(recv, i, "default")
        for i in range(len(self.workers)):
            recv = torch.zeros(1,).cuda()
            collective.send(recv, i, "default")

    def send_params(self, dst_rank):
        """ Send this param shard to the destination worker """
        count = 0
        groupStart()
        for name, v in self.params.items():
            collective.send(v, dst_rank, "default")
            if count < 1:
                count += 1
                logging.warning(f"{name} {v[0][0]}, {v.size()}")
            elif count < 2:
                count += 1
                logging.warning(f"{name} {v}, {v.size()}")
            elif count < 3:
                count += 1
                logging.warning(f"{name} {v}, {v.size()}")
            else:
                break
        groupEnd()
        time.sleep(5000)

    def get_params(self):
        return self.params

    def set_params(self, params):
        for k, v in params.items():
            self.params[k] = v.cuda()
        self.optimizer = torch.optim.SGD(self.params.values(), lr=0.001)
        return True

    def _inc_gradients(self, gradients):
        # gradients should be a stitched dict
        for name, p in self.get_params().items():
            if gradients[name] is not None:
                if p.grad is None:
                    p.grad = gradients[name]
                else:
                    p.grad += gradients[name]
        self.grad_counts += 1

    def update(self, src_rank):
        """Receive gradients and update"""
        keys = list(self.params.keys())
        grads = dict()
        recv_list = []
        for key in keys:
            to_recv = self.params[key]
            recv_list.append(torch.zeros(to_recv.size()).cuda())

        groupStart()
        for i in range(len(keys)):
            collective.recv(recv_list[i], src_rank, "default")
        groupEnd()

        for i in range(len(keys)):
            grads[keys[i]] = recv_list[i]

        self._inc_gradients(grads)
        if self.grad_counts == len(self.workers):
            #self.optimizer.zero_grad()
            #self._set_gradients(grads)
            self.optimizer.step()
            self.optimizer.zero_grad()

        return True



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

    def setup_collective_group(self, rank, world_size, num_ps, backend, group_name="default"):
        self.rank = rank
        self.num_ps = num_ps
        self.group_name = group_name
        self.group_size = world_size
        self.name_list = [[] for i in range(num_ps)]

        # the first num_ps processes are servers.
        col.init_collective_group(world_size, num_ps+rank,
                                  backend=backend, group_name=group_name)
        
        # hand shake with server?
        for i in range(num_ps):
            send = torch.ones(1,).cuda()
            collective.send(send, self.num_workers + i, "default")
        for i in range(num_ps):
            send = torch.ones(1,).cuda()
            collective.recv(send, self.num_workers + i, "default")
        return

    def num_params(self):
        return len(self.get_weights())

    def params_distribution(self):
        distribution = []
        weights = self.get_weights(cpu=True)
        for k, v in weights.items():
            distribution.append(v.numel())
        return distribution

    def get_data_loader(self):
        return self.training_operator.trainloader

    def derive_updates(self, batch):
        # TODO (Hao): handling data loader next.
        # TODO (Hao): change it to derive_update and apply_update.
        return self.training_operator.derive_updates(batch)

    def apply_updates(self, updates):
        assert updates
        self.training_operator.apply_updates(updates)

    def updates_transform(self, updates):
        return self.training_operator.updates_transform(updates)

    def compute_gradients(self, weights):
        # different from original core ps. 
        # Here derive_updates return loss_val and graident in order.
        return self.training_operator.derive_updates(batch)

    def split_gradients(self, grad, assignments):
        # assuming messages are gradients or parameters
        # this grad is ready to be called by apply_gradients in ParameterServer
        num_shards = np.unique(np.array(assignments)).size
        shards = [dict() for i in range(num_shards)]
        for i, (k, v) in enumerate(grad.items()):
            shards[assignments[i]][k] = v
        return shards

    def split_parameters(self, assignments):
        params = self.get_parameters(cpu=False)
        num_shards = np.unique(np.array(assignments)).size
        shards = [dict() for i in range(num_shards)]
        for i, (k, v) in enumerate(params.items()):
            shards[assignments[i]][k] = v.data.cpu()  # this will only be used by ps which locates on cpus
        return shards

    def index_shard(self, shards, index):
        return shards[index]

    def set_parameters(self, params):
        return self.training_operator.get_parameters(params)

    def get_parameters(self, cpu):
        return self.training_operator.get_parameters()

    def get_gradients(self):
        # training_operator call gradients or we save gradient in replica 
        # when derive_updates.
        self.training_operator.get_gradients()

    def set_assignments(self, assignments):
        self.assignments = assignments
        keys = list(self.get_weights(cpu=False).keys())
        for i, a in enumerate(self.assignments):
            self.name_list[a].append(keys[i])

    def compute(self):
        """Returns the loss, and send gradients to servers"""
        # First receive params from servers
        param_shards = []
        weights = self.get_weights(cpu=False)
        params = dict()
        # create the receive lists to group collective calls
        recv_list = []
        for i in range(self.num_ps):
            recv_list.append([])
            param_shard_keys = self.name_list[i]
            for key in param_shard_keys:
                to_recv = weights[key]
                recv_list[-1].append((torch.ones(to_recv.size()) * 2).cuda())

        logging.warning(f"worker {self.rank} {recv_list[0][0][0][0]}, {recv_list[0][0].size()}, {recv_list[0][1]}, {recv_list[0][1].size()}, {recv_list[0][2]}, {recv_list[0][2].size()}")
        groupStart()
        for i in range(self.num_ps):
            for j in range(len(self.name_list[i])):
                logging.warning(f"recv {i}{j} {self.name_list[i][j]}")
                collective.recv(recv_list[i][j], self.num_workers+i, "default")
                if j == 2:
                    break
            break
        groupEnd()
        logging.warning(f"worker {self.rank} {recv_list[0][0][0][0]}, {recv_list[0][0].size()}, {recv_list[0][1]}, {recv_list[0][1].size()}, {recv_list[0][2]}, {recv_list[0][2].size()}")
        time.sleep(100)
        for i in range(self.num_ps):
            param_shard_keys = self.name_list[i]
            for j in range(len(param_shard_keys)):
                params[param_shard_keys[j]] = recv_list[i][j]

        loss, grad  = self.compute_gradients(params)
        split_grad = self.split_gradients(grad, self.assignments)
        groupStart()
        for i in range(self.num_ps):
            this_shard = self.index_shard(split_grad, i)
            for _, v in this_shard.items():
                collective.send(v, self.num_workers+i, "default")
        groupEnd()
        return loss


# class Replica:
#     """Express the training semantics of data-parallel replica.

#     This class includes some glue code between the user-provided opterator
#     and Ray cluster/collective-group setup.
#     """
#     def __init__(self,
#                  training_operator_cls, operator_config):
#         self.training_operator_cls = training_operator_cls
#         self.operator_config = operator_config
#         # collective-related information
#         self.group_size = None
#         self.rank = None
#         self.group_name = None

#     def setup_operator(self):
#         # figure out the signature of training_operator_cls later.
#         self.training_operator = self.training_operator_cls(self.operator_config)

#     def setup_collective_group(self, rank, world_size, backend, group_name="default"):
#         self.rank = rank
#         self.group_name = group_name
#         self.group_size = world_size
#         col.init_collective_group(world_size, rank,
#                                   backend=backend, group_name=group_name)
#         return

#     def num_params(self):
#         return len(self.get_weights())

#     def params_distribution(self):
#         distribution = []
#         weights = self.get_weights(cpu=True)
#         for k, v in weights.items():
#             distribution.append(v.numel())
#         return distribution


#     def train(self):
#         # need to record some metric, like loss
#         for batch in self.training_operator.yield_train_loader():
#             loss_val = self.train_batch(batch)
#         return 1

#     def train_batch(self, batch):
#         loss_val, updates = self.derive_updates(batch)
#         assert updates
#         # TODO: make the signature correct
        
#         # HUI: maybe need a transform for updates to make a list.
#         # for jax, gradient need to call `tree_flatten` and get list.
#         for g in self.updates_transform(updates):
#             col.allreduce(g)
#         self.apply_updates(updates)
#         return loss_val

#     def derive_updates(self, batch):
#         # TODO (Hao): handling data loader next.
#         # TODO (Hao): change it to derive_update and apply_update.
#         return self.training_operator.derive_updates(batch)

#     def apply_updates(self, updates):
#         assert updates
#         self.training_operator.apply_updates(updates)

#     def updates_transform(self, updates):
#         return self.training_operator.updates_transform(updates)

#     def validate(self, info={}):
#         return self.training_operator.validate(info)

#     def shutdown(self):
#         # destroy the collective group resources on this process
#         col.destroy_collective_group(self.group_name)
#         if not self.training_operator:
#             del self.training_operator
#         return 1

#     def save_parameters(self, checkpoint):
#         self.training_operator.save_parameters(checkpoint)

#     def load_parameters(self, checkpoint):
#         self.training_operator.load_parameters(checkpoint)



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

    def train(self, max_retries=1, info={}):
        rets = [replica.train.remote()
                for _, replica in enumerate(self.replicas)]
        stats = ray.get(rets)
        return stats

    def validate(self, info={}):
        rets = [replica.validate.remote(info)
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
