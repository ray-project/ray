import ray
from ray.util.distml.base_trainer import BaseTrainer
import ray.util.collective as col
import cupy as cp


class ParameterServerStrategy(BaseTrainer):
    def __init__(self, *args, num_workers=1, num_ps=1, fusion=False, **kwargs):
       
        self.num_ps = num_ps 
        self.num_workers = num_workers
        self._fusion = fusion

        self.assignments = None
        super(ParameterServerStrategy, self).__init__(*args, **kwargs)

    def _init_strategy(self):
        """Do initialization for the distributed strategy."""
        # All sync with worker 0
        init_weights_id = self.worker_group.get_parameters(cpu=True)

        self._round_robin_sharding()

        # set assignments to every worker
        self.worker_group.set_assignments(self.assignments)
        # ray.wait([w.set_assignments.remote(self.assignments) for w in self.workers])

        # all workers get synced
        self.worker_group.set_parameters(init_weights_id)
        # for i, worker in enumerate(self.workers):
        #     if i != 0:
        #         ray.wait([worker.set_parameters.remote(init_weights_id)])

        # now spawn parameter server actors
        shard_ids = self.worker_group.split_parameters(self.assignments)
        # shard_ids = self.workers[0].split_parameters.remote(self.assignments)

        for i, server in enumerate(self.server_group.replicas):
            this_shard_id = self.worker_group.replicas[0].index_shard.remote(shard_ids, i)
            ray.wait([server.set_params.remote(this_shard_id)])

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
            "num_cpus_per_worker": self.num_cpus_per_worker,
            "num_gpus_per_worker": self.num_gpus_per_worker,
        }

        servergroup_init_args = {
            "params": params,
            "dist_params": dist_params_server,
            "num_cpus_per_worker": self.num_cpus_per_worker,
            "num_gpus_per_worker": self.num_gpus_per_worker,
        }

        # Should we make two groups for worker and server?
        self.worker_group = DataParallelGroup(**workergroup_init_args)
        self.server_group = DataParallelGroup(**servergroup_init_args)

        # Once the group is created, we start it.
        self.worker_group.start_replicas(self.num_workers)
        self.server_group.start_replicas(self.num_ps)  # server at the last num_ps processes.

        worker_rets = self.worker_group.test_connection()
        server_rets = self.server_group.test_connection()
        ray.get(worker_rets+server_rets)
        ray.get(self.worker_group.setup_operator())
        ray.get(self.server_group.setup_operator())  # server at the last num_ps processes.

    def shutdown(self, force=False):
        self.data_parallel_group.shutdown(force=force)

    def save_parameters(self, checkpoint):
        self.data_parallel_group.save_parameters(checkpoint)

    def load_parameters(self, checkpoint):
        self.data_parallel_group.load_parameters(checkpoint)

    def _round_robin_sharding(self):
        """Generate the assignment of variable to servers."""
        parameter_distribution = ray.get(self.worker_group.replicas[0].params_distribution.remote())
        assignments = [0 for _ in parameter_distribution]
        loads = [0 for _ in range(self.num_ps)]
        for i, var_size in enumerate(parameter_distribution):
            min_ps_index = loads.index(min(loads))
            loads[min_ps_index] += var_size
            assignments[i] = min_ps_index
        print("Load of each ps {}".format(loads))
        self.assignments = assignments

    def step(self):
        loss_vals = []
        rets = []
        for worker in self.workers:
            for server in self.servers:
                # every server sends its shard to the worker
                server.send_params.remote(self.workers.index(worker))
            # the worker receives shards from ps, compute loss, gradients
            # and sends these gradients to every server
            loss = worker.compute.remote()
            time.sleep(100)
            for server in self.servers:
                rets.append(server.update.remote(self.workers.index(worker)))
            rets.append(loss)
            loss_vals.append(loss)
        ray.get(rets)
        return ray.get(loss_vals)


class PS(object):  # HUI: maybe we could let 'PS' derive 'Worker'
    def __init__(self, 
                 training_operator_cls, operator_config):
        # rank should be true rank. means, rank has already plus num_worker.
        self.training_operator_cls = training_operator_cls
        self.operator_config = operator_config

        self.grad_counts = 0
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
        # self.name_list = [[] for i in range(num_ps)]

        # the last num_ps processes are servers.
        col.init_collective_group(num_ps + num_workers, rank,
                                  backend=backend, group_name=group_name)

    def test_connection(self):
        # hand shake with server?
        for i in range(self.num_workers):
            recv = cp.zeros((1,))
            col.recv(recv, i, self.group_name)
        for i in range(self.num_workers):
            recv = cp.zeros((1,))
            col.send(recv, i, self.group_name)
        return

    def send_parameters(self, dst_rank):
        """ Send this param shard to the destination worker """
        count = 0
        groupStart()
        for name, v in self.params.items():
            col.send(v, dst_rank, "default")
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

    def get_parameters(self):
        return self.params

    def set_parameters(self, params):
        return self.training_operator.set_parameters(params)
        # for k, v in params.items():
        #     self.params[k] = v.cuda()
        # self.optimizer = torch.optim.SGD(self.params.values(), lr=0.001)

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
            col.recv(recv_list[i], src_rank, self.group_name)
        groupEnd()

        for i in range(len(keys)):
            grads[keys[i]] = recv_list[i]

        self._inc_gradients(grads)
        if self.grad_counts == len(self.workers):
            #self.optimizer.zero_grad()
            #self._set_gradients(grads)

            ## call apply_updates?
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
            send = cp.ones((1,))
            col.send(send, self.num_workers + i, self.group_name)
        for i in range(self.num_ps):
            send = cp.ones((1,))
            col.recv(send, self.num_workers + i, self.group_name)
        return

    def num_params(self):
        return len(self.get_parameters())

    def params_distribution(self):
        distribution = []
        weights = self.get_parameters(cpu=True)
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
        return self.training_operator.set_parameters(params)

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
        weights = self.get_parameters(cpu=False)
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
                col.recv(recv_list[i][j], self.num_workers+i, "default")
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
                col.send(v, self.num_workers+i, "default")
        groupEnd()
        return loss


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

        self.is_server = self._dist_params["is_server"]
        self.num_ps = self._dist_params["num_ps"]
        self.num_workers = self._dist_params["num_workers"]

        self._distributed_replicas = None

    def _setup_collective_group(self, num_replicas):
        if self._dist_params["strategy"] == "ps":
            num_ps = self._dist_params["num_ps"]
            num_workers = self._dist_params["num_workers"]
            is_server = self.is_server
            rets = [replica.setup_collective_group.remote(rank=i+is_server*num_workers, num_workers=num_workers, num_ps=num_ps, backend="nccl")
                    for i, replica in enumerate(self._distributed_replicas)]
        else:  # this can be extend for allreduce.
            raise RuntimeError("Unrecognized strategy.")
        return rets

    def setup_operator(self):
        setups = [replica.setup_operator.remote()
                for i, replica in enumerate(self._distributed_replicas)]
        return setups

    def start_replicas(self, num_replicas):
        if self.is_server:
            # make an actor
            RemoteReplica = ray.remote(num_cpus=self._num_cpus_per_worker,
                                       num_gpus=self._num_gpus_per_worker)(PS)
        else:
            RemoteReplica = ray.remote(num_cpus=self._num_cpus_per_worker,
                                       num_gpus=self._num_gpus_per_worker)(Worker)

        self._distributed_replicas = [
            RemoteReplica.remote(**self._params)
            for _ in range(num_replicas)
        ]

        # move to asyne
        # setup the rank and group in each replica
        ray.get(self._setup_collective_group(
            len(self._distributed_replicas)))

        # ray.get(self.test_connection())

        # # setup the model training operator 
        # ray.get(self._setup_operator())

    def test_connection(self):
        rets = [replica.test_connection.remote() 
                for _, replica in enumerate(self.replicas)]
        return rets

    def set_assignments(assignments):
        rets = [replica.set_assignments.remote(assignments) 
                for _, replica in enumerate(self.replicas)]
        return rets


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

    def set_parameters(self, params):
        rets = [replica.set_parameters.remote(params)
                for _, replica in enumerate(self.replicas)]
        ray.get(rets)
        
    def get_parameters(self, cpu=False):
        ret = self.replicas[0].get_parameters.remote(cpu)
        return ray.get([ret])

    def split_parameters(self, assignments):
        ret = self.replicas[0].split_parameters.remote(assignments)
        return ray.get([ret])