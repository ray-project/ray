import numpy as np
import ray
import torch
import ray.train as train
from ray.air import session

from ray.air.config import ScalingConfig

from torch.nn.parallel import DistributedDataParallel as DDP

from ray.train._internal.utils import construct_train_func
from ray.train._internal.backend_executor import BackendExecutor
from ray.train.torch import TorchConfig
from ray.train import TrainingIterator
from ray.train._internal.checkpoint import CheckpointManager
from ray.train._internal.dataset_spec import RayDatasetSpec
from ray.util.queue import Queue


class TorchSARLTrainer:
    def __init__(self, config):
        num_workers = config.get("num_gpus", 1) or 1
        use_gpu = bool(config.get("num_gpus", 0))
        if not all(key in config for key in ["module_class", "module_config"]):
            raise ValueError("You must specify a RL module_class and RL module_config in order to use the TorchSARLTrainer") 
        module_class = config["module_class"]
        module_config = config["module_config"]
        batch_size = config["batch_size"]
        scaling_config = ScalingConfig(num_workers=num_workers, resources_per_worker={"cpu": 1, "gpu": 0.5})
        self.training_iterator = self._make_ray_train_trainer(scaling_config, module_class, module_config, batch_size)

    def train(self, batch):
        # this is the function is called by the user to train the model
        for queue in self.queues:
            queue.put(batch)
        results = next(self.training_iterator)
        self.curr_weights = results[0]["module_weights"]
        return results

    @staticmethod
    def init_optimizer(module, optimizer_config):
        raise NotImplementedError

    @staticmethod
    def compute_loss_and_update(module, batch, optimizer):
        raise NotImplementedError

    def get_weights(self):
        # We need to get the weights from the trainer to the workers
        return self.curr_weights

    def _make_ray_train_trainer(self, scaling_config, module_class, module_config, batch_size):
        self.queues = [Queue(maxsize=1) for _ in range(scaling_config.num_workers)]
        backend_config = TorchConfig()
        config = {"rl_module_config": module_config,
                  "rl_module_class": module_class,
                  "init_optimizer_fn": self.init_optimizer,
                  "compute_loss_and_update_fn": self.compute_loss_and_update,
                  "queues": self.queues,
                  "batch_size": batch_size}
        train_loop_per_worker = construct_train_func(
            self._training_func,
            config,
            fn_arg_name="train_loop_per_worker",
            discard_returns=True,
        )
        backend_executor = BackendExecutor(
            backend_config=backend_config,
            num_workers=scaling_config.num_workers,
            num_cpus_per_worker=scaling_config.num_cpus_per_worker,
            num_gpus_per_worker=scaling_config.num_gpus_per_worker,
            max_retries=0,
        )
        checkpoint_manager = CheckpointManager(
            checkpoint_strategy=None, run_dir=None
        )
        # Start the remote actors.
        backend_executor.start(initialization_hook=None)
        empty_spec = RayDatasetSpec(dataset_or_dict=None)
        training_iterator = TrainingIterator(
            backend_executor=backend_executor,
            backend_config=backend_config,
            train_func=train_loop_per_worker,
            dataset_spec=empty_spec,
            checkpoint_manager=checkpoint_manager,
            checkpoint=None,
            checkpoint_strategy=None,
        )
        return training_iterator

    @staticmethod
    def _training_func(config):
        if "rl_module_config" not in config:
            raise ValueError("rl_module_config not in config")
        rl_module_config = config["rl_module_config"]
        rl_module_class = config["rl_module_class"]
        init_optimizer_fn = config["init_optimizer_fn"]
        compute_loss_and_update_fn = config["compute_loss_and_update_fn"]
        batch_size = config["batch_size"]

        rl_module = rl_module_class(rl_module_config)
        queue = config["queues"][session.get_local_rank()]

        device = train.torch.get_device()
        rl_module.to(device)
        pg = torch.distributed.new_group(list(range(session.get_world_size())))
        device_id = [session.get_local_rank()] if str(device).startswith("cuda") else []
        ddp_rl_module = DDP(rl_module, device_ids=device_id, process_group=pg)
        optimizer = init_optimizer_fn(ddp_rl_module, {})
        global_size = session.get_world_size()
        while 1:
            whole_batch = queue.get()
            batch_size = np.ceil(len(whole_batch) / global_size)
            start = batch_size * session.get_local_rank()
            end = min(start + batch_size, len(whole_batch))
            batch = whole_batch[int(start):int(end)]
            training_results = compute_loss_and_update_fn(ddp_rl_module, batch, optimizer)
            results = {"module_weights": None, "training_results": training_results}
            session.report(results)
