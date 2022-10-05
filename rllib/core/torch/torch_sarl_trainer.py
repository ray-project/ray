import numpy as np
import torch
from torch.nn.parallel import DistributedDataParallel as DDP

from ray.rllib.core.sarl_trainer import SARLTrainer

from ray.util.queue import Queue

import ray.train as train
from ray.air import session
from ray.air.config import ScalingConfig
from ray.train._internal.utils import construct_train_func
from ray.train._internal.backend_executor import BackendExecutor
from ray.train.torch import TorchConfig
from ray.train import TrainingIterator
from ray.train._internal.checkpoint import CheckpointManager
from ray.train._internal.dataset_spec import RayDatasetSpec


class TorchSARLTrainer(SARLTrainer):
    def __init__(self, config):
        num_workers = config.get("num_gpus", 0) or 1
        use_gpu = bool(config.get("num_gpus", 0))
        init_rl_module_fn = config.get("rl_module_init_fn", None)
        module_config = config.get("rl_module_config", {})
        """
        we can't use fractional gpus with this :(

        Leads to nccl errors:

        ncclInvalidUsage: This usually reflects invalid usage of NCCL library
        (such as too many async ops, too many collectives at once, mixing
        streams in a group, etc).
        """
        scaling_config = ScalingConfig(
            num_workers=num_workers,
            use_gpu=use_gpu,
        )
        self.queues, self.training_iterator = self._make_ray_train_trainer(
            scaling_config, init_rl_module_fn, module_config
        )

    @staticmethod
    def init_rl_module(module_config):
        raise NotImplementedError

    @staticmethod
    def init_optimizer(module, optimizer_config):
        raise NotImplementedError

    @staticmethod
    def compute_loss_and_update(module, batch, optimizer):
        raise NotImplementedError

    @staticmethod
    def compute_loss(batch, fwd_out, device, **kwargs):
        raise NotImplementedError

    @staticmethod
    def compute_grads_and_apply_if_needed(
        batch, fwd_out, loss_out, rl_module, optimizer, device, **kwargs
    ):
        if not isinstance(optimizer, torch.optim.Optimizer):
            for _opt in optimizer:
                _opt.zero_grad()
        else:
            optimizer.zero_grad()
        loss = loss_out["total_loss"]
        loss.backward()
        if not isinstance(optimizer, torch.optim.Optimizer):
            for _opt in optimizer:
                _opt.step()
        else:
            optimizer.step()

        return {}

    @staticmethod
    def compile_results(
        batch,
        fwd_out,
        loss_out,
        compute_grads_and_apply_if_needed_info_dict,
        rl_module,
        **kwargs
    ):
        raise NotImplementedError

    def get_weights(self):
        # We need to get the weights from the trainer to the workers
        return self.curr_weights

    def _make_ray_train_trainer(self, scaling_config, init_rl_module_fn, module_config):
        init_rl_module_fn = init_rl_module_fn or self.init_rl_module
        queues = [Queue(maxsize=1) for _ in range(scaling_config.num_workers)]
        backend_config = TorchConfig()
        config = {
            "rl_module_config": module_config,
            "init_rl_module_fn": init_rl_module_fn,
            "init_optimizer_fn": self.init_optimizer,
            "compute_grads_and_apply_if_needed_fn": (
                self.compute_grads_and_apply_if_needed
            ),
            "compute_loss_fn": self.compute_loss,
            "compile_results_fn": self.compile_results,
            "queues": queues,
        }
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
        checkpoint_manager = CheckpointManager(checkpoint_strategy=None, run_dir=None)
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
        return queues, training_iterator

    @staticmethod
    def _training_func(config):
        rl_module_config = config["rl_module_config"]
        init_rl_module_fn = config["init_rl_module_fn"]
        init_optimizer_fn = config["init_optimizer_fn"]
        compute_loss_fn = config["compute_loss_fn"]
        compute_grads_and_apply_if_needed_fn = config[
            "compute_grads_and_apply_if_needed_fn"
        ]
        compile_results_fn = config["compile_results_fn"]
        queue = config["queues"][session.get_local_rank()]

        rl_module = init_rl_module_fn(rl_module_config)
        device = train.torch.get_device()
        rl_module.to(device)
        pg = torch.distributed.new_group(list(range(session.get_world_size())))
        device_id = [session.get_local_rank()] if str(device).startswith("cuda") else []
        ddp_rl_module = DDP(rl_module, device_ids=device_id, process_group=pg)
        unwrapped_module = ddp_rl_module.module
        optimizer = init_optimizer_fn(ddp_rl_module, {})
        global_size = session.get_world_size()
        while 1:
            whole_batch = queue.get()
            batch_size = np.ceil(len(whole_batch) / global_size)
            start = batch_size * session.get_local_rank()
            end = min(start + batch_size, len(whole_batch))
            batch = whole_batch[int(start) : int(end)]
            fwd_out = ddp_rl_module(batch, device=device)
            loss_dict = compute_loss_fn(batch, fwd_out, device=device)
            info_dict = compute_grads_and_apply_if_needed_fn(
                batch, fwd_out, loss_dict, unwrapped_module, optimizer, device
            )
            training_results = compile_results_fn(
                batch, fwd_out, loss_dict, info_dict, unwrapped_module
            )
            results = {"module_weights": None, "training_results": training_results}
            session.report(results)
