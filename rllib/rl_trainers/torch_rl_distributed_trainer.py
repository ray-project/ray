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


class TorchRLDataParallelTrainer:

    def __init__(self, config):
        num_workers = config.get("num_gpus", 0)
        use_gpu = bool(num_workers)
        scaling_config = ScalingConfig(num_workers=num_workers, use_gpu=use_gpu)
        queues = [Queue(maxsize=1) for _ in range(num_workers)]
        backend_config = TorchConfig()
        config = {"queues": queues, "rl_module_config": config["rl_module_config"], 
            "rl_module_class": config["rl_module_class"]}
        train_loop_per_worker = construct_train_func(
            self.training_func,
            config,
            fn_arg_name="train_loop_per_worker",
            discard_returns=True,
        )
        self.debug_mode = config.get("debug_mode", False)

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

        self.training_iterator = TrainingIterator(
            backend_executor=backend_executor,
            backend_config=backend_config,
            train_func=train_loop_per_worker,
            dataset_spec=empty_spec,
            checkpoint_manager=checkpoint_manager,
            checkpoint=None,
            checkpoint_strategy=None,
        )
        self.curr_weights = None

    @classmethod
    def training_func(self, config):
        if "rl_module_config" not in config:
            raise ValueError("rl_module_config not in config")
        rl_module_config = config["rl_module_config"]
        rl_module_class = config["rl_module_class"]
        rl_module = rl_module_class(rl_module_config)
        queue = config["queues"][session.get_local_rank()]

        device = train.torch.get_device()
        num_models = rl_module.num_models

        pgs = [torch.distributed.new_group(list(range(session.get_world_size()))) for _ in range(num_models)]
        moved_models = []
        for pg, model in zip(pgs, rl_module.models):
            model.to(device)
            model = DDP(model, device_ids=[session.get_local_rank()], process_group=pg)
            moved_models.append(model)

        rl_module.set_models(moved_models)
        rl_module.init_optimizers()

        while 1:
            data = queue.get()
            out = []
            result = {}
            result["loss"] = {}
            for model in rl_module.models:
                # this has to call model.__call__ or model.forward
                # eventually in order to work with ddp
                out = model.forward_train(data)
                loss = model.loss(out)
                result["loss"][str(model)] = loss
                model_gradients = model.update(loss, get_gradients=self.debug_mode)

            # This is how we will return the weights from the trainer to the
            # workers
            result["module_weights"] = ray.put(rl_module.get_state())
            # session.report is kinda like the return statement after training
            session.report(result)

    def train(self, dataset):
        # this is the function is called by the user to train the model
        results = next(self.training_iterator.run(dataset))
        self.curr_weights = results["module_weights"]
        return results

    def get_weights(self):
        # We need to get the weights from the trainer to the workers
        return self.curr_weights
