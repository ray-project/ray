import logging
import os

import torch
from torchrec.distributed import TrainPipelineSparseDist
from torchrec.optim.keyed import CombinedOptimizer, KeyedOptimizerWrapper
from torchrec.optim.optimizers import in_backward_optimizer_filter

import ray.train
import ray.train.torch

from runner import TrainLoopRunner


logger = logging.getLogger(__name__)


class TorchRecRunner(TrainLoopRunner):
    def _setup(self):
        device = ray.train.torch.get_device()

        self.model = self.factory.get_model()

        dense_optimizer = KeyedOptimizerWrapper(
            dict(in_backward_optimizer_filter(self.model.named_parameters())),
            lambda params: torch.optim.Adagrad(params, lr=15.0, eps=1e-8),
        )
        self.optimizer = CombinedOptimizer(
            [self.model.fused_optimizer, dense_optimizer]
        )
        self.pipeline = TrainPipelineSparseDist(
            self.model, self.optimizer, device, execute_all_batches=True
        )

    def _train_step(self, train_dataloader):
        self.pipeline.progress(train_dataloader)

    def _validate_step(self, val_dataloader):
        return 0

    def _save_training_state(self, local_dir: str):
        return
        torch.save(self.model.state_dict(), os.path.join(local_dir, "model.pt"))
        torch.save(self.optimizer.state_dict(), os.path.join(local_dir, "optimizer.pt"))

    def _load_training_state(self, local_dir: str):
        return
        self.model.load_state_dict(
            torch.load(os.path.join(local_dir, "model.pt"), map_location="cpu")
        )
        self.optimizer.load_state_dict(
            torch.load(os.path.join(local_dir, "optimizer.pt"), map_location="cpu")
        )
