import logging
import os
from unittest.mock import MagicMock

import torch
import torch.nn
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

        # TODO: This code depends on the model having a fused_optimizer,
        # which is hidden in the `get_model` method of the factory.
        dense_optimizer = KeyedOptimizerWrapper(
            dict(in_backward_optimizer_filter(self.model.named_parameters())),
            lambda params: torch.optim.Adagrad(params, lr=15.0, eps=1e-8),
        )
        self.optimizer = CombinedOptimizer(
            [self.model.fused_optimizer, dense_optimizer]
        )

        def dummy_fwd(self, x):
            loss, outputs = MagicMock(), MagicMock()
            return loss, outputs

        custom_fwd = dummy_fwd if self.benchmark_config.skip_train_step else None

        self.pipeline = TrainPipelineSparseDist(
            self.model,
            self.optimizer,
            device,
            execute_all_batches=True,
            custom_fwd=custom_fwd,
        )

    def _train_step(self, train_dataloader):
        if self.benchmark_config.skip_train_step:
            # NOTE: Setting model to eval mode to skips the backward + optimizer
            # step in the TrainPipelineSparseDist pipeline.
            self.pipeline._model.eval()
        else:
            self.pipeline._model.train()

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
