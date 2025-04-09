import logging
import gc
import os

import torch
import torch.nn

from torchrec.distributed.train_pipeline import StagedTrainPipeline, SparseDataDistUtil
from torchrec.distributed.train_pipeline.utils import PipelineStage
from torchrec.optim.keyed import CombinedOptimizer, KeyedOptimizerWrapper
from torchrec.optim.optimizers import in_backward_optimizer_filter

import ray.train
import ray.train.torch

from runner import TrainLoopRunner


logger = logging.getLogger(__name__)


class TorchRecRunner(TrainLoopRunner):
    def _setup(self):
        if self.factory.benchmark_config.mock_gpu:
            raise ValueError("Mock GPU is not supported for running TorchRec.")

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

        sdd = SparseDataDistUtil(
            model=self.model,
            data_dist_stream=torch.cuda.Stream(),
            # prefetch_stream=torch.cuda.Stream(),
        )
        pipeline = [
            PipelineStage(
                name="data_copy",
                runnable=lambda batch: batch.to(device, non_blocking=True),
                stream=torch.cuda.Stream(),
            ),
            PipelineStage(
                name="start_sparse_data_dist",
                runnable=sdd.start_sparse_data_dist,
                stream=sdd.data_dist_stream,
                fill_callback=sdd.wait_sparse_data_dist,
            ),
            # PipelineStage(
            #     name="prefetch",
            #     runnable=sdd.prefetch,
            #     stream=sdd.prefetch_stream,
            #     fill_callback=sdd.load_prefetch,
            # ),
        ]

        self.pipeline = StagedTrainPipeline(pipeline_stages=pipeline)

    def _wrap_dataloader(self, dataloader, prefix: str):
        dataloader_iter = iter(dataloader)

        def dataloader_with_torchrec_pipeline():
            while batch := self.pipeline.progress(dataloader_iter):
                yield batch

        return super()._wrap_dataloader(dataloader_with_torchrec_pipeline(), prefix)

    def _train_step(self, batch):
        self.model.train()

        self.optimizer.zero_grad()
        loss, out = self.model(batch)
        loss.backward()
        self.optimizer.step()

    def _validate_step(self, batch):
        self.model.eval()

        with torch.no_grad():
            loss, out = self.model(batch)

        return loss.item()

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

    def _cleanup(self):
        del self.model
        del self.optimizer
        del self.pipeline

        torch.cuda.synchronize()
        torch.cuda.empty_cache()
        gc.collect()