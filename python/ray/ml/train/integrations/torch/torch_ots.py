from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional, Type

import numpy as np
import torch
from ray import train
from ray.ml import ScalingConfig, RunConfig, Preprocessor, Checkpoint
from ray.ml.train.integrations.torch import TorchTrainer
from ray.ml.trainer import GenDataset
from ray.train.torch import TorchConfig
from torch import nn, optim


@dataclass
class TorchOffTheShelfConfig:
    batch_size: int = 32
    num_epochs: int = 10

    validation_dataset_name: str = "validation"
    validation_frequency: int = 1
    validation_fn: Optional[
        Callable[[Dict, torch.Tensor, torch.Tensor, np.float32], Dict]
    ] = None


def default_validation_fn(validation_results: Dict, prediction, labels, loss):
    old_loss = validation_results.get("validation_loss", 0.0)
    validation_results["validation_loss"] = old_loss + loss
    return validation_results


class TorchOffTheShelfTrainer(TorchTrainer):
    def __init__(
        self,
        *,
        model_cls: Optional[Type[nn.Module]],
        model_args: Optional[Dict] = None,
        loss_fn_cls: Optional[Type[nn.Module]] = nn.MSELoss,
        loss_fn_args: Optional[Dict] = None,
        optimizer_cls: Optional[Type[optim.Optimizer]] = optim.SGD,
        optimizer_args: Optional[Dict] = None,
        config: Optional[Dict[str, Any]] = None,
        torch_config: Optional[TorchConfig] = None,
        scaling_config: Optional[ScalingConfig] = None,
        run_config: Optional[RunConfig] = None,
        datasets: Optional[Dict[str, GenDataset]] = None,
        preprocessor: Optional[Preprocessor] = None,
        resume_from_checkpoint: Optional[Checkpoint] = None,
    ):
        config = config or {}
        model_args = model_args or {}
        loss_fn_args = loss_fn_args or {}
        optimizer_args = optimizer_args or {}

        def train_loop_per_worker(train_config):
            ots_config = TorchOffTheShelfConfig(**train_config)
            ots_config.validation_fn = ots_config.validation_fn or default_validation_fn
            train_dataset_shard = train.get_dataset_shard("train")

            try:
                validation_dataset_shard = train.get_dataset_shard("validation")
            except KeyError:
                validation_dataset_shard = None

            model = model_cls(**model_args)
            loss_fn = loss_fn_cls(**loss_fn_args)
            optimizer = optimizer_cls(model.parameters(), **optimizer_args)

            model = train.torch.prepare_model(model)

            for epoch in range(ots_config.num_epochs):
                for batch, (x, y) in enumerate(
                    train_dataset_shard.to_torch(batch_size=ots_config.batch_size)
                ):
                    output = model(x)
                    loss = loss_fn(output, y)
                    optimizer.zero_grad()
                    loss.backward()
                    optimizer.step()

                    if validation_dataset_shard:
                        # Do not validate on first batch if frequency > 1
                        if (batch + 1) % (ots_config.validation_frequency + 1) == 0:
                            validation_result = {}
                            for vx, vy in validation_dataset_shard.to_torch():
                                prediction = model(vx)
                                test_loss = loss_fn(prediction, vy)
                                validation_result = ots_config.validation_fn(
                                    validation_result, prediction, vy, test_loss
                                )
                            train.report(loss=loss, **validation_result)
                    else:
                        train.report(loss=loss)

                train.save_checkpoint(model=model.state_dict())

        super().__init__(
            train_loop_per_worker=train_loop_per_worker,
            train_loop_config=config,
            torch_config=torch_config,
            scaling_config=scaling_config,
            run_config=run_config,
            datasets=datasets,
            preprocessor=preprocessor,
            resume_from_checkpoint=resume_from_checkpoint,
        )
