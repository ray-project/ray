from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional, Type, Iterable, Union

import torch
from ray import train
from ray.ml import ScalingConfig, RunConfig, Preprocessor, Checkpoint
from ray.ml.train.integrations.torch import TorchTrainer
from ray.ml.trainer import GenDataset
from ray.train.torch import TorchConfig
from torch import nn, optim


@dataclass
class TorchOffTheShelfConfig:
    train_dataset_name: str = "train"
    batch_size: int = 32
    num_epochs: int = 10

    validation_dataset_name: str = "validate"
    validation_frequency: int = 1
    validation_fn: Optional[
        Callable[[Dict, int, torch.Tensor, torch.Tensor, torch.Tensor], Dict]
    ] = None


def default_validation_fn(
    validation_results: Dict,
    batch: int,
    prediction: torch.Tensor,
    labels: torch.Tensor,
    loss: torch.Tensor,
):
    """Default validation function that keeps an average of the validation loss."""
    old_loss = validation_results.get("validation_loss", 0.0)
    new_loss = (old_loss * batch + loss.item()) / (batch + 1)
    validation_results["validation_loss"] = new_loss
    return validation_results


class TorchOffTheShelfTrainer(TorchTrainer):
    """Torch trainer for a simple train-validation loop.

    This trainer implements a simple train-validation loop based on PyTorch.
    It can be used as an off-the-shelf implementation for simple learning tasks.
    You can configure a model, and optimizer, and a loss function.

    The pseudo code for this trainer is:

    .. code-block:: python

        for x, y in training_dataset:
            output = model(x)
            loss = loss_fn(output, y)
            loss.backward()
            optimizer.step()

            maybe_validate_on(validation_dataset)


    Args:
        model_cls: Model class. This will be instantiated with the ``model_args``.
        model_args: Arguments to pass to model class on initialization.
        loss_fn_cls: Loss function class.
        loss_fn_args: Arguments to pass to loss function class on initialization.
        optimizer_cls: Optimizer class.
        optimizer_args: Arguments to pass to optimizer on initialization.
        label_column: Column name for the labels in the Ray datasets.
        feature_columns: Column names for the features in the Ray datasets.
        config: Configuration for the training and validation behavior of the
            off-the-shelf trainer. See ``TorchOffTheShelfConfig``.
        torch_config: Configuration for setting up the PyTorch backend. If set to
            None, use the default configuration. This replaces the ``backend_config``
            arg of ``DataParallelTrainer``.
        scaling_config: Configuration for how to scale data parallel training.
        run_config: Configuration for the execution of the training run.
        datasets: Any Ray Datasets to use for training. Use
            the key "train" to denote which dataset is the training
            dataset. If a ``preprocessor`` is provided and has not already been fit,
            it will be fit on the training dataset. All datasets will be transformed
            by the ``preprocessor`` if one is provided.
        preprocessor: A ``ray.ml.preprocessor.Preprocessor`` to preprocess the
            provided datasets.
        resume_from_checkpoint: A checkpoint to resume training from.

    Example:

        ..code-block:: python

            from sklearn.datasets import make_regression
            from torch import nn, optim

            import ray
            from ray.ml.train.integrations.torch.torch_ots import TorchOffTheShelfTrainer


            def get_dataset() -> ray.data.Dataset:
                features, labels = make_regression(
                    n_samples=10_000, n_features=100, n_informative=2
                )
                return ray.data.from_items(
                    [{"x": x, "y": y} for x, y in zip(features, labels)])


            trainer = TorchOffTheShelfTrainer(
                model_cls=nn.Linear,
                model_args={"in_features": 100, "out_features": 1},
                loss_fn_cls=nn.MSELoss,
                optimizer_cls=optim.SGD,
                optimizer_args={"lr": 0.05},
                scaling_config={"num_workers": 2},
                datasets={"train": lambda: get_dataset()},
            )
            trainer.fit()

    """

    def __init__(
        self,
        *,
        model_cls: Optional[Type[nn.Module]],
        model_args: Optional[Dict] = None,
        loss_fn_cls: Optional[Type[nn.Module]] = nn.MSELoss,
        loss_fn_args: Optional[Dict] = None,
        optimizer_cls: Optional[Type[optim.Optimizer]] = optim.SGD,
        optimizer_args: Optional[Dict] = None,
        label_column: str = "y",
        label_column_dtype: torch.dtype = torch.float,
        feature_columns: Iterable[str] = ("x",),
        feature_column_dtypes: torch.dtype = torch.float,
        config: Optional[Union[TorchOffTheShelfConfig, Dict[str, Any]]] = None,
        torch_config: Optional[TorchConfig] = None,
        scaling_config: Optional[ScalingConfig] = None,
        run_config: Optional[RunConfig] = None,
        datasets: Optional[Dict[str, GenDataset]] = None,
        preprocessor: Optional[Preprocessor] = None,
        resume_from_checkpoint: Optional[Checkpoint] = None,
    ):
        if config and isinstance(config, TorchOffTheShelfConfig):
            config = config.__dict__
        else:
            config = config or {}

        model_args = model_args or {}
        loss_fn_args = loss_fn_args or {}
        optimizer_args = optimizer_args or {}

        def train_loop_per_worker(train_config):
            ots_config = TorchOffTheShelfConfig(**train_config)

            validation_fn = ots_config.validation_fn or default_validation_fn
            train_dataset_shard = train.get_dataset_shard(ots_config.train_dataset_name)

            try:
                validation_dataset_shard = train.get_dataset_shard(
                    ots_config.validation_dataset_name
                )
            except KeyError:
                validation_dataset_shard = None

            model = model_cls(**model_args)
            loss_fn = loss_fn_cls(**loss_fn_args)
            optimizer = optimizer_cls(model.parameters(), **optimizer_args)

            model = train.torch.prepare_model(model)

            for epoch in range(ots_config.num_epochs):
                for batch, (x, y) in enumerate(
                    train_dataset_shard.to_torch(
                        label_column=label_column,
                        feature_columns=list(feature_columns),
                        batch_size=ots_config.batch_size,
                        label_column_dtype=label_column_dtype,
                        feature_column_dtypes=feature_column_dtypes,
                    )
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
                            for vbatch, (vx, vy) in enumerate(
                                validation_dataset_shard.to_torch(
                                    label_column=label_column,
                                    feature_columns=list(feature_columns),
                                    batch_size=ots_config.batch_size,
                                    label_column_dtype=label_column_dtype,
                                    feature_column_dtypes=feature_column_dtypes,
                                )
                            ):
                                prediction = model(vx)
                                test_loss = loss_fn(prediction, vy)
                                validation_result = validation_fn(
                                    validation_result, vbatch, prediction, vy, test_loss
                                )
                            train.report(loss=loss.item(), **validation_result)
                    else:
                        train.report(loss=loss.item())

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
