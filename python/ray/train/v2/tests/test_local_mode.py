import math
import os
import sys
from unittest.mock import MagicMock, patch

import lightgbm
import pandas as pd
import pytest
import xgboost
from datasets import Dataset
from sklearn.datasets import load_breast_cancer
from sklearn.model_selection import train_test_split
from transformers import AutoConfig, AutoModelForCausalLM, Trainer, TrainingArguments

import ray
from ray.data.preprocessors import Concatenator
from ray.tests.conftest import _ray_start_cluster
from ray.train import ScalingConfig
from ray.train.constants import TRAIN_DATASET_KEY
from ray.train.examples.pytorch.torch_linear_example import (
    train_func as linear_train_func,
)
from ray.train.huggingface.transformers import (
    RayTrainReportCallback as HuggingFaceRayTrainReportCallback,
    prepare_trainer,
)
from ray.train.lightgbm import (
    LightGBMTrainer,
    RayTrainReportCallback as LightGBMRayTrainReportCallback,
)
from ray.train.lightning import (
    RayDDPStrategy,
    RayFSDPStrategy,
    RayLightningEnvironment,
    RayTrainReportCallback as LightningRayTrainReportCallback,
)
from ray.train.lightning._lightning_utils import import_lightning
from ray.train.tests._huggingface_data import train_data, validation_data
from ray.train.tests.lightning_test_utils import DummyDataModule, LinearModule
from ray.train.tests.util import create_dict_checkpoint
from ray.train.torch import TorchTrainer
from ray.train.v2._internal.execution.local_mode.torch import LocalTorchController
from ray.train.v2._internal.execution.train_fn_utils import get_train_fn_utils
from ray.train.v2.api.data_parallel_trainer import DataParallelTrainer
from ray.train.v2.jax import JaxTrainer
from ray.train.xgboost import (
    RayTrainReportCallback as XGBoostRayTrainReportCallback,
    XGBoostTrainer,
)

if sys.version_info >= (3, 12):
    # Tensorflow is not installed for Python 3.12 because of keras compatibility.
    pass
else:
    from ray.train.examples.tf.tensorflow_regression_example import (
        train_func as tensorflow_linear_train_func,
    )
    from ray.train.tensorflow import TensorflowTrainer

pl = import_lightning()


@pytest.fixture
def ray_start_6_cpus():
    address_info = ray.init(num_cpus=6)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


@pytest.fixture
def ray_tpu_single_host(monkeypatch):
    """Start a mock single-host TPU Ray cluster with 2x4 v6e (8 chips per host)."""
    with _ray_start_cluster() as cluster:
        monkeypatch.setenv("TPU_ACCELERATOR_TYPE", "v6e-8")

        # Simulate one node with 8 TPU chips.
        cluster.add_node(
            num_cpus=4,
            resources={"TPU": 8},
        )

        ray.init(address=cluster.address)

        yield cluster
        ray.shutdown()


def test_data_parallel_trainer_local_mode():
    def train_fn():
        with create_dict_checkpoint({}) as checkpoint:
            ray.train.report(metrics={"test": 1}, checkpoint=checkpoint)

    trainer = DataParallelTrainer(train_fn, scaling_config=ScalingConfig(num_workers=0))
    result = trainer.fit()
    assert result.metrics == {"test": 1}
    assert result.checkpoint


def test_jax_trainer_local_mode(ray_tpu_single_host, monkeypatch):
    def jax_train_func():
        import jax

        devices = jax.devices()
        print(f"Devices on this worker: {devices}")
        ray.train.report({"result": [str(d) for d in devices]})

    mock_jax = MagicMock()
    mock_jax.devices.return_value = ["TPU:0"]
    monkeypatch.setitem(sys.modules, "jax", mock_jax)

    trainer = JaxTrainer(
        train_loop_per_worker=jax_train_func,
        scaling_config=ScalingConfig(
            num_workers=0,
        ),
    )
    result = trainer.fit()
    assert result.error is None
    assert result.metrics == {"result": ["TPU:0"]}


def test_lightgbm_trainer_local_mode(ray_start_6_cpus):
    def lightgbm_train_fn_per_worker(
        config: dict,
        label_column: str,
        dataset_keys: set,
        num_boost_round: int = 10,
    ):
        remaining_iters = num_boost_round
        train_ds_iter = ray.train.get_dataset_shard(TRAIN_DATASET_KEY)
        train_df = train_ds_iter.materialize().to_pandas()

        eval_ds_iters = {
            k: ray.train.get_dataset_shard(k)
            for k in dataset_keys
            if k != TRAIN_DATASET_KEY
        }
        eval_dfs = {k: d.materialize().to_pandas() for k, d in eval_ds_iters.items()}

        train_X, train_y = train_df.drop(label_column, axis=1), train_df[label_column]
        train_set = lightgbm.Dataset(train_X, label=train_y)

        # NOTE: Include the training dataset in the evaluation datasets.
        # This allows `train-*` metrics to be calculated and reported.
        valid_sets = [train_set]
        valid_names = [TRAIN_DATASET_KEY]

        for eval_name, eval_df in eval_dfs.items():
            eval_X, eval_y = eval_df.drop(label_column, axis=1), eval_df[label_column]
            valid_sets.append(lightgbm.Dataset(eval_X, label=eval_y))
            valid_names.append(eval_name)

        # Add network params of the worker group to enable distributed training.
        config.update(ray.train.lightgbm.get_network_params())

        lightgbm.train(
            params=config,
            train_set=train_set,
            num_boost_round=remaining_iters,
            valid_sets=valid_sets,
            valid_names=valid_names,
            init_model=None,
            callbacks=[LightGBMRayTrainReportCallback()],
        )

    data_raw = load_breast_cancer()
    dataset_df = pd.DataFrame(data_raw["data"], columns=data_raw["feature_names"])
    dataset_df["target"] = data_raw["target"]
    train_df, test_df = train_test_split(dataset_df, test_size=0.3)

    train_df_with_cat = train_df.copy()
    test_df_with_cat = test_df.copy()
    dataset_shard_size = 1
    train_df_with_cat["categorical_column"] = pd.Series(
        (["A", "B"] * math.ceil(len(train_df_with_cat) / dataset_shard_size))[
            : len(train_df_with_cat)
        ]
    ).astype("category")
    test_df_with_cat["categorical_column"] = pd.Series(
        (["A", "B"] * math.ceil(len(test_df_with_cat) / dataset_shard_size))[
            : len(test_df_with_cat)
        ]
    ).astype("category")

    scale_config = ScalingConfig(num_workers=0)
    train_dataset = ray.data.from_pandas(train_df_with_cat)
    valid_dataset = ray.data.from_pandas(test_df_with_cat)
    trainer = LightGBMTrainer(
        train_loop_per_worker=lambda: lightgbm_train_fn_per_worker(
            config={},
            label_column="target",
            dataset_keys={TRAIN_DATASET_KEY, "valid"},
        ),
        train_loop_config={
            "objective": "binary",
            "metric": ["binary_logloss", "binary_error"],
        },
        scaling_config=scale_config,
        datasets={TRAIN_DATASET_KEY: train_dataset, "valid": valid_dataset},
    )
    result = trainer.fit()
    checkpoint = result.checkpoint
    assert checkpoint is not None


@pytest.mark.parametrize("datasource", ["dataloader", "datamodule"])
def test_lightning_trainer_local_mode(ray_start_6_cpus, datasource):

    num_epochs = 1
    batch_size = 8
    dataset_size = 256
    dataset_shard_size = 1
    strategy_name = "ddp"
    accelerator = "cpu"

    strategy_map = {"ddp": RayDDPStrategy(), "fsdp": RayFSDPStrategy()}

    def train_loop():
        model = LinearModule(input_dim=32, output_dim=4, strategy=strategy_name)

        strategy = strategy_map[strategy_name]

        trainer = pl.Trainer(
            max_epochs=num_epochs,
            devices="auto",
            accelerator=accelerator,
            strategy=strategy,
            plugins=[RayLightningEnvironment()],
            callbacks=[LightningRayTrainReportCallback()],
        )

        datamodule = DummyDataModule(batch_size, dataset_size)

        if datasource == "dataloader":
            trainer.fit(
                model,
                train_dataloaders=datamodule.train_dataloader(),
                val_dataloaders=datamodule.val_dataloader(),
            )
        if datasource == "datamodule":
            trainer.fit(model, datamodule=datamodule)

    trainer = TorchTrainer(
        train_loop_per_worker=train_loop,
        scaling_config=ScalingConfig(num_workers=0, use_gpu=(accelerator == "gpu")),
    )

    results = trainer.fit()
    assert results.metrics["epoch"] == num_epochs - 1
    assert (
        results.metrics["step"]
        == num_epochs * dataset_size / dataset_shard_size / batch_size
    )
    assert "loss" in results.metrics
    assert "val_loss" in results.metrics


@pytest.mark.skipif(
    sys.version_info >= (3, 12),
    reason="Tensorflow is not installed for Python 3.12 because of keras compatibility.",
)
def test_tensorflow_linear_local_mode(ray_start_4_cpus):
    """Also tests air Keras callback."""
    epochs = 1

    def train_func(config):
        result = tensorflow_linear_train_func(config)
        assert len(result) == epochs

    train_loop_config = {
        "lr": 1e-3,
        "batch_size": 32,
        "epochs": epochs,
    }
    scaling_config = ScalingConfig(num_workers=0)
    dataset = ray.data.read_csv("s3://anonymous@air-example-data/regression.csv")
    columns_to_concatenate = [f"x{i:03}" for i in range(100)]
    preprocessor = Concatenator(columns=columns_to_concatenate, output_column_name="x")
    dataset = preprocessor.transform(dataset)

    trainer = TensorflowTrainer(
        train_loop_per_worker=train_func,
        train_loop_config=train_loop_config,
        scaling_config=scaling_config,
        datasets={TRAIN_DATASET_KEY: dataset},
    )
    result = trainer.fit()
    assert not result.error
    assert result.checkpoint


def test_torch_trainer_local_mode(ray_start_6_cpus):
    def train_func(config):
        result = linear_train_func(config)
        assert len(result) == epochs
        assert result[-1]["loss"] < result[0]["loss"]

    epochs = 3
    scaling_config = ScalingConfig(num_workers=0)
    config = {"lr": 1e-2, "hidden_size": 1, "batch_size": 4, "epochs": epochs}
    trainer = TorchTrainer(
        train_loop_per_worker=train_func,
        train_loop_config=config,
        scaling_config=scaling_config,
    )
    result = trainer.fit()
    assert result.error is None
    assert result.metrics is not None
    assert result.metrics["loss"] is not None
    assert result.checkpoint


HF_BATCH_SIZE_PER_WORKER = 2
HF_MODEL_NAME = "hf-internal-testing/tiny-random-BloomForCausalLM"
HF_MAX_EPOCHS = 1
HF_TRAIN_DATASET_SIZE = 16


@pytest.mark.parametrize("use_ray_data", [False, True])
def test_e2e_hf_local_mode(ray_start_4_cpus, use_ray_data):
    def get_transformers_configurations():
        """Get configurations with dynamic step calculations based on number of workers."""
        steps_per_epoch = HF_TRAIN_DATASET_SIZE // HF_BATCH_SIZE_PER_WORKER
        return {
            "epoch_gpu": {
                "evaluation_strategy": "epoch",
                "save_strategy": "epoch",
                "logging_strategy": "epoch",
                "eval_steps": None,
                "save_steps": None,
                "logging_steps": None,
                "no_cuda": False,
            },
            "steps_gpu": {
                "evaluation_strategy": "steps",
                "save_strategy": "steps",
                "logging_strategy": "steps",
                "eval_steps": steps_per_epoch,
                "save_steps": steps_per_epoch * 2,
                "logging_steps": 1,
                "no_cuda": False,
            },
            "steps_cpu": {
                "evaluation_strategy": "steps",
                "save_strategy": "steps",
                "logging_strategy": "steps",
                "eval_steps": steps_per_epoch,
                "save_steps": steps_per_epoch,
                "logging_steps": 1,
                "no_cuda": True,
            },
            "steps_cpu_local": {
                "evaluation_strategy": "steps",
                "save_strategy": "steps",
                "logging_strategy": "steps",
                "eval_steps": steps_per_epoch,
                "save_steps": steps_per_epoch,
                "logging_steps": 1,
                "no_cuda": True,
            },
        }

    config_id = "steps_cpu_local"
    num_workers = 0

    def train_func(config):
        # Datasets
        if config["use_ray_data"]:
            train_ds_shard = ray.train.get_dataset_shard("train")
            eval_ds_shard = ray.train.get_dataset_shard("eval")

            train_dataset = train_ds_shard.iter_torch_batches(
                batch_size=HF_BATCH_SIZE_PER_WORKER
            )
            eval_dataset = eval_ds_shard.iter_torch_batches(
                batch_size=HF_BATCH_SIZE_PER_WORKER
            )
        else:
            train_df = pd.read_json(train_data)
            validation_df = pd.read_json(validation_data)

            train_dataset = Dataset.from_pandas(train_df)
            eval_dataset = Dataset.from_pandas(validation_df)

        # Model
        model_config = AutoConfig.from_pretrained(HF_MODEL_NAME)
        model = AutoModelForCausalLM.from_config(model_config)

        # HF Transformers Trainer
        training_args = TrainingArguments(
            f"{HF_MODEL_NAME}-wikitext2",
            evaluation_strategy=config["evaluation_strategy"],
            logging_strategy=config["logging_strategy"],
            save_strategy=config["save_strategy"],
            eval_steps=config["eval_steps"],
            save_steps=config["save_steps"],
            logging_steps=config["logging_steps"],
            num_train_epochs=config.get("num_train_epochs", HF_MAX_EPOCHS),
            max_steps=config.get("max_steps", -1),
            learning_rate=config.get("learning_rate", 2e-5),
            per_device_train_batch_size=HF_BATCH_SIZE_PER_WORKER,
            per_device_eval_batch_size=HF_BATCH_SIZE_PER_WORKER,
            weight_decay=0.01,
            disable_tqdm=True,
            no_cuda=config["no_cuda"],
            report_to="none",
        )
        trainer = Trainer(
            model=model,
            args=training_args,
            train_dataset=train_dataset,
            eval_dataset=eval_dataset,
        )

        # Report to Ray Train
        trainer.add_callback(HuggingFaceRayTrainReportCallback())
        trainer = prepare_trainer(trainer)

        # Start Training
        trainer.train()

    configurations = get_transformers_configurations()
    train_loop_config = configurations[config_id]

    # Calculate the num of Ray training iterations
    max_steps = HF_MAX_EPOCHS * HF_TRAIN_DATASET_SIZE // HF_BATCH_SIZE_PER_WORKER

    train_loop_config["use_ray_data"] = use_ray_data

    datasets = None
    if use_ray_data:
        # Must specify `max_steps` for Iterable Dataset
        train_loop_config["max_steps"] = max_steps

        train_df = pd.read_json(train_data)
        validation_df = pd.read_json(validation_data)

        ray_train_ds = ray.data.from_pandas(train_df)
        ray_eval_ds = ray.data.from_pandas(validation_df)
        datasets = {"train": ray_train_ds, "eval": ray_eval_ds}
    else:
        # Specify `num_train_epochs` for Map-style Dataset
        train_loop_config["num_train_epochs"] = HF_MAX_EPOCHS

    use_gpu = not train_loop_config["no_cuda"]

    trainer = TorchTrainer(
        train_func,
        train_loop_config=train_loop_config,
        scaling_config=ScalingConfig(num_workers=num_workers, use_gpu=use_gpu),
        datasets=datasets,
    )
    result = trainer.fit()

    assert result.metrics["step"] == max_steps
    assert "eval_loss" in result.metrics
    if not use_ray_data:
        assert result.metrics["epoch"] == HF_MAX_EPOCHS


def test_xgboost_trainer_local_mode(ray_start_4_cpus):
    def xgboost_train_fn_per_worker():
        label_column = "target"
        dataset_keys = {TRAIN_DATASET_KEY, "valid"}
        checkpoint = ray.train.get_checkpoint()
        starting_model = None
        remaining_iters = 10
        if checkpoint:
            starting_model = XGBoostRayTrainReportCallback.get_model(checkpoint)
            starting_iter = starting_model.num_boosted_rounds()
            remaining_iters = remaining_iters - starting_iter

        train_ds_iter = ray.train.get_dataset_shard(TRAIN_DATASET_KEY)
        train_df = train_ds_iter.materialize().to_pandas()

        eval_ds_iters = {
            k: ray.train.get_dataset_shard(k)
            for k in dataset_keys
            if k != TRAIN_DATASET_KEY
        }
        eval_dfs = {k: d.materialize().to_pandas() for k, d in eval_ds_iters.items()}

        train_X, train_y = train_df.drop(label_column, axis=1), train_df[label_column]
        dtrain = xgboost.DMatrix(train_X, label=train_y)

        # NOTE: Include the training dataset in the evaluation datasets.
        # This allows `train-*` metrics to be calculated and reported.
        evals = [(dtrain, TRAIN_DATASET_KEY)]

        for eval_name, eval_df in eval_dfs.items():
            eval_X, eval_y = eval_df.drop(label_column, axis=1), eval_df[label_column]
            evals.append((xgboost.DMatrix(eval_X, label=eval_y), eval_name))

        evals_result = {}
        xgboost.train(
            {},
            dtrain=dtrain,
            evals=evals,
            evals_result=evals_result,
            num_boost_round=remaining_iters,
            xgb_model=starting_model,
        )

    data_raw = load_breast_cancer()
    dataset_df = pd.DataFrame(data_raw["data"], columns=data_raw["feature_names"])
    dataset_df["target"] = data_raw["target"]
    train_df, test_df = train_test_split(dataset_df, test_size=0.3)

    train_dataset = ray.data.from_pandas(train_df)
    valid_dataset = ray.data.from_pandas(test_df)
    scale_config = ScalingConfig(num_workers=0)
    trainer = XGBoostTrainer(
        train_loop_per_worker=xgboost_train_fn_per_worker,
        train_loop_config={
            "tree_method": "approx",
            "objective": "binary:logistic",
            "eval_metric": ["logloss", "error"],
        },
        scaling_config=scale_config,
        datasets={TRAIN_DATASET_KEY: train_dataset, "valid": valid_dataset},
    )
    result = trainer.fit()
    with pytest.raises(DeprecationWarning):
        XGBoostTrainer.get_model(result.checkpoint)


def test_torch_distributed_variables_local_train_fn_utils():
    """Test that torch distributed variables are correctly used to create LocalTrainFnUtils."""

    # Test scenario 1: Without torch distributed environment variables
    with patch.dict(os.environ, {}, clear=True):
        controller = LocalTorchController("test_experiment")

        def dummy_train_func():
            train_fn_utils = get_train_fn_utils()
            # Verify default values when no torch distributed env vars are set
            context = train_fn_utils.get_context()
            assert context.get_world_size() == 1
            assert context.get_world_rank() == 0
            assert context.get_local_rank() == 0
            assert context.get_local_world_size() == 1
            assert context.get_node_rank() == 0

        controller.run(dummy_train_func)

    # Test scenario 2: With torch distributed environment variables (CPU)
    torch_env_vars = {
        "RANK": "2",
        "LOCAL_RANK": "1",
        "WORLD_SIZE": "4",
        "LOCAL_WORLD_SIZE": "2",
        "MASTER_ADDR": "127.0.0.1",
        "MASTER_PORT": "29500",
    }

    with patch.dict(os.environ, torch_env_vars, clear=True), patch(
        "torch.distributed.is_initialized", return_value=False
    ), patch("torch.distributed.get_world_size", return_value=4), patch(
        "torch.distributed.get_rank", return_value=2
    ), patch(
        "torch.cuda.is_available", return_value=False
    ), patch(
        "torch.distributed.init_process_group"
    ) as mock_init_pg:

        controller = LocalTorchController("test_experiment")

        def dummy_train_func():
            train_fn_utils = get_train_fn_utils()
            # Verify torch distributed values are correctly passed
            context = train_fn_utils.get_context()
            assert context.get_world_size() == 4
            assert context.get_world_rank() == 2
            assert context.get_local_rank() == 1
            assert context.get_local_world_size() == 2
            assert (
                context.get_node_rank() == 1
            )  # global_rank // nproc_per_node = 2 // 2 = 1

        controller.run(dummy_train_func)

        # Verify torch.distributed methods were called with CPU backend
        mock_init_pg.assert_called_once_with(backend="gloo")

    # Test scenario 3: With torch distributed environment variables (GPU)
    with patch.dict(os.environ, torch_env_vars, clear=True), patch(
        "torch.distributed.is_initialized", return_value=False
    ), patch("torch.distributed.get_world_size", return_value=4), patch(
        "torch.distributed.get_rank", return_value=2
    ), patch(
        "torch.cuda.is_available", return_value=True
    ), patch(
        "torch.distributed.init_process_group"
    ) as mock_init_pg, patch(
        "torch.cuda.set_device"
    ) as mock_set_device:

        controller = LocalTorchController("test_experiment")

        def dummy_train_func():
            train_fn_utils = get_train_fn_utils()
            # Verify torch distributed values are correctly passed
            context = train_fn_utils.get_context()
            assert context.get_world_size() == 4
            assert context.get_world_rank() == 2
            assert context.get_local_rank() == 1
            assert context.get_local_world_size() == 2
            assert context.get_node_rank() == 1

        controller.run(dummy_train_func)

        mock_init_pg.assert_called_once_with(backend="nccl")
        mock_set_device.assert_called_once_with(1)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-x", __file__]))
