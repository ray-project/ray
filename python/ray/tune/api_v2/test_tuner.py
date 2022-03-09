import copy
from typing import Callable, Dict, List, Type, Union

from ray import tune
from ray.data import Dataset
from ray.train.trainer import wrap_function
from ray.tune.utils.placement_groups import PlacementGroupFactory
from ray.tune.trainable import Trainable
from ray.tune.integration.xgboost import TuneReportCheckpointCallback
from ray.tune.api_v2.convertible_to_trainable import ConvertibleToTrainable
from ray.tune.tuner import Tuner

from dataclasses import dataclass

import xgboost_ray
from xgboost_ray.tune import _get_tune_resources

# Some tests to drive the new API code path. Will be removed once other parts are in.

import abc
import os
import pickle
import tarfile
import tempfile
from typing import Any, Optional

import xgboost
import shutil
import ray

from sklearn.datasets import load_breast_cancer

import pandas as pd
import numpy as np

DataBatchType = Union[pd.DataFrame, np.ndarray]


class Preprocessor(abc.ABC):
    def fit(self, dataset: ray.data.Dataset) -> "Preprocessor":
        raise NotImplementedError

    def transform(self, dataset: ray.data.Dataset) -> ray.data.Dataset:
        raise NotImplementedError

    def fit_transform(self, dataset: ray.data.Dataset) -> ray.data.Dataset:
        # Todo: optimize
        self.fit(dataset)
        return self.transform(dataset)


class Scaler(Preprocessor):
    def __init__(self, columns: List[str]):
        self.columns = columns
        self.stats = None

    def fit(self, dataset: ray.data.Dataset) -> "Preprocessor":
        aggregates = [Agg(col) for Agg in [Mean, Std] for col in self.columns]
        self.stats = dataset.aggregate(*aggregates)
        return self

    def transform(self, dataset: ray.data.Dataset) -> ray.data.Dataset:
        columns = self.columns
        stats = self.stats

        def _scale(df: pd.DataFrame):
            def column_standard_scaler(s: pd.Series):
                s_mean = stats[f"mean({s.name})"]
                s_std = stats[f"std({s.name})"]
                return (s - s_mean) / s_std

            df.loc[:, columns] = df.loc[:, columns].transform(column_standard_scaler)
            return df

        return dataset.map_batches(_scale, batch_format="pandas")

    def __repr__(self):
        return f"<Scaler columns={self.columns} stats={self.stats}>"


class Repartitioner(Preprocessor):
    def __init__(self, num_partitions: int):
        self.num_partitions = num_partitions

    def fit(self, dataset: ray.data.Dataset) -> "Preprocessor":
        return self

    def transform(self, dataset: ray.data.Dataset) -> ray.data.Dataset:
        return dataset.repartition(num_blocks=self.num_partitions)

    def __repr__(self):
        return f"<Repartitioner num_partitions={self.num_partitions}>"


class Chain(Preprocessor):
    def __init__(self, *preprocessors):
        self.preprocessors = preprocessors

    def fit(self, dataset: ray.data.Dataset) -> "Preprocessor":
        for preprocessor in self.preprocessors:
            preprocessor.fit(dataset)
        return self

    def transform(self, dataset: ray.data.Dataset) -> ray.data.Dataset:
        for preprocessor in self.preprocessors:
            dataset = preprocessor.transform(dataset)
        return dataset

    def __repr__(self):
        return (
            f"<Chain preprocessors=["
            f"{', '.join(str(p) for p in self.preprocessors)}"
            f"]>"
        )


def _pack(dir: str):
    _, tmpfile = tempfile.mkstemp()
    with tarfile.open(tmpfile, "w:gz") as tar:
        tar.add(dir, arcname="")

    with open(tmpfile, "rb") as f:
        stream = f.read()

    return stream


def _unpack(stream: bytes, dir: str):
    _, tmpfile = tempfile.mkstemp()

    with open(tmpfile, "wb") as f:
        f.write(stream)

    with tarfile.open(tmpfile) as tar:
        tar.extractall(dir)


class ArtifactData:
    def __init__(self, data: Any):
        self.data = data


class ArtifactDirectory(ArtifactData):
    pass


class ArtifactFile(ArtifactData):
    pass


class ArtifactObject(ArtifactData):
    pass


class Artifact(abc.ABC):
    """Artifact interface"""

    pass


class ObjectStoreArtifact(Artifact):
    def __init__(self, obj_ref: ray.ObjectRef):
        self.obj_ref = obj_ref

    def _to_local_storage(self, path: str) -> "LocalStorageArtifact":
        return LocalStorageArtifact(path=path)

    def to_local_storage(self, path: str) -> "LocalStorageArtifact":
        data = ray.get(self.obj_ref)
        with open(path, "wb") as fp:
            pickle.dump(data, fp)
        return self._to_local_storage(path)


class LocalStorageArtifact(Artifact):
    def __init__(self, path: str):
        self.path = path

    def _to_object_store(self, obj_ref: ray.ObjectRef) -> "ObjectStoreArtifact":
        return ObjectStoreArtifact(obj_ref=obj_ref)

    def to_object_store(self) -> "ObjectStoreArtifact":
        if os.path.isdir(self.path):
            data = ArtifactDirectory(_pack(self.path))
        else:
            with open(self.path, "r") as fp:
                data = ArtifactFile(fp.read())
        return self._to_object_store(ray.put(data))


class Checkpoint(abc.ABC):
    def load_preprocessor(self, **options) -> Preprocessor:
        """Returns fitted preprocessor."""
        raise NotImplementedError

    def as_callable_class(self, **options):
        raise NotImplementedError


class ObjectStoreCheckpoint(Checkpoint):
    def __init__(self, obj_ref: ray.ObjectRef):
        self.obj_ref = obj_ref

    def _to_local_storage(self, path: str) -> "LocalStorageCheckpoint":
        return LocalStorageCheckpoint(path=path)

    def to_local_storage(self, path: Optional[str] = None) -> "LocalStorageCheckpoint":
        if path is None:
            path = tempfile.mktemp()
        data = ray.get(self.obj_ref)
        if isinstance(data, ArtifactDirectory):
            _unpack(data.data, path)
        elif isinstance(data, ArtifactFile):
            with open(path, "wb") as fp:
                pickle.dump(data.data, fp)
        else:
            with open(path, "wb") as fp:
                pickle.dump(data, fp)
        return self._to_local_storage(path)

    def __repr__(self):
        return f"<ObjectStoreCheckpoint obj_ref={self.obj_ref}>"

    def __getstate__(self):
        state = self.__dict__.copy()
        obj_ref = state.pop("obj_ref", None)
        if obj_ref:
            data = ray.get(obj_ref)
        else:
            data = None

        state["_data"] = data
        return state

    def __setstate__(self, state):
        data = state.pop("_data", None)
        self.__dict__.update(state)

        if data:
            self.obj_ref = ray.put(data)


class LocalStorageCheckpoint(Checkpoint):
    def __init__(self, path: str):
        self.path = path

    def _to_object_store(self, obj_ref: ray.ObjectRef) -> "ObjectStoreCheckpoint":
        return ObjectStoreCheckpoint(obj_ref=obj_ref)

    def to_object_store(self) -> "ObjectStoreCheckpoint":
        if os.path.isdir(self.path):
            data = ArtifactDirectory(_pack(self.path))
        else:
            with open(self.path, "r") as fp:
                data = ArtifactFile(fp.read())
        return self._to_object_store(obj_ref=ray.put(data))

    def __repr__(self):
        return f"<LocalStorageCheckpoint path={self.path}>"


import abc

import numpy as np
import pandas as pd

import ray.data
from ray.data.aggregate import Mean, Std


def test_transform_scaler():
    """Scale B and C, but not A"""
    num_items = 1_000
    col_a = np.random.normal(loc=4.0, scale=0.4, size=num_items)
    col_b = np.random.normal(loc=8.0, scale=0.7, size=num_items)
    col_c = np.random.normal(loc=7.0, scale=0.3, size=num_items)
    in_df = pd.DataFrame.from_dict({"A": col_a, "B": col_b, "C": col_c})

    ds = ray.data.from_pandas(in_df)

    scaler = Scaler(["B", "C"])
    scaler.fit(ds)
    transformed = scaler.transform(ds)
    out_df = transformed.to_pandas()

    assert in_df["A"].equals(out_df["A"])
    assert not in_df["B"].equals(out_df["B"])
    assert not in_df["C"].equals(out_df["C"])

    print(in_df)
    print(out_df)


# num_workers, gpu, etc.
@dataclass
class ScalingConfig:
    pass


# checkpoint_dir, etc.
RunConfig = Dict[str, Any]

# dataset / dataset factory
GenDataset = Union[Dataset, Callable[[], Dataset]]


class Trainer(ConvertibleToTrainable, abc.ABC):
    def __init__(
        self,
        scaling_config: Optional[ScalingConfig] = None,
        run_config: Optional[RunConfig] = None,
        resume_from_checkpoint: Optional[Checkpoint] = None,
        **kwargs,
    ):
        self.scaling_config = scaling_config
        self.run_config = run_config
        self.resume_from_checkpoint = resume_from_checkpoint
        self.kwargs = kwargs

    def fit(self, dataset: ray.data.Dataset, preprocessor: Preprocessor):
        raise NotImplementedError


class FunctionTrainer(Trainer, abc.ABC):
    def train_fn(
        self,
        run_config: RunConfig,
        scaling_config: ScalingConfig,
        datasets: Dict[str, ray.data.Dataset],
        checkpoint: Optional[Checkpoint],
        **kwargs,
    ) -> Any:
        raise NotImplementedError

    def resource_fn(self, scaling_config: ScalingConfig) -> PlacementGroupFactory:
        raise NotImplementedError

    def fit(self, datasets: ray.data.Dataset, preprocessor: Preprocessor):
        from ray.tune.tuner import Tuner

        trainable = self.as_trainable()
        tuner = Tuner(
            trainable=trainable,
            run_config=self.run_config,
            param_space={
                "preprocessor": preprocessor,
                "datasets": datasets,
                **self.kwargs,
            },
        )
        analysis, _ = tuner.fit()
        return analysis

    def as_trainable(self) -> Type["Trainable"]:

        self_run_config = copy.deepcopy(self.run_config) or {}
        self_scaling_config = copy.deepcopy(self.scaling_config) or {}
        self_resume_from_checkpoint = copy.deepcopy(self.resume_from_checkpoint)
        self_kwargs = copy.deepcopy(self.kwargs)

        # Using a function trainable here as XGBoost-Ray's integrations
        # (e.g. callbacks) are optimized for this case
        def internal_train_fn(config, checkpoint_dir):
            override_run_config = config.pop("run_config", None)
            override_scaling_config = config.pop("scaling_config", None)

            this_datasets = {}
            override_datasets = config.pop("datasets", None)

            if override_datasets:
                if isinstance(override_datasets, dict):
                    this_datasets.update(override_datasets)
                elif isinstance(override_datasets, Dataset):
                    this_datasets = override_datasets
                else:
                    assert False
            else:
                another_override_datasets = self_kwargs.pop("datasets", None)
                if another_override_datasets:
                    if isinstance(another_override_datasets, dict):
                        this_datasets.update(another_override_datasets)
                    elif isinstance(another_override_datasets, Dataset):
                        this_datasets = another_override_datasets
                    else:
                        assert False

            preprocessor = config.pop("preprocessor", None)

            if preprocessor:
                processed_datasets = {
                    name: preprocessor.fit_transform(ds)
                    for name, ds in this_datasets.items()
                }

            else:
                processed_datasets = this_datasets

            run_config = self_run_config or {}
            if override_run_config:
                run_config.update(override_run_config)

            scaling_config = self_scaling_config or {}
            if override_scaling_config:
                scaling_config.update(override_scaling_config)

            def update(name: str, arg: Any, config: Dict[str, Any]):
                # Pop config items to allow fo deep override
                config_item = config.pop(name, None)
                if isinstance(arg, dict) and isinstance(config_item, dict):
                    arg.update(config_item)
                    return arg
                return config_item or arg

            updated_kwargs = {
                name: update(name, arg, config) for name, arg in self_kwargs.items()
            }

            # Update with remaining config ite s
            updated_kwargs.update(config)

            if checkpoint_dir:
                checkpoint = LocalStorageCheckpoint(path=checkpoint_dir)
            else:
                checkpoint = self_resume_from_checkpoint

            self.train_fn(
                run_config=run_config,
                scaling_config=scaling_config,
                datasets=processed_datasets,
                checkpoint_dir=checkpoint_dir,
                **updated_kwargs,
            )

        trainable = wrap_function(internal_train_fn)
        trainable.__name__ = self.train_fn.__name__

        # Monkey patching the resource requests for dynamic resource
        # allocation
        def resource_request(config):
            config = copy.deepcopy(config)

            scaling_config = {
                "num_actors": 0,
                "cpus_per_actor": 1,
                "gpus_per_actor": 1,
                "resources_per_actor": None,
            }
            if self_scaling_config:
                scaling_config.update(self_scaling_config)

            override_scaling_config = config.pop("scaling_config", None)
            if override_scaling_config:
                scaling_config.update(override_scaling_config)

            return self.resource_fn(scaling_config)

        trainable.default_resource_request = resource_request

        def postprocess_checkpoint(config: Dict[str, Any], checkpoint: Checkpoint):
            checkpoint.preprocessor = config.get("preprocessor", None)

        trainable.postprocess_checkpoint = postprocess_checkpoint

        return trainable


class XGBoostTrainer(FunctionTrainer):
    def train_fn(
        self,
        run_config: RunConfig,
        scaling_config: ScalingConfig,
        datasets: Dict[str, ray.data.Dataset],
        checkpoint_dir: Optional[str],
        label: str,
        params: Optional[Dict[str, Any]],
        **kwargs,
    ):
        # train_dataset = datasets["train_dataset"]
        # #####################################################
        # ############### CHANGE THIS BACK ###################
        train_dataset = gen_dataset_func()
        prep_v1 = Chain(
            Scaler(["worst radius", "worst area"]), Repartitioner(num_partitions=4)
        )
        train_dataset = prep_v1.fit_transform(train_dataset)
        # #####################################################
        train_dataset.show()

        dmatrix = xgboost_ray.RayDMatrix(train_dataset, label=label)
        evals_result = {}

        ray_params = xgboost_ray.RayParams()
        ray_params.__dict__.update(**run_config)
        ray_params.__dict__.update(**scaling_config)

        xgb_model = None
        if checkpoint_dir:
            assert False

        xgboost_ray.train(
            dtrain=dmatrix,
            params=params,
            evals_result=evals_result,
            ray_params=ray_params,
            callbacks=[TuneReportCheckpointCallback(filename="model.xgb", frequency=1)],
            xgb_model=xgb_model,
            **kwargs,
        )

    def resource_fn(self, scaling_config: ScalingConfig):
        return _get_tune_resources(**scaling_config)


class XGBoostPredictor:
    def __init__(self, model):
        self.model = model

    @classmethod
    def from_checkpoint(cls, checkpoint):
        # local_storage_cp = checkpoint.to_local_storage()
        # Change to above once Checkpoint abstraction is ready.
        local_storage_cp_path = checkpoint.value
        bst = xgboost.Booster(
            model_file=os.path.join(local_storage_cp_path, "model.xgb")
        )
        # shutil.rmtree(local_storage_cp.path)
        return XGBoostPredictor(bst)

    def predict(self, data: DataBatchType) -> DataBatchType:
        dmatrix = xgboost.DMatrix(data)
        return self.model.predict(dmatrix)


# def test_xgboost_trainer():
#
#     def gen_dataset():
#         data_raw = load_breast_cancer(as_frame=True)
#         dataset_df = data_raw["data"]
#         dataset_df["target"] = data_raw["target"]
#         dataset = ray.data.from_pandas(dataset_df)
#         return dataset
#
#     preprocessor = Chain(
#         Scaler(["worst radius", "worst area"]), Repartitioner(num_partitions=2)
#     )
#
#     params = {
#         "tree_method": "approx",
#         "objective": "binary:logistic",
#         "eval_metric": ["logloss", "error"],
#     }
#
#     trainer = XGBoostTrainer(
#         scaling_config={
#             "num_actors": 2,
#             "gpus_per_actor": 0,
#             "cpus_per_actor": 2,
#         },
#         run_config={"max_actor_restarts": 1},
#         label="target",
#         params=params,
#     )
#     # result = trainer.fit({"train_dataset": DatasetWrapper(dataset=gen_dataset(), gen_fn=gen_dataset)}, preprocessor=preprocessor)
#     result = trainer.fit({"train_dataset": gen_dataset()}, preprocessor=preprocessor)
#     print(result)
#
#     this_checkpoint = result.get_best_trial().checkpoint
#
#     data_raw = load_breast_cancer(as_frame=True)
#     dataset_df = data_raw["data"]
#
#     this_model = XGBoostPredictor.from_checkpoint(this_checkpoint)
#     predicted = this_model.predict(dataset_df)
#     print(predicted)


from ray.data.datasource.datasource import Datasource, ReadTask
from ray.data.impl.block_list import BlockMetadata


class TestDatasource(Datasource):
    def prepare_read(self, parallelism: int, **read_args):
        import pyarrow as pa

        def load_data():
            data_raw = load_breast_cancer(as_frame=True)
            dataset_df = data_raw["data"]
            dataset_df["target"] = data_raw["target"]
            return [pa.Table.from_pandas(dataset_df)]

        meta = BlockMetadata(num_rows=None, size_bytes=None, schema=None, input_files=None, exec_stats=None)
        return [ReadTask(load_data, meta)]


def gen_dataset_func() -> Dataset:
    test_datasource = TestDatasource()
    return ray.data.read_datasource(test_datasource)


# def gen_dataset_func() -> ray.data.Dataset:
#     data_raw = load_breast_cancer(as_frame=True)
#     dataset_df = data_raw["data"]
#     dataset_df["target"] = data_raw["target"]
#     dataset = ray.data.from_pandas(dataset_df)
#     return dataset

def test_xgboost_tuner(fail_after_finished: int = 0):
    shutil.rmtree("/Users/xwjiang/ray_results/tuner_resume", ignore_errors=True)

    # Tune datasets
    # dataset_v1 = dataset.random_shuffle(seed=1234)
    # dataset_v2, _ = dataset.random_shuffle(seed=2345).split(2, equal=True)

    # dataset_v1.get_internal_block_refs()

    # For Tune table output (makes it easier to debug)
    ray.data.Dataset.__repr__ = lambda self: (
        f"<Dataset num_rows=" f"{self._meta_count()}>"
    )

    # Tune preprocessors
    prep_v1 = Chain(
        Scaler(["worst radius", "worst area"]), Repartitioner(num_partitions=4)
    )

    prep_v2 = Chain(
        Scaler(["worst concavity", "worst smoothness"]), Repartitioner(num_partitions=8)
    )

    param_space = {
        "scaling_config": {
            "num_actors": tune.grid_search([1, 2, 4]),
            # "num_actors": 2,
            "cpus_per_actor": 2,
            "gpus_per_actor": 0,
        },
        # "preprocessor": tune.grid_search([prep_v1, prep_v2]),
        "preprocessor": prep_v1,
        "datasets": {
            "train_dataset": tune.grid_search([gen_dataset_func(), gen_dataset_func()]),
            # "train_dataset": gen_dataset_func(),
        },
        "params": {
            "objective": "binary:logistic",
            "tree_method": "approx",
            "eval_metric": ["logloss", "error"],
            "eta": tune.loguniform(1e-4, 1e-1),
            "subsample": tune.uniform(0.5, 1.0),
            "max_depth": tune.randint(1, 9),
        },
    }

    if fail_after_finished > 0:
        callbacks = [StopperCallback(fail_after_finished=fail_after_finished)]
    else:
        callbacks = None

    tuner = Tuner(
        trainable=XGBoostTrainer(
            run_config={"max_actor_restarts": 1},
            scaling_config=None,
            resume_from_checkpoint=None,
            label="target",
        ),
        param_space=param_space,
        name="tuner_resume",
        callbacks=callbacks,
    )

    results = tuner.fit()
    import ipdb; ipdb.set_trace()


def test_xgboost_resume():
    # Dataset pickling/unpickling currentyl does not work
    # thus we have to set this again

    tuner = Tuner.restore("/Users/xwjiang/ray_results/tuner_resume")

    results = tuner.fit()
    print(results.results)

    # best_result = results.results[0]
    # best_checkpoint = best_result.checkpoint
    # print(best_result.metrics, best_checkpoint)
    #
    # predict_data = gen_dataset_func(apply_label=False)
    # best_model = best_checkpoint.load_model()
    # predicted = best_model.predict(predict_data)
    # print(predicted.to_pandas())


if __name__ == "__main__":
    # ray.init("ray://127.0.0.1:10001")

    ray.init()
    test_xgboost_tuner()
    # test_xgboost_resume()
