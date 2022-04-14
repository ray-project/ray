import copy
from typing import Dict, Type

import ray
from ray.tune import Trainable, PlacementGroupFactory
from ray.tune.result import RESULT_DUPLICATE
from ray.util.placement_group import get_current_placement_group


class _CrossValidationTrainable(Trainable):
    """Abstract Trainable class for CV."""

    # Wrapped trainable upon which CV is conducted.
    _trainable = None

    def __init__(self, **kwargs):
        self._placement_group = get_current_placement_group()
        self._workers = []
        self._finished = False
        super().__init__(**kwargs)

    @classmethod
    def _validate_cv_config(cls, cv_config: Dict):
        assert "num_folds" in cv_config.keys()
        assert "train_and_validation" in cv_config.keys()
        return cv_config["num_folds"], cv_config["train_and_validation"]

    def setup(self, config: Dict):
        remote_trainable_cls = ray.remote(self.__class__._trainable)
        # strip `config` off any datasets argument if any.
        assert "datasets" in config.keys()
        num_folds, dataset = self._validate_cv_config(config.pop("datasets"))
        shards = dataset.split(num_folds)
        pg_factory = self.__class__._trainable.default_resource_request(config)
        assert num_folds == 3  # just gonna verify the simple case.
        for i in range(num_folds):
            # check the API
            if i == 0:
                train_ds = shards[1].union(shards[2])
                val_ds = shards[0]
            elif i == 1:
                train_ds = shards[0].union(shards[2])
                val_ds = shards[1]
            else:
                train_ds = shards[0].union(shards[1])
                val_ds = shards[2]

            worker_config = copy.deepcopy(config)
            worker_config.update({"datasets": {"train": train_ds, "validation": val_ds}})
            worker = remote_trainable_cls.options(
                placement_group_capture_child_tasks=True,
                placement_group=self._placement_group,
                placement_group_bundle_index=-1).remote(**worker_config)
            self.workers.append(worker)

    @classmethod
    def default_resource_request(cls, config):
        num_folds, _ = cls._validate_cv_config(config["datasets"])
        # assuming a trainer + workers bundle structure.
        per_trainable_pg = cls._trainable.default_resource_request(config)
        bundles = per_trainable_pg._bundles
        assert len(bundles) > 0
        new_bundles = bundles * num_folds
        return PlacementGroupFactory(new_bundles, strategy=per_trainable_pg._strategy)

    def step(self) -> Dict:
        if self._finished:
            raise RuntimeError("Training has already finished.")
        result = ray.get([w.step() for w in self._workers])
        if RESULT_DUPLICATE in result:
            self._finished = True
        return result

    def stop(self):
        ray.get([w.stop() for w in self._workers])


def create_cross_validation_trainable(
    trainable: Type[Trainable],
) -> Type[_CrossValidationTrainable]:

    class WrappedCrossValidationTrainable(_CrossValidationTrainable):
        _trainable = trainable

    return WrappedCrossValidationTrainable
