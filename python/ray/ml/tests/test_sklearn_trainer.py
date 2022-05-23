import pytest
import pandas as pd

import ray
from ray import tune
from ray.ml.checkpoint import Checkpoint
from ray.ml.constants import TRAIN_DATASET_KEY

from ray.ml.train.integrations.sklearn import SklearnTrainer, load_checkpoint
from ray.ml.preprocessor import Preprocessor

from sklearn.datasets import load_breast_cancer
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier


@pytest.fixture
def ray_start_4_cpus():
    address_info = ray.init(num_cpus=4)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


scale_config = {"trainer_resources": {"CPU": 2}}

data_raw = load_breast_cancer()
dataset_df = pd.DataFrame(data_raw["data"], columns=data_raw["feature_names"])
dataset_df["target"] = data_raw["target"]
train_df, test_df = train_test_split(dataset_df, test_size=0.3)


def test_fit(ray_start_4_cpus):
    train_dataset = ray.data.from_pandas(train_df)
    valid_dataset = ray.data.from_pandas(test_df)
    trainer = SklearnTrainer(
        estimator=RandomForestClassifier(),
        scaling_config=scale_config,
        label_column="target",
        datasets={TRAIN_DATASET_KEY: train_dataset, "valid": valid_dataset},
    )
    result = trainer.fit()

    assert "valid" in result.metrics
    assert "cv" not in result.metrics


def test_fit_cv(ray_start_4_cpus):
    train_dataset = ray.data.from_pandas(train_df)
    valid_dataset = ray.data.from_pandas(test_df)
    trainer = SklearnTrainer(
        estimator=RandomForestClassifier(),
        scaling_config=scale_config,
        label_column="target",
        cv=5,
        return_train_score_cv=True,
        datasets={TRAIN_DATASET_KEY: train_dataset, "valid": valid_dataset},
    )
    result = trainer.fit()

    assert "valid" in result.metrics
    assert "cv" in result.metrics
    assert "test_score" in result.metrics["cv"]
    assert "test_score_mean" in result.metrics["cv"]
    assert "train_score" in result.metrics["cv"]
    assert "train_score_mean" in result.metrics["cv"]


def test_no_auto_cpu_params(ray_start_4_cpus, tmpdir):
    train_dataset = ray.data.from_pandas(train_df)
    valid_dataset = ray.data.from_pandas(test_df)

    class DummyPreprocessor(Preprocessor):
        def __init__(self):
            super().__init__()
            self.is_same = True

        def fit(self, dataset):
            self.fitted_ = True

        def _transform_pandas(self, df: "pd.DataFrame") -> "pd.DataFrame":
            return df

    trainer = SklearnTrainer(
        estimator=RandomForestClassifier(n_jobs=1),
        scaling_config=scale_config,
        label_column="target",
        datasets={TRAIN_DATASET_KEY: train_dataset, "valid": valid_dataset},
        preprocessor=DummyPreprocessor(),
        set_estimator_cpus=False,
    )
    result = trainer.fit()

    model, _ = load_checkpoint(result.checkpoint)
    assert model.n_jobs == 1


def test_preprocessor_in_checkpoint(ray_start_4_cpus, tmpdir):
    train_dataset = ray.data.from_pandas(train_df)
    valid_dataset = ray.data.from_pandas(test_df)

    class DummyPreprocessor(Preprocessor):
        def __init__(self):
            super().__init__()
            self.is_same = True

        def fit(self, dataset):
            self.fitted_ = True

        def _transform_pandas(self, df: "pd.DataFrame") -> "pd.DataFrame":
            return df

    trainer = SklearnTrainer(
        estimator=RandomForestClassifier(),
        scaling_config=scale_config,
        label_column="target",
        datasets={TRAIN_DATASET_KEY: train_dataset, "valid": valid_dataset},
        preprocessor=DummyPreprocessor(),
    )
    result = trainer.fit()

    # Move checkpoint to a different directory.
    checkpoint_dict = result.checkpoint.to_dict()
    checkpoint = Checkpoint.from_dict(checkpoint_dict)
    checkpoint_path = checkpoint.to_directory(tmpdir)
    resume_from = Checkpoint.from_directory(checkpoint_path)

    model, preprocessor = load_checkpoint(resume_from)
    assert hasattr(model, "feature_importances_")
    assert preprocessor.is_same
    assert preprocessor.fitted_


def test_tune(ray_start_4_cpus):
    train_dataset = ray.data.from_pandas(train_df)
    valid_dataset = ray.data.from_pandas(test_df)
    trainer = SklearnTrainer(
        estimator=RandomForestClassifier(),
        scaling_config=scale_config,
        label_column="target",
        params={"max_depth": 4},
        cv=5,
        datasets={TRAIN_DATASET_KEY: train_dataset, "valid": valid_dataset},
    )

    tune.run(
        trainer.as_trainable(),
        config={"params": {"max_depth": tune.randint(2, 4)}},
        num_samples=2,
        metric="cv/test_score_mean",
        mode="max",
    )

    # Make sure original Trainer is not affected.
    assert trainer.params["max_depth"] == 4


def test_validation(ray_start_4_cpus):
    train_dataset = ray.data.from_pandas(train_df)
    valid_dataset = ray.data.from_pandas(test_df)
    with pytest.raises(KeyError, match=TRAIN_DATASET_KEY):
        SklearnTrainer(
            estimator=RandomForestClassifier(),
            scaling_config=scale_config,
            label_column="target",
            datasets={"valid": valid_dataset},
        )
    with pytest.raises(KeyError, match="cv"):
        SklearnTrainer(
            estimator=RandomForestClassifier(),
            scaling_config=scale_config,
            label_column="target",
            datasets={TRAIN_DATASET_KEY: train_dataset, "cv": valid_dataset},
        )
    with pytest.raises(ValueError, match="are not allowed to be set"):
        SklearnTrainer(
            estimator=RandomForestClassifier(),
            scaling_config={"num_workers": 2},
            label_column="target",
            datasets={TRAIN_DATASET_KEY: train_dataset, "valid": valid_dataset},
        )
    with pytest.raises(ValueError, match="parallelize_cv"):
        SklearnTrainer(
            estimator=RandomForestClassifier(),
            scaling_config={"trainer_resources": {"GPU": 1}},
            label_column="target",
            cv=5,
            parallelize_cv=True,
            datasets={TRAIN_DATASET_KEY: train_dataset, "valid": valid_dataset},
        )


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
