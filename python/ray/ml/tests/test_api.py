import pytest

import ray
from ray.ml import Checkpoint
from ray.ml.trainer import Trainer
from ray.ml.preprocessor import Preprocessor


class DummyTrainer(Trainer):
    def training_loop(self) -> None:
        pass


class DummyDataset(ray.data.Dataset):
    def __init__(self):
        pass


def test_run_config():
    with pytest.raises(ValueError):
        DummyTrainer(run_config="invalid")

    with pytest.raises(ValueError):
        DummyTrainer(run_config=False)

    with pytest.raises(ValueError):
        DummyTrainer(run_config=True)

    with pytest.raises(ValueError):
        DummyTrainer(run_config={})

    # Succeed
    DummyTrainer(run_config=None)

    # Succeed
    DummyTrainer(run_config=ray.ml.RunConfig())


def test_scaling_config():
    with pytest.raises(ValueError):
        DummyTrainer(scaling_config="invalid")

    with pytest.raises(ValueError):
        DummyTrainer(scaling_config=False)

    with pytest.raises(ValueError):
        DummyTrainer(scaling_config=True)

    # Succeed
    DummyTrainer(scaling_config={})

    # Succeed
    DummyTrainer(scaling_config=None)


def test_datasets():
    with pytest.raises(ValueError):
        DummyTrainer(datasets="invalid")

    with pytest.raises(ValueError):
        DummyTrainer(datasets=False)

    with pytest.raises(ValueError):
        DummyTrainer(datasets=True)

    with pytest.raises(ValueError):
        DummyTrainer(datasets={"test": "invalid"})

    # Succeed
    DummyTrainer(datasets=None)

    # Succeed
    DummyTrainer(datasets={"test": DummyDataset()})


def test_preprocessor():
    with pytest.raises(ValueError):
        DummyTrainer(preprocessor="invalid")

    with pytest.raises(ValueError):
        DummyTrainer(preprocessor=False)

    with pytest.raises(ValueError):
        DummyTrainer(preprocessor=True)

    with pytest.raises(ValueError):
        DummyTrainer(preprocessor={})

    # Succeed
    DummyTrainer(preprocessor=None)

    # Succeed
    DummyTrainer(preprocessor=Preprocessor())


def test_resume_from_checkpoint():
    with pytest.raises(ValueError):
        DummyTrainer(resume_from_checkpoint="invalid")

    with pytest.raises(ValueError):
        DummyTrainer(resume_from_checkpoint=False)

    with pytest.raises(ValueError):
        DummyTrainer(resume_from_checkpoint=True)

    with pytest.raises(ValueError):
        DummyTrainer(resume_from_checkpoint={})

    # Succeed
    DummyTrainer(resume_from_checkpoint=None)

    # Succeed
    DummyTrainer(resume_from_checkpoint=Checkpoint.from_dict({"empty": ""}))


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
