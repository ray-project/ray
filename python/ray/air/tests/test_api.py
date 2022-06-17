import pytest

import ray
from ray.air import Checkpoint
from ray.air._internal.config import ensure_only_allowed_dataclass_keys_updated
from ray.air.config import ScalingConfigDataClass
from ray.data.preprocessor import Preprocessor
from ray.train.trainer import BaseTrainer


class DummyTrainer(BaseTrainer):
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
    DummyTrainer(run_config=ray.air.RunConfig())


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


def test_scaling_config_validate_config_valid_class():
    scaling_config = {"num_workers": 2}
    ensure_only_allowed_dataclass_keys_updated(
        ScalingConfigDataClass(**scaling_config), ["num_workers"]
    )


def test_scaling_config_validate_config_prohibited_class():
    # Check for prohibited keys
    scaling_config = {"num_workers": 2}
    with pytest.raises(ValueError) as exc_info:
        ensure_only_allowed_dataclass_keys_updated(
            ScalingConfigDataClass(**scaling_config),
            ["trainer_resources"],
        )
    assert "num_workers" in str(exc_info.value)
    assert "to be updated" in str(exc_info.value)


def test_scaling_config_validate_config_bad_allowed_keys():
    # Check for keys not present in dict
    scaling_config = {"num_workers": 2}
    with pytest.raises(ValueError) as exc_info:
        ensure_only_allowed_dataclass_keys_updated(
            ScalingConfigDataClass(**scaling_config),
            ["BAD_KEY"],
        )
    assert "BAD_KEY" in str(exc_info.value)
    assert "are not present in" in str(exc_info.value)


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
