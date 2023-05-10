import pytest

import ray
from ray.air import Checkpoint
from ray.air._internal.config import ensure_only_allowed_dataclass_keys_updated
from ray.air.config import ScalingConfig, CheckpointConfig
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


def test_checkpointing_config():
    with pytest.raises(ValueError):
        CheckpointConfig(
            checkpoint_score_attribute="metric", checkpoint_score_order="invalid"
        )

    checkpointing = CheckpointConfig()
    assert checkpointing._tune_legacy_checkpoint_score_attr is None

    checkpointing = CheckpointConfig(checkpoint_score_attribute="metric")
    assert checkpointing._tune_legacy_checkpoint_score_attr == "metric"

    checkpointing = CheckpointConfig(
        checkpoint_score_attribute="metric", checkpoint_score_order="max"
    )
    assert checkpointing._tune_legacy_checkpoint_score_attr == "metric"

    checkpointing = CheckpointConfig(
        checkpoint_score_attribute="metric", checkpoint_score_order="min"
    )
    assert checkpointing._tune_legacy_checkpoint_score_attr == "min-metric"


def test_scaling_config():
    with pytest.raises(ValueError):
        DummyTrainer(scaling_config="invalid")

    with pytest.raises(ValueError):
        DummyTrainer(scaling_config=False)

    with pytest.raises(ValueError):
        DummyTrainer(scaling_config=True)

    with pytest.raises(ValueError):
        DummyTrainer(scaling_config={})

    # Succeed
    DummyTrainer(scaling_config=ScalingConfig())

    # Succeed
    DummyTrainer(scaling_config=None)


def test_scaling_config_validate_config_valid_class():
    scaling_config = {"num_workers": 2}
    ensure_only_allowed_dataclass_keys_updated(
        ScalingConfig(**scaling_config), ["num_workers"]
    )


def test_scaling_config_validate_config_prohibited_class():
    # Check for prohibited keys
    scaling_config = {"num_workers": 2}
    with pytest.raises(ValueError) as exc_info:
        ensure_only_allowed_dataclass_keys_updated(
            ScalingConfig(**scaling_config),
            ["trainer_resources"],
        )
    assert "num_workers" in str(exc_info.value)
    assert "to be updated" in str(exc_info.value)


def test_scaling_config_validate_config_bad_allowed_keys():
    # Check for keys not present in dict
    scaling_config = {"num_workers": 2}
    with pytest.raises(ValueError) as exc_info:
        ensure_only_allowed_dataclass_keys_updated(
            ScalingConfig(**scaling_config),
            ["BAD_KEY"],
        )
    assert "BAD_KEY" in str(exc_info.value)
    assert "are not present in" in str(exc_info.value)


@pytest.mark.parametrize(
    "trainer_resources", [None, {}, {"CPU": 1}, {"CPU": 2, "GPU": 1}, {"CPU": 0}]
)
@pytest.mark.parametrize("num_workers", [None, 1, 2])
@pytest.mark.parametrize(
    "resources_per_worker_and_use_gpu",
    [
        (None, False),
        (None, True),
        ({}, False),
        ({"CPU": 1}, False),
        ({"CPU": 2, "GPU": 1}, True),
        ({"CPU": 0}, False),
    ],
)
@pytest.mark.parametrize("placement_strategy", ["PACK", "SPREAD"])
def test_scaling_config_pgf_equivalance(
    trainer_resources, resources_per_worker_and_use_gpu, num_workers, placement_strategy
):
    resources_per_worker, use_gpu = resources_per_worker_and_use_gpu
    scaling_config = ScalingConfig(
        trainer_resources=trainer_resources,
        num_workers=num_workers,
        resources_per_worker=resources_per_worker,
        use_gpu=use_gpu,
        placement_strategy=placement_strategy,
    )
    try:
        pgf = scaling_config.as_placement_group_factory()
        scaling_config_from_pgf = ScalingConfig.from_placement_group_factory(pgf)
        assert scaling_config == scaling_config_from_pgf
        assert scaling_config_from_pgf.as_placement_group_factory() == pgf
    except ValueError as e:
        # We do not have to test invalid placement group factories
        assert str(e) == (
            "Cannot initialize a ResourceRequest with an empty head "
            "and zero worker bundles."
        )


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
