import pytest

import ray
from ray.air import Checkpoint, RunConfig, ScalingConfig, session
from ray.train.base_trainer import TrainingFailedError
from ray.train.data_parallel_trainer import DataParallelTrainer


@pytest.fixture
def ray_start_4_cpus():
    address_info = ray.init(num_cpus=4)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


def test_data_parallel_trainer_restore(ray_start_4_cpus, tmpdir):
    obj_ref = ray.put({"test": 1})

    def train_fn(config):
        assert ray.get(obj_ref)["test"] == 1
        assert ray.get(config["obj_ref"])["test"] == 1

        checkpoint = session.get_checkpoint()
        it = 0
        if checkpoint:
            print("\nLoading from checkpoint...\n")
            it = checkpoint.to_dict()["it"] + 1
        session.report({"it": it}, checkpoint=Checkpoint.from_dict({"it": it}))
        if it == 0:
            raise RuntimeError("")

    train_loop_config = {"obj_ref": obj_ref}
    datasets = {"train": ray.data.from_items([{"feature": i} for i in range(10)])}

    trainer = DataParallelTrainer(
        train_loop_per_worker=train_fn,
        train_loop_config=train_loop_config,
        datasets=datasets,
        scaling_config=ScalingConfig(num_workers=1),
        run_config=RunConfig(name="data_parallel_restore_test", local_dir=tmpdir),
    )
    with pytest.raises(TrainingFailedError):
        result = trainer.fit()

    trainer = DataParallelTrainer.restore(
        str(tmpdir / "data_parallel_restore_test"),
        train_loop_per_worker=train_fn,
        train_loop_config=train_loop_config,
        datasets=datasets,
    )
    result = trainer.fit()
    assert result.metrics["it"] == 1


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
