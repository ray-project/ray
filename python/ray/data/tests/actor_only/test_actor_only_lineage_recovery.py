"""Ray Data lineage reconstruction on the actor-only backend: a lost block or a
dead actor is recovered by re-running its lineage. Without reconstruction the
backend still runs, with a warning (its actors are never restarted by Ray
core)."""
import logging
import os

import pytest

import ray
from ray.data._internal.execution.interfaces.physical_operator import DataOpTask
from ray.exceptions import ObjectLostError
from ray.job_config import JobConfig

NUM_ROWS = 200


@pytest.fixture
def ray_with_data_reconstruction():
    ray.shutdown()
    # Ray Core lineage reconstruction off turns Ray Data's on.
    ray.init(
        num_cpus=4,
        job_config=JobConfig(_disable_job_level_lineage_reconstruction=True),
    )
    yield
    ray.shutdown()


def _two_stage_pipeline(udf=None, **map_kwargs):
    def add_one(batch):
        batch["id"] = batch["id"] + 1
        return batch

    return ray.data.range(NUM_ROWS, override_num_blocks=10).map_batches(
        udf or add_one,
        batch_size=10,
        compute=ray.data.ActorPoolStrategy(size=2),
        **map_kwargs,
    )


def _assert_every_row_once(ds, offset=1):
    assert sorted(row["id"] for row in ds.take_all()) == [
        i + offset for i in range(NUM_ROWS)
    ]


def test_runs_without_data_reconstruction_with_warning(caplog, propagate_logs):
    ray.shutdown()
    ray.init(num_cpus=4)
    try:
        with caplog.at_level(logging.WARNING, logger="ray.data"):
            _assert_every_row_once(_two_stage_pipeline())
        assert "without Ray Data lineage reconstruction" in caplog.text
    finally:
        ray.shutdown()


def test_plan_reconstruction_does_not_support_runs_without_it(
    ray_with_data_reconstruction,
):
    """A limit (like a split, union or mix) isn't supported by lineage
    reconstruction: the dataset runs without it rather than being refused."""
    assert len(_two_stage_pipeline().limit(5).take_all()) == 5


def test_lost_output_is_reconstructed(ray_with_data_reconstruction, monkeypatch):
    """A block lost while being read is rebuilt from its seed input."""
    lost = []
    original = DataOpTask.on_data_ready

    def lose_once(self, *, max_to_read, metadata_fetcher):
        if "add_one" in self.operator_name and not lost and self.task_index() == 1:
            lost.append(self.task_index())
            raise ObjectLostError(ray.ObjectRef.nil().hex(), None, "injected by test")
        return original(
            self, max_to_read=max_to_read, metadata_fetcher=metadata_fetcher
        )

    monkeypatch.setattr(DataOpTask, "on_data_ready", lose_once)

    _assert_every_row_once(_two_stage_pipeline())
    assert lost == [1]


class _DieOnce:
    """Kills its actor process the first time it sees row 57."""

    def __init__(self, marker: str):
        self._marker = marker

    def __call__(self, batch):
        if 57 in batch["id"] and not os.path.exists(self._marker):
            open(self._marker, "w").close()
            os._exit(1)
        batch["id"] = batch["id"] + 1
        return batch


def test_dead_actor_work_is_reconstructed(ray_with_data_reconstruction, tmp_path):
    """Ray core doesn't restart the actor; the sizer replaces it and lineage
    reconstruction re-runs the work it held."""
    marker = str(tmp_path / "died")
    ds = _two_stage_pipeline(_DieOnce, fn_constructor_kwargs={"marker": marker})

    _assert_every_row_once(ds)
    assert os.path.exists(marker)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
