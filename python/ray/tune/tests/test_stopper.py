import pickle

import pytest
from freezegun import freeze_time

from ray.tune.stopper import ExperimentPlateauStopper, TimeoutStopper


def test_timeout_stopper_timeout():
    with freeze_time() as frozen:
        stopper = TimeoutStopper(timeout=60)
        assert not stopper.stop_all()
        frozen.tick(40)
        assert not stopper.stop_all()
        frozen.tick(22)
        assert stopper.stop_all()


def test_timeout_stopper_recover_before_timeout():
    """If checkpointed before timeout, should continue where we left."""
    with freeze_time() as frozen:
        stopper = TimeoutStopper(timeout=60)
        assert not stopper.stop_all()
        frozen.tick(40)
        assert not stopper.stop_all()
        checkpoint = pickle.dumps(stopper)

        # Continue sometime in the future. This is after start_time + timeout
        # but we should still continue training.
        frozen.tick(200)

        # Continue, so we shouldn't time out
        stopper = pickle.loads(checkpoint)
        assert not stopper.stop_all()
        frozen.tick(10)
        assert not stopper.stop_all()
        frozen.tick(12)
        assert stopper.stop_all()


def test_timeout_stopper_recover_after_timeout():
    """If checkpointed after timeout, should still stop after recover."""
    with freeze_time() as frozen:
        stopper = TimeoutStopper(timeout=60)
        assert not stopper.stop_all()
        frozen.tick(62)
        assert stopper.stop_all()
        checkpoint = pickle.dumps(stopper)

        # Continue sometime in the future
        frozen.tick(200)

        # Continue, so we should still time out.
        stopper = pickle.loads(checkpoint)
        assert stopper.stop_all()
        frozen.tick(10)
        assert stopper.stop_all()


@pytest.mark.parametrize("mode", ["min", "max"])
def test_experiment_plateau_stopper_accepts_huge_ints(mode):
    """Integer metrics past int64 must still be tracked, not raise."""
    stopper = ExperimentPlateauStopper("loss", std=1.0, top=2, mode=mode)
    assert [stopper(f"trial_{i}", {"loss": 2**64}) for i in range(2)] == [False, True]


@pytest.mark.parametrize("mode", ["min", "max"])
def test_experiment_plateau_stopper_ignores_nan(mode):
    """A NaN result must not keep the stopper from detecting a plateau."""
    stopper = ExperimentPlateauStopper("loss", std=0.01, top=3, mode=mode)
    decisions = [
        stopper(f"trial_{i}", {"loss": value})
        for i, value in enumerate([float("nan")] + [1.0] * 5)
    ]
    assert decisions == [False, False, False, True, True, True]


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__] + sys.argv[1:]))
