import datetime
import pickle

import pytest
from freezegun import freeze_time

from ray.tune.stopper import TimeoutStopper


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


@pytest.mark.parametrize(
    "invalid_timeout",
    [
        True,
        False,
        float("nan"),
        float("inf"),
        float("-inf"),
        -1,
        -0.5,
        datetime.timedelta(seconds=-5),
        "60",
    ],
)
def test_timeout_stopper_validation(invalid_timeout):
    with pytest.raises(ValueError):
        TimeoutStopper(timeout=invalid_timeout)


def test_timeout_stopper_backward_clock_and_epoch_zero(monkeypatch):
    """Ensure epoch 0.0 is tracked and backward clock steps do not refund budget."""
    import types

    times = iter([0.0, 40.0, 20.0, 45.0])
    monkeypatch.setattr(
        "ray.tune.stopper.timeout.time",
        types.SimpleNamespace(time=lambda: next(times)),
    )
    stopper = TimeoutStopper(timeout=60)
    # First check at t=0.0 sets _last_check = 0.0 (budget = 60)
    assert not stopper.stop_all()
    # Second check at t=40.0 deducts 40s even though _last_check == 0.0 (budget = 20)
    assert not stopper.stop_all()
    # Backward clock step to t=20.0 does not refund 20s (budget stays 20)
    assert not stopper.stop_all()
    # Check at t=45.0 deducts 25s (budget = -5 <= 0) -> stops
    assert stopper.stop_all()


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__] + sys.argv[1:]))
