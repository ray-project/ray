import pickle

from freezegun import freeze_time
import pytest

from ray.tune.stopper import TimeoutStopper, TrialPlateauStopper


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


def test_trial_plateau_stopper_zero_metric_threshold():
    with pytest.raises(ValueError, match="mode"):
        TrialPlateauStopper(metric="loss", metric_threshold=0.0)

    stopper = TrialPlateauStopper(
        metric="loss", std=0.01, num_results=4, grace_period=4, metric_threshold=0.0, mode="min"
    )
    # Plateaued at 0.5 > 0.0 -> should not stop because threshold 0.0 is not reached
    for _ in range(4):
        assert not stopper("trial_1", {"loss": 0.5})

    # Plateaued at -0.1 <= 0.0 -> should stop
    for _ in range(3):
        assert not stopper("trial_2", {"loss": -0.1})
    assert stopper("trial_2", {"loss": -0.1})


def test_trial_plateau_stopper_missing_metric():
    stopper = TrialPlateauStopper(
        metric="loss", std=0.01, num_results=3, grace_period=3, metric_threshold=1.0, mode="min"
    )
    assert not stopper("trial_1", {"loss": 0.5})
    assert not stopper("trial_1", {"loss": 0.5})
    # Missing metric in intermediate report should not raise TypeError or corrupt history
    assert not stopper("trial_1", {"other_metric": 1.0})
    assert stopper("trial_1", {"loss": 0.5})



if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__] + sys.argv[1:]))
