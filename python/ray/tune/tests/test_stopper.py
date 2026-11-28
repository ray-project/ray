import pickle

from freezegun import freeze_time

from ray.tune.stopper import (
    CombinedStopper,
    ExperimentPlateauStopper,
    MaximumIterationStopper,
    TimeoutStopper,
)


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


def test_combined_stopper_passes_every_result_to_every_stopper():
    """A later stopper still sees the result an earlier one stops a trial on."""
    plateau = ExperimentPlateauStopper("loss", top=3, mode="min")
    stopper = CombinedStopper(MaximumIterationStopper(max_iter=2), plateau)

    for i, final_loss in enumerate([0.3, 0.2, 0.1]):
        assert not stopper(f"t{i}", {"loss": 1.0})
        # MaximumIterationStopper ends each trial on its second result.
        assert stopper(f"t{i}", {"loss": final_loss})

    # The plateau stopper must rank the final losses, which have not plateaued,
    # rather than only the identical first-iteration losses.
    assert not stopper.stop_all()


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__] + sys.argv[1:]))
