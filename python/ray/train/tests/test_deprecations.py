import pytest


def test_deprecations():
    with pytest.raises(DeprecationWarning):
        from ray.train import Trainer

        Trainer(backend="test", num_workers=1)

    with pytest.raises(DeprecationWarning):
        from ray.train.trainer import Trainer

        Trainer(backend="test", num_workers=1)

    with pytest.raises(DeprecationWarning):
        from ray.train import TrainingCallback

        TrainingCallback()

    with pytest.raises(DeprecationWarning):
        from ray.train.callbacks import TrainingCallback

        TrainingCallback()

    with pytest.raises(DeprecationWarning):
        from ray.train.callbacks.print import PrintCallback

        PrintCallback()

    with pytest.raises(DeprecationWarning):
        from ray.train import CheckpointStrategy

        CheckpointStrategy()

    with pytest.raises(DeprecationWarning):
        from ray.train.checkpoint import CheckpointStrategy

        CheckpointStrategy()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
