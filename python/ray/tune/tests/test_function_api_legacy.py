import pytest
import sys

from ray import tune


def test_checkpoint_dir_deprecation():
    def train_fn(config, checkpoint_dir=None):
        pass

    with pytest.raises(DeprecationWarning, match=r".*checkpoint_dir.*"):
        tune.run(train_fn)

    def train_fn(config, reporter):
        pass

    with pytest.raises(DeprecationWarning, match=r".*reporter.*"):
        tune.run(train_fn)

    def train_fn(config):
        tune.report(test=1)

    with pytest.raises(DeprecationWarning, match=r".*tune\.report.*"):
        tune.run(train_fn, fail_fast="raise")

    def train_fn(config):
        with tune.checkpoint_dir(step=1) as _:
            pass

    with pytest.raises(DeprecationWarning, match=r".*tune\.checkpoint_dir.*"):
        tune.run(train_fn, fail_fast="raise")


if __name__ == "__main__":

    sys.exit(pytest.main(["-v", __file__]))
