import sys
import warnings

from ray import tune


def test_checkpoint_dir_deprecation():
    warnings.filterwarnings("always")

    def train(config, checkpoint_dir=None):
        for i in range(10):
            tune.report({"foo": "bar"})

    with warnings.catch_warnings(record=True) as w:
        tune.run(train, num_samples=1)
        found_pattern = False
        for _w in w:
            if issubclass(
                _w.category, DeprecationWarning
            ) and "To save and load checkpoint in trainable function" in str(
                _w.message
            ):
                found_pattern = True
                break
        assert found_pattern


if __name__ == "__main__":
    import pytest

    sys.exit(pytest.main(["-v", __file__]))
