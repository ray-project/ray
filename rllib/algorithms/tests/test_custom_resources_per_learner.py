import pytest

from ray.rllib.algorithms.algorithm_config import AlgorithmConfig


def test_default_is_empty():
    cfg = AlgorithmConfig()
    assert cfg.custom_resources_per_learner == {}


def test_round_trip():
    cfg = AlgorithmConfig().learners(
        custom_resources_per_learner={"my_label": 0.001, "other": 1}
    )
    assert cfg.custom_resources_per_learner == {"my_label": 0.001, "other": 1}


def test_not_provided_preserves_value():
    cfg = AlgorithmConfig().learners(custom_resources_per_learner={"x": 1})
    cfg = cfg.learners(num_learners=2)  # touching `learners()` again must not clear
    assert cfg.custom_resources_per_learner == {"x": 1}


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
