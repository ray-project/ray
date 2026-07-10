from types import SimpleNamespace
from unittest import mock

import pytest

from ray.rllib.algorithms.algorithm import Algorithm
from ray.rllib.utils.replay_buffers import (
    EpisodeReplayBuffer,
    MultiAgentPrioritizedReplayBuffer,
)


def _call(config, smoothing=5):
    """Call the (unbound) buffer factory with a minimal fake `self`."""
    fake_self = SimpleNamespace(
        config=SimpleNamespace(metrics_num_episodes_for_smoothing=smoothing)
    )
    with mock.patch(
        "ray.rllib.algorithms.algorithm.from_config", return_value="BUFFER"
    ):
        return Algorithm._create_local_replay_buffer_if_necessary(fake_self, config)


def test_replay_buffer_type_as_class_does_not_crash():
    """`replay_buffer_config["type"]` may be a class, not a string.

    Regression test for #60491: `AlgorithmConfig.validate()` resolves the
    `type` string into the actual class, after which the substring check
    `"EpisodeReplayBuffer" in type` raised
    `TypeError: argument of type 'ABCMeta' is not iterable`.
    """
    # Non-episode buffer class -> branch skipped, still returns a buffer.
    config = {
        "replay_buffer_config": {
            "type": MultiAgentPrioritizedReplayBuffer,
            "capacity": 100,
        }
    }
    assert _call(config) == "BUFFER"


def test_episode_replay_buffer_class_injects_smoothing():
    """An EpisodeReplayBuffer class triggers the smoothing-kwarg injection."""
    config = {"replay_buffer_config": {"type": EpisodeReplayBuffer, "capacity": 100}}
    assert _call(config, smoothing=42) == "BUFFER"
    assert config["replay_buffer_config"]["metrics_num_episodes_for_smoothing"] == 42


def test_replay_buffer_type_as_string_still_works():
    """The legacy string form of `type` must keep working."""
    config = {
        "replay_buffer_config": {
            "type": "MultiAgentPrioritizedReplayBuffer",
            "capacity": 100,
        }
    }
    assert _call(config) == "BUFFER"


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
