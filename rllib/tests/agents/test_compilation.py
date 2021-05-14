"""Quick compilation tests for all algorithms.

The present tests are:
compilation tests - to ensure that algorithms compile with all frameworks

These tests exist in a separate file so that we can tag them separately in Bazel build.
"""

import logging

import pytest

from ray.rllib.agents.trainer import Trainer
from ray.rllib.agents.trainer_factory import (
    Algorithm,
    Framework,
)
from ray.rllib.tests.agents.parameters import (
    test_compilation_params,
)

logger = logging.getLogger(__name__)


@pytest.mark.usefixtures("ray_env")
@pytest.mark.usefixtures("using_framework")
@pytest.mark.parametrize(
    "algorithm, config_overrides, env, framework, n_iter, threshold",
    test_compilation_params,
)
def test_algorithms_can_compile_with_different_frameworks(
    algorithm: Algorithm,
    config_overrides: dict,
    env: str,
    framework: Framework,
    n_iter: int,
    threshold: float,
    trainer: Trainer,
):
    """I should be able to compile the each algorithm with the following frameworks:
        1. TensorFlow (Graph Mode)
        2. TensorFlow (Eager Mode)
        3. PyTorch
    NOTE: Not all algorithms have been implemented in all frameworks.
    """
    results = None
    for i in range(n_iter):
        results = trainer.train()
        logger.info(results)
    if n_iter >= 1:
        assert results is not None
    if results:
        if ("evaluation_interval" in config_overrides
                and config_overrides["evaluation_interval"]):
            assert results["evaluation"]["episode_reward_mean"] >= threshold
        else:
            assert results["episode_reward_mean"] >= threshold


