"""Learning (Convergence) tests for all agents live here.

The present tests are:
convergence tests - to ensure that algorithms learn with all frameworks
monotonic convergence tests - similar to convergence tests, but take advantage of the
    monotonic improvement nature of algorithms to run faster.

These tests exist in a separate file so that we can tag them separately in Bazel build.
"""
import logging

import pytest

from ray.rllib.agents.trainer import Trainer
from ray.rllib.agents.trainer_factory import Algorithm, Framework
from ray.rllib.tests.agents.parameters import (
    test_convergence_params,
    test_monotonic_convergence_params,
)


logger = logging.getLogger(__name__)


@pytest.mark.minutes
@pytest.mark.usefixtures("ray_env")
@pytest.mark.usefixtures("using_framework")
@pytest.mark.parametrize(
    "algorithm, config_overrides, env, framework, n_iter, threshold",
    test_convergence_params,
)
def test_algorithms_can_converge_with_different_frameworks(
    algorithm: Algorithm,
    config_overrides: dict,
    env: str,
    framework: Framework,
    n_iter: int,
    threshold: float,
    trainer: Trainer,
):
    """I should be able to train an algorithm to convergence with the following
    frameworks:
        1. TensorFlow (Graph Mode)
        2. TensorFlow (Eager Mode)
        3. PyTorch
    NOTE: Not all algorithms have been implemented in all frameworks.
    """
    results = None
    episode_reward_mean = -float("inf")
    all_rewards_mean = []
    all_rewards_max = []
    all_rewards_min = []
    for i in range(n_iter):
        results = trainer.train()
        episode_reward_mean = (results["evaluation"]["episode_reward_mean"]
                               if ("evaluation_interval" in config_overrides
                                   and config_overrides["evaluation_interval"])
                               else results["episode_reward_mean"])
        logger.warning(f"Train call {i} with reward {episode_reward_mean}")
        print(f"Train call {i} with reward {episode_reward_mean}")
        all_rewards_mean.append(episode_reward_mean)
        episode_reward_max = (results["evaluation"]["episode_reward_max"]
                               if ("evaluation_interval" in config_overrides
                                   and config_overrides["evaluation_interval"])
                               else results["episode_reward_max"])
        all_rewards_max.append(episode_reward_max)
        episode_reward_min = (results["evaluation"]["episode_reward_min"]
                              if ("evaluation_interval" in config_overrides
                                  and config_overrides["evaluation_interval"])
                              else results["episode_reward_min"])
        all_rewards_min.append(episode_reward_min)
    logger.warning(f"All rewards mean:\n{all_rewards_mean}")
    print(f"All rewards mean:\n{all_rewards_mean}")
    logger.warning(f"All rewards min:\n{all_rewards_min}")
    print(f"All rewards min:\n{all_rewards_min}")
    logger.warning(f"All rewards max:\n{all_rewards_max}")
    print(f"All rewards max:\n{all_rewards_max}")
    if n_iter >= 1:
        assert results is not None
    if results:
        assert episode_reward_mean >= threshold


@pytest.mark.minutes
@pytest.mark.usefixtures("ray_env")
@pytest.mark.usefixtures("using_framework")
@pytest.mark.parametrize(
    "algorithm, config_overrides, env, framework, n_iter, threshold",
    test_monotonic_convergence_params,
)
def test_monotonically_improving_algorithms_can_converge_with_different_frameworks(
    algorithm: Algorithm,
    config_overrides: dict,
    env: str,
    framework: Framework,
    n_iter: int,
    threshold: float,
    trainer: Trainer,
):
    """I should be able to train an algorithm to convergence with the following
    frameworks:
        1. TensorFlow (Graph Mode)
        2. TensorFlow (Eager Mode)
        3. PyTorch
    NOTE: Not all algorithms have been implemented in all frameworks.
    NOTE: For monotonically improving algorithms (like PPO), its enough to stop training
    after the episode reward mean of an epoch exceeds the set threshold, even if we
    haven't trained for n_iter number of epochs.
    """
    learnt = False
    episode_reward_mean = -float("inf")
    all_rewards = []
    for i in range(n_iter):
        results = trainer.train()
        episode_reward_mean = (results["evaluation"]["episode_reward_mean"]
                               if ("evaluation_interval" in config_overrides
                                   and config_overrides["evaluation_interval"])
                               else results["episode_reward_mean"])
        logger.warning(f"Train call {i} with reward {episode_reward_mean} and "
                    f"Metrics:\n{results}")
        all_rewards.append(episode_reward_mean)
        if episode_reward_mean >= threshold:
            learnt = True
            break
    logger.warning(f"All rewards:\n{all_rewards}")
    print(f"All rewards:\n{all_rewards}")

    assert learnt, f"{episode_reward_mean} < {threshold}"
