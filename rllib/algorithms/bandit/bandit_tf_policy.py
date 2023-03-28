import gymnasium as gym
import logging
import time
from typing import Dict

from gymnasium import spaces
import ray
from ray.rllib.algorithms.bandit.bandit_tf_model import (
    DiscreteLinearModelThompsonSampling,
    DiscreteLinearModelUCB,
    DiscreteLinearModel,
    ParametricLinearModelThompsonSampling,
    ParametricLinearModelUCB,
)
from ray.rllib.models.catalog import ModelCatalog
from ray.rllib.models.modelv2 import restore_original_dimensions
from ray.rllib.policy.policy import Policy
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.policy.tf_policy_template import build_tf_policy
from ray.rllib.utils.error import UnsupportedSpaceException
from ray.rllib.utils.metrics.learner_info import LEARNER_STATS_KEY
from ray.rllib.utils.tf_utils import make_tf_callable
from ray.rllib.utils.typing import TensorType, AlgorithmConfigDict
from ray.util.debug import log_once

logger = logging.getLogger(__name__)


class BanditPolicyOverrides:
    def __init__(self):
        @make_tf_callable(self.get_session())
        def learn_on_batch(postprocessed_batch) -> Dict[str, TensorType]:
            # INFOS dict can't be converted to Tensor with the interceptor.
            postprocessed_batch.set_get_interceptor(None)

            unflattened_obs = restore_original_dimensions(
                postprocessed_batch[SampleBatch.CUR_OBS],
                self.observation_space,
                self.framework,
            )

            info = {}

            start = time.time()
            self.model.partial_fit(
                unflattened_obs,
                postprocessed_batch[SampleBatch.REWARDS],
                postprocessed_batch[SampleBatch.ACTIONS],
            )

            infos = postprocessed_batch[SampleBatch.INFOS]
            if "regret" in infos[0]:
                regret = sum(
                    row["infos"]["regret"] for row in postprocessed_batch.rows()
                )
                self.regrets.append(regret)
                info["cumulative_regret"] = sum(self.regrets)
            else:
                if log_once("no_regrets"):
                    logger.warning(
                        "The env did not report `regret` values in "
                        "its `info` return, ignoring."
                    )
            info["update_latency"] = time.time() - start
            return {LEARNER_STATS_KEY: info}

        self.learn_on_batch = learn_on_batch


def validate_spaces(
    policy: Policy,
    observation_space: gym.spaces.Space,
    action_space: gym.spaces.Space,
    config: AlgorithmConfigDict,
) -> None:
    """Validates the observation- and action spaces used for the Policy.

    Args:
        policy: The policy, whose spaces are being validated.
        observation_space: The observation space to validate.
        action_space: The action space to validate.
        config: The Policy's config dict.

    Raises:
        UnsupportedSpaceException: If one of the spaces is not supported.
    """
    # Only support single Box or single Discrete spaces.
    if not isinstance(action_space, gym.spaces.Discrete):
        msg = (
            f"Action space ({action_space}) of {policy} is not supported for "
            f"Bandit algorithms. Must be `Discrete`."
        )
        # Hint at using the MultiDiscrete to Discrete wrapper for Bandits.
        if isinstance(action_space, gym.spaces.MultiDiscrete):
            msg += (
                " Try to wrap your environment with the "
                "`ray.rllib.env.wrappers.recsim::"
                "MultiDiscreteToDiscreteActionWrapper` class: `tune.register_env("
                "[some str], lambda ctx: MultiDiscreteToDiscreteActionWrapper("
                "[your gym env])); config = {'env': [some str]}`"
            )
        raise UnsupportedSpaceException(msg)


def make_model(policy, obs_space, action_space, config):
    _, logit_dim = ModelCatalog.get_action_dist(
        action_space, config["model"], framework="tf"
    )

    model_cls = DiscreteLinearModel

    if hasattr(obs_space, "original_space"):
        original_space = obs_space.original_space
    else:
        original_space = obs_space

    exploration_config = config.get("exploration_config")
    # Model is dependent on exploration strategy because of its implicitness

    # TODO: Have a separate model catalogue for bandits
    if exploration_config:
        if exploration_config["type"] == "ThompsonSampling":
            if isinstance(original_space, spaces.Dict):
                assert (
                    "item" in original_space.spaces
                ), "Cannot find 'item' key in observation space"
                model_cls = ParametricLinearModelThompsonSampling
            else:
                model_cls = DiscreteLinearModelThompsonSampling
        elif exploration_config["type"] == "UpperConfidenceBound":
            if isinstance(original_space, spaces.Dict):
                assert (
                    "item" in original_space.spaces
                ), "Cannot find 'item' key in observation space"
                model_cls = ParametricLinearModelUCB
            else:
                model_cls = DiscreteLinearModelUCB

    model = model_cls(
        obs_space, action_space, logit_dim, config["model"], name="LinearModel"
    )
    return model


def after_init(policy, *args):
    policy.regrets = []
    BanditPolicyOverrides.__init__(policy)


BanditTFPolicy = build_tf_policy(
    name="BanditTFPolicy",
    get_default_config=lambda: ray.rllib.algorithms.bandit.bandit.BanditConfig(),
    validate_spaces=validate_spaces,
    make_model=make_model,
    loss_fn=None,
    mixins=[BanditPolicyOverrides],
    after_init=after_init,
)
