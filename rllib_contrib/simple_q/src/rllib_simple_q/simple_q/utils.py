import gymnasium as gym

from ray.rllib.models.catalog import ModelCatalog
from ray.rllib.utils.error import UnsupportedSpaceException

Q_SCOPE = "q_func"
Q_TARGET_SCOPE = "target_q_func"


def make_q_models(policy):
    if not isinstance(policy.action_space, gym.spaces.Discrete):
        raise UnsupportedSpaceException(
            f"Action space {policy.action_space} is not supported for DQN."
        )

    model = ModelCatalog.get_model_v2(
        obs_space=policy.observation_space,
        action_space=policy.action_space,
        num_outputs=policy.action_space.n,
        model_config=policy.config["model"],
        framework=policy.config["framework"],
        name=Q_SCOPE,
    )

    target_model = ModelCatalog.get_model_v2(
        obs_space=policy.observation_space,
        action_space=policy.action_space,
        num_outputs=policy.action_space.n,
        model_config=policy.config["model"],
        framework=policy.config["framework"],
        name=Q_TARGET_SCOPE,
    )

    return model, target_model
