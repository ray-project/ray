import gym
import numpy as np

from ray.rllib import Policy
from ray.rllib.algorithms.ddpg.ddpg_tf_model import DDPGTFModel
from ray.rllib.algorithms.ddpg.ddpg_torch_model import DDPGTorchModel
from ray.rllib.algorithms.ddpg.noop_model import NoopModel, TorchNoopModel
from ray.rllib.models import ModelV2
from ray.rllib.models.catalog import ModelCatalog
from ray.rllib.utils.error import UnsupportedSpaceException


def make_ddpg_models(policy: Policy) -> ModelV2:
    if policy.config["use_state_preprocessor"]:
        default_model = None  # catalog decides
        num_outputs = 256  # arbitrary
        policy.config["model"]["no_final_linear"] = True
    else:
        default_model = (
            TorchNoopModel if policy.config["framework"] == "torch" else NoopModel
        )
        num_outputs = int(np.product(policy.observation_space.shape))

    model = ModelCatalog.get_model_v2(
        obs_space=policy.observation_space,
        action_space=policy.action_space,
        num_outputs=num_outputs,
        model_config=policy.config["model"],
        framework=policy.config["framework"],
        model_interface=(
            DDPGTorchModel if policy.config["framework"] == "torch" else DDPGTFModel
        ),
        default_model=default_model,
        name="ddpg_model",
        actor_hidden_activation=policy.config["actor_hidden_activation"],
        actor_hiddens=policy.config["actor_hiddens"],
        critic_hidden_activation=policy.config["critic_hidden_activation"],
        critic_hiddens=policy.config["critic_hiddens"],
        twin_q=policy.config["twin_q"],
        add_layer_norm=(
            policy.config["exploration_config"].get("type") == "ParameterNoise"
        ),
    )

    policy.target_model = ModelCatalog.get_model_v2(
        obs_space=policy.observation_space,
        action_space=policy.action_space,
        num_outputs=num_outputs,
        model_config=policy.config["model"],
        framework=policy.config["framework"],
        model_interface=(
            DDPGTorchModel if policy.config["framework"] == "torch" else DDPGTFModel
        ),
        default_model=default_model,
        name="target_ddpg_model",
        actor_hidden_activation=policy.config["actor_hidden_activation"],
        actor_hiddens=policy.config["actor_hiddens"],
        critic_hidden_activation=policy.config["critic_hidden_activation"],
        critic_hiddens=policy.config["critic_hiddens"],
        twin_q=policy.config["twin_q"],
        add_layer_norm=(
            policy.config["exploration_config"].get("type") == "ParameterNoise"
        ),
    )

    return model


def validate_spaces(
    policy: Policy,
    observation_space: gym.spaces.Space,
    action_space: gym.spaces.Space,
) -> None:
    if not isinstance(action_space, gym.spaces.Box):
        raise UnsupportedSpaceException(
            "Action space ({}) of {} is not supported for "
            "DDPG.".format(action_space, policy)
        )
    elif len(action_space.shape) > 1:
        raise UnsupportedSpaceException(
            "Action space ({}) of {} has multiple dimensions "
            "{}. ".format(action_space, policy, action_space.shape)
            + "Consider reshaping this into a single dimension, "
            "using a Tuple action space, or the multi-agent API."
        )
