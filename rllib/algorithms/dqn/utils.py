import gym
import numpy as np

from ray.rllib.algorithms.dqn.distributional_q_tf_model import DistributionalQTFModel
from ray.rllib.algorithms.dqn.dqn_torch_model import DQNTorchModel
from ray.rllib.algorithms.simple_q.utils import Q_SCOPE, Q_TARGET_SCOPE
from ray.rllib.evaluation.postprocessing import adjust_nstep
from ray.rllib.models.catalog import ModelCatalog
from ray.rllib.policy.policy import Policy
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.error import UnsupportedSpaceException
from ray.rllib.utils.exploration.parameter_noise import ParameterNoise
from ray.rllib.utils.numpy import convert_to_numpy


def make_dqn_models(policy: Policy):
    if not isinstance(policy.action_space, gym.spaces.Discrete):
        raise UnsupportedSpaceException(
            f"Action space {policy.action_space} is not supported for DQN."
        )

    if policy.config["hiddens"]:
        # try to infer the last layer size, otherwise fall back to 256
        num_outputs = ([256] + list(policy.config["model"]["fcnet_hiddens"]))[-1]
        policy.config["model"]["no_final_linear"] = True
    else:
        num_outputs = policy.action_space.n

    # TODO(sven): Move option to add LayerNorm after each Dense
    #  generically into ModelCatalog.
    add_layer_norm = (
        isinstance(getattr(policy, "exploration", None), ParameterNoise)
        or policy.config["exploration_config"]["type"] == "ParameterNoise"
    )

    model = ModelCatalog.get_model_v2(
        obs_space=policy.observation_space,
        action_space=policy.action_space,
        num_outputs=num_outputs,
        model_config=policy.config["model"],
        framework="torch" if policy.config["framework"] == "torch" else "tf",
        model_interface=DQNTorchModel
        if policy.config["framework"] == "torch"
        else DistributionalQTFModel,
        name=Q_SCOPE,
        q_hiddens=policy.config["hiddens"],
        dueling=policy.config["dueling"],
        num_atoms=policy.config["num_atoms"],
        use_noisy=policy.config["noisy"],
        v_min=policy.config["v_min"],
        v_max=policy.config["v_max"],
        sigma0=policy.config["sigma0"],
        # TODO(sven): Move option to add LayerNorm after each Dense
        #  generically into ModelCatalog.
        add_layer_norm=add_layer_norm,
    )

    policy.target_model = ModelCatalog.get_model_v2(
        obs_space=policy.observation_space,
        action_space=policy.action_space,
        num_outputs=num_outputs,
        model_config=policy.config["model"],
        framework="torch" if policy.config["framework"] == "torch" else "tf",
        model_interface=DQNTorchModel
        if policy.config["framework"] == "torch"
        else DistributionalQTFModel,
        name=Q_TARGET_SCOPE,
        q_hiddens=policy.config["hiddens"],
        dueling=policy.config["dueling"],
        num_atoms=policy.config["num_atoms"],
        use_noisy=policy.config["noisy"],
        v_min=policy.config["v_min"],
        v_max=policy.config["v_max"],
        sigma0=policy.config["sigma0"],
        # TODO(sven): Move option to add LayerNorm after each Dense
        #  generically into ModelCatalog.
        add_layer_norm=add_layer_norm,
    )

    return model


def postprocess_nstep_and_prio(
    policy: Policy, batch: SampleBatch, other_agent=None, episode=None
) -> SampleBatch:
    # N-step Q adjustments.
    if policy.config["n_step"] > 1:
        adjust_nstep(policy.config["n_step"], policy.config["gamma"], batch)

    # Create dummy prio-weights (1.0) in case we don't have any in
    # the batch.
    if SampleBatch.PRIO_WEIGHTS not in batch:
        batch[SampleBatch.PRIO_WEIGHTS] = np.ones_like(batch[SampleBatch.REWARDS])

    # Prioritize on the worker side.
    if batch.count > 0 and policy.config["replay_buffer_config"].get(
        "worker_side_prioritization", False
    ):
        td_errors = policy.compute_td_error(
            batch[SampleBatch.OBS],
            batch[SampleBatch.ACTIONS],
            batch[SampleBatch.REWARDS],
            batch[SampleBatch.NEXT_OBS],
            batch[SampleBatch.DONES],
            batch[SampleBatch.PRIO_WEIGHTS],
        )
        # Retain compatibility with old-style Replay args
        epsilon = policy.config.get("replay_buffer_config", {}).get(
            "prioritized_replay_eps"
        ) or policy.config.get("prioritized_replay_eps")
        if epsilon is None:
            raise ValueError("prioritized_replay_eps not defined in config.")

        new_priorities = np.abs(convert_to_numpy(td_errors)) + epsilon
        batch[SampleBatch.PRIO_WEIGHTS] = new_priorities

    return batch
