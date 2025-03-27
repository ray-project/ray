from typing import Dict, Optional

import numpy as np

from ray.rllib.algorithms.ppo.ppo_learner import (
    LEARNER_RESULTS_VF_EXPLAINED_VAR_KEY,
    LEARNER_RESULTS_VF_LOSS_UNCLIPPED_KEY,
)
from ray.rllib.core.learner.learner import VF_LOSS_KEY
from ray.rllib.evaluation.episode import Episode
from ray.rllib.evaluation.postprocessing import discount_cumsum
from ray.rllib.policy.policy import Policy
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.annotations import DeveloperAPI
from ray.rllib.utils.nested_dict import NestedDict
from ray.rllib.utils.numpy import convert_to_numpy
from ray.rllib.utils.torch_utils import convert_to_torch_tensor
from ray.rllib.utils.typing import AgentID, TensorType


class Postprocessing:
    """Abstract class for postprocessing"""


class RewardValuePostprocessing(Postprocessing):
    ADVANTAGES = "advantages"
    VALUE_TARGETS = "value_targets"
    REWARDS = SampleBatch.REWARDS
    VF_PREDS = SampleBatch.VF_PREDS
    VALUES_BOOTSTRAPPED = SampleBatch.VALUES_BOOTSTRAPPED
    VF_LOSS_KEY = VF_LOSS_KEY
    RETURNS = "accumulated_rewards"
    LEARNER_RESULTS_VF_LOSS_UNCLIPPED_KEY = LEARNER_RESULTS_VF_LOSS_UNCLIPPED_KEY
    LEARNER_RESULTS_VF_EXPLAINED_VAR_KEY = LEARNER_RESULTS_VF_EXPLAINED_VAR_KEY


class CostValuePostprocessing(Postprocessing):
    ADVANTAGES = "cost_advantages"
    VALUE_TARGETS = "cost_value_targets"
    REWARDS = "costs"
    VF_PREDS = "cvf_preds"
    VALUES_BOOTSTRAPPED = "cost_values_bootstrapped"
    VF_LOSS_KEY = "cost_" + VF_LOSS_KEY
    RETURNS = "accumulated_costs"
    LEARNER_RESULTS_VF_LOSS_UNCLIPPED_KEY = (
        "cost_" + LEARNER_RESULTS_VF_LOSS_UNCLIPPED_KEY
    )
    LEARNER_RESULTS_VF_EXPLAINED_VAR_KEY = (
        "cost_" + LEARNER_RESULTS_VF_EXPLAINED_VAR_KEY
    )


@DeveloperAPI
def compute_advantages(
    rollout: SampleBatch,
    last_r: float,
    gamma: float = 0.9,
    lambda_: float = 1.0,
    use_gae: bool = True,
    use_critic: bool = True,
    post_process: Postprocessing = None,
    rewards: TensorType = None,
    vf_preds: TensorType = None,
):
    """Given a rollout, compute its value targets and the advantages.

    Args:
        rollout: SampleBatch of a single trajectory.
        last_r: Value estimation for last observation.
        gamma: Discount factor.
        lambda_: Parameter for GAE.
        use_gae: Using Generalized Advantage Estimation.
        use_critic: Whether to use critic (value estimates). Setting
            this to False will use 0 as baseline.
        post_process: Postprocessing class can be Reward or Cost
            postprocessing
        rewards: Override the reward values in rollout.
        vf_preds: Override the value function predictions in rollout.

    Returns:
        SampleBatch with experience from rollout and processed rewards.
    """
    assert (
        post_process.VF_PREDS in rollout or not use_critic
    ), "use_critic=True but values not found"
    assert use_critic or not use_gae, "Can't use gae without using a value function"
    last_r = convert_to_numpy(last_r)

    if rewards is None:
        rewards = rollout[post_process.REWARDS]
    if vf_preds is None and use_critic:
        vf_preds = rollout[post_process.VF_PREDS]

    # computing accumulated returns
    rewards_plus_v = np.concatenate([rewards, np.array([last_r])])
    accumulated_returns = discount_cumsum(rewards_plus_v, 1.0)[:-1].astype(np.float32)
    rollout[post_process.RETURNS] = accumulated_returns[0] * np.ones_like(
        rewards
    )  # needed for RNN

    if use_gae:
        vpred_t = np.concatenate([vf_preds, np.array([last_r])])
        delta_t = rewards + gamma * vpred_t[1:] - vpred_t[:-1]
        # This formula for the advantage comes from:
        # "Generalized Advantage Estimation": https://arxiv.org/abs/1506.02438
        rollout[post_process.ADVANTAGES] = discount_cumsum(delta_t, gamma * lambda_)
        rollout[post_process.VALUE_TARGETS] = (
            rollout[post_process.ADVANTAGES] + vf_preds
        ).astype(np.float32)
    else:
        discounted_returns = discount_cumsum(rewards_plus_v, gamma)[:-1].astype(
            np.float32
        )
        if use_critic:
            rollout[post_process.ADVANTAGES] = discounted_returns - vf_preds
            rollout[post_process.VALUE_TARGETS] = discounted_returns
        else:
            rollout[post_process.ADVANTAGES] = discounted_returns
            rollout[post_process.VALUE_TARGETS] = np.zeros_like(
                rollout[post_process.ADVANTAGES]
            )

    rollout[post_process.ADVANTAGES] = rollout[post_process.ADVANTAGES].astype(
        np.float32
    )

    return rollout


@DeveloperAPI
def compute_gae_for_sample_batch(
    policy: Policy,
    sample_batch: SampleBatch,
    other_agent_batches: Optional[Dict[AgentID, SampleBatch]] = None,
    episode: Optional[Episode] = None,
):
    sample_batch = compute_cost_gae_for_sample_batch(
        policy=policy,
        sample_batch=sample_batch,
        other_agent_batches=other_agent_batches,
        episode=episode,
        post_process=RewardValuePostprocessing,
        gamma=policy.config["gamma"],
        lambda_=policy.config["lambda"],
        use_gae=policy.config["use_gae"],
        use_critic=policy.config.get("use_critic", True),
    )
    # adding costs to the sample batch if they exist
    sample_batch[CostValuePostprocessing.REWARDS] = np.array(
        [
            info.get("cost", 0) if type(info) == dict else info
            for info in sample_batch["infos"]
        ]
    )

    sample_batch = compute_cost_gae_for_sample_batch(
        policy=policy,
        sample_batch=sample_batch,
        other_agent_batches=other_agent_batches,
        episode=episode,
        post_process=CostValuePostprocessing,
        gamma=policy.config["cost_gamma"],
        lambda_=policy.config["cost_lambda_"],
        use_gae=policy.config["use_cost_gae"],
        use_critic=policy.config["use_cost_critic"],
    )
    return sample_batch


@DeveloperAPI
def compute_cost_gae_for_sample_batch(
    policy: Policy,
    sample_batch: SampleBatch,
    other_agent_batches: Optional[Dict[AgentID, SampleBatch]] = None,
    episode: Optional[Episode] = None,
    post_process: Postprocessing = None,
    gamma: float = 0.9,
    lambda_: float = 1.0,
    use_gae: bool = True,
    use_critic: bool = True,
) -> SampleBatch:
    """Adds GAE (generalized advantage estimations) to a trajectory.

    The trajectory contains only data from one episode and from one agent.
    - If  `config.batch_mode=truncate_episodes` (default), sample_batch may
    contain a truncated (at-the-end) episode, in case the
    `config.rollout_fragment_length` was reached by the sampler.
    - If `config.batch_mode=complete_episodes`, sample_batch will contain
    exactly one episode (no matter how long).
    New columns can be added to sample_batch and existing ones may be altered.

    Args:
        policy: The Policy used to generate the trajectory (`sample_batch`)
        sample_batch: The SampleBatch to postprocess.
        other_agent_batches: Optional dict of AgentIDs mapping to other
            agents' trajectory data (from the same episode).
            NOTE: The other agents use the same policy.
        episode: Optional multi-agent episode object in which the agents
            operated.

    Returns:
        The postprocessed, modified SampleBatch (or a new one).
    """

    # Compute the SampleBatch.VALUES_BOOTSTRAPPED column, which we'll need for the
    # following `last_r` arg in `compute_advantages()`.
    sample_batch = compute_bootstrap_value(
        sample_batch, policy, post_process=post_process
    )

    vf_preds = np.array(sample_batch[post_process.VF_PREDS])
    rewards = np.array(sample_batch[post_process.REWARDS])

    # We need to squeeze out the time dimension if there is one
    # Sanity check that both have the same shape
    if len(vf_preds.shape) == 2:
        assert vf_preds.shape == rewards.shape
        vf_preds = np.squeeze(vf_preds, axis=1)
        rewards = np.squeeze(rewards, axis=1)
        squeezed = True
    else:
        squeezed = False

    # Adds the policy logits, VF preds, and advantages to the batch,
    # using GAE ("generalized advantage estimation") or not.
    batch = compute_advantages(
        rollout=sample_batch,
        last_r=sample_batch[post_process.VALUES_BOOTSTRAPPED][-1],
        gamma=gamma,
        lambda_=lambda_,
        use_gae=use_gae,
        use_critic=use_critic,
        vf_preds=vf_preds,
        rewards=rewards,
        post_process=post_process,
    )

    if squeezed:
        # If we needed to squeeze rewards and vf_preds, we need to unsqueeze
        # advantages again for it to have the same shape
        batch[post_process.ADVANTAGES] = np.expand_dims(
            batch[post_process.ADVANTAGES], axis=1
        )

    return batch


@DeveloperAPI
def compute_bootstrap_value(
    sample_batch: SampleBatch, policy: Policy, post_process: Postprocessing
) -> SampleBatch:
    """Performs a value function computation at the end of a trajectory.

    If the trajectory is terminated (not truncated), will not use the value function,
    but assume that the value of the last timestep is 0.0.
    In all other cases, will use the given policy's value function to compute the
    "bootstrapped" value estimate at the end of the given trajectory. To do so, the
    very last observation (sample_batch[NEXT_OBS][-1]) and - if applicable -
    the very last state output (sample_batch[STATE_OUT][-1]) wil be used as inputs to
    the value function.

    The thus computed value estimate will be stored in a new column of the
    `sample_batch`: SampleBatch.VALUES_BOOTSTRAPPED. Thereby, values at all timesteps
    in this column are set to 0.0, except or the last timestep, which receives the
    computed bootstrapped value.
    This is done, such that in any loss function (which processes raw, intact
    trajectories, such as those of IMPALA and APPO) can use this new column as follows:

    Example: numbers=ts in episode, '|'=episode boundary (terminal),
    X=bootstrapped value (!= 0.0 b/c ts=12 is not a terminal).
    ts=5 is NOT a terminal.
    T:                     8   9  10  11  12 <- no terminal
    VF_PREDS:              .   .   .   .   .
    VALUES_BOOTSTRAPPED:   0   0   0   0   X

    Args:
        sample_batch: The SampleBatch (single trajectory) for which to compute the
            bootstrap value at the end. This SampleBatch will be altered in place
            (by adding a new column: SampleBatch.VALUES_BOOTSTRAPPED).
        policy: The Policy object, whose value function to use.

    Returns:
         The altered SampleBatch (with the extra SampleBatch.VALUES_BOOTSTRAPPED
         column).
    """
    # Trajectory is actually complete -> last r=0.0.
    if sample_batch[SampleBatch.TERMINATEDS][-1]:
        last_r = 0.0
    # Trajectory has been truncated -> last r=VF estimate of last obs.
    else:
        # Input dict is provided to us automatically via the Model's
        # requirements. It's a single-timestep (last one in trajectory)
        # input_dict.
        # Create an input dict according to the Policy's requirements.
        input_dict = sample_batch.get_single_step_input_dict(
            policy.view_requirements, index="last"
        )
        if policy.config.get("_enable_rl_module_api"):
            # Note: During sampling you are using the parameters at the beginning of
            # the sampling process. If I'll be using this advantages during training
            # should it not be the latest parameters during training for this to be
            # correct? Does this mean that I need to preserve the trajectory
            # information during training and compute the advantages inside the loss
            # function?
            # TODO (Kourosh): Another thing we need to figure out is which end point
            #  to call here (why forward_exploration)? What if this method is getting
            #  called inside the learner loop or via another abstraction like
            #  RLSampler.postprocess_trajectory() which is non-batched cpu/gpu task
            #  running across different processes for different trajectories?
            #  This implementation right now will compute even the action_dist which
            #  will not be needed but takes time to compute.
            if policy.framework == "torch":
                input_dict = convert_to_torch_tensor(input_dict, device=policy.device)

            # For recurrent models, we need to add a time dimension.
            input_dict = policy.maybe_add_time_dimension(
                input_dict, seq_lens=input_dict[SampleBatch.SEQ_LENS]
            )
            input_dict = NestedDict(input_dict)
            fwd_out = policy.model.forward_exploration(input_dict)
            # For recurrent models, we need to remove the time dimension.
            fwd_out = policy.maybe_remove_time_dimension(fwd_out)
            last_r = fwd_out[post_process.VF_PREDS][-1]
        else:
            last_r = policy._value(**input_dict)

    vf_preds = np.array(sample_batch[post_process.VF_PREDS])
    # We need to squeeze out the time dimension if there is one
    if len(vf_preds.shape) == 2:
        vf_preds = np.squeeze(vf_preds, axis=1)
        squeezed = True
    else:
        squeezed = False

    # Set the SampleBatch.VALUES_BOOTSTRAPPED field to VF_PREDS[1:] + the
    # very last timestep (where this bootstrapping value is actually needed), which
    # we set to the computed `last_r`.
    sample_batch[post_process.VALUES_BOOTSTRAPPED] = np.concatenate(
        [
            convert_to_numpy(vf_preds[1:]),
            np.array([convert_to_numpy(last_r)], dtype=np.float32),
        ],
        axis=0,
    )

    if squeezed:
        sample_batch[post_process.VF_PREDS] = np.expand_dims(vf_preds, axis=1)
        sample_batch[post_process.VALUES_BOOTSTRAPPED] = np.expand_dims(
            sample_batch[post_process.VALUES_BOOTSTRAPPED], axis=1
        )

    return sample_batch
