import numpy as np
import scipy.signal
from typing import Dict, Optional

from ray.rllib.evaluation.episode import Episode
from ray.rllib.policy.policy import Policy
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.annotations import DeveloperAPI, OldAPIStack
from ray.rllib.utils.numpy import convert_to_numpy
from ray.rllib.utils.torch_utils import convert_to_torch_tensor
from ray.rllib.utils.typing import AgentID
from ray.rllib.utils.typing import TensorType


@DeveloperAPI
class Postprocessing:
    """Constant definitions for postprocessing."""

    ADVANTAGES = "advantages"
    VALUE_TARGETS = "value_targets"


@OldAPIStack
def adjust_nstep(n_step: int, gamma: float, batch: SampleBatch) -> None:
    """Rewrites `batch` to encode n-step rewards, terminateds, truncateds, and next-obs.

    Observations and actions remain unaffected. At the end of the trajectory,
    n is truncated to fit in the traj length.

    Args:
        n_step: The number of steps to look ahead and adjust.
        gamma: The discount factor.
        batch: The SampleBatch to adjust (in place).

    Examples:
        n-step=3
        Trajectory=o0 r0 d0, o1 r1 d1, o2 r2 d2, o3 r3 d3, o4 r4 d4=True o5
        gamma=0.9
        Returned trajectory:
        0: o0 [r0 + 0.9*r1 + 0.9^2*r2 + 0.9^3*r3] d3 o0'=o3
        1: o1 [r1 + 0.9*r2 + 0.9^2*r3 + 0.9^3*r4] d4 o1'=o4
        2: o2 [r2 + 0.9*r3 + 0.9^2*r4] d4 o1'=o5
        3: o3 [r3 + 0.9*r4] d4 o3'=o5
        4: o4 r4 d4 o4'=o5
    """

    assert (
        batch.is_single_trajectory()
    ), "Unexpected terminated|truncated in middle of trajectory!"

    len_ = len(batch)

    # Shift NEXT_OBS, TERMINATEDS, and TRUNCATEDS.
    batch[SampleBatch.NEXT_OBS] = np.concatenate(
        [
            batch[SampleBatch.OBS][n_step:],
            np.stack([batch[SampleBatch.NEXT_OBS][-1]] * min(n_step, len_)),
        ],
        axis=0,
    )
    batch[SampleBatch.TERMINATEDS] = np.concatenate(
        [
            batch[SampleBatch.TERMINATEDS][n_step - 1 :],
            np.tile(batch[SampleBatch.TERMINATEDS][-1], min(n_step - 1, len_)),
        ],
        axis=0,
    )
    # Only fix `truncateds`, if present in the batch.
    if SampleBatch.TRUNCATEDS in batch:
        batch[SampleBatch.TRUNCATEDS] = np.concatenate(
            [
                batch[SampleBatch.TRUNCATEDS][n_step - 1 :],
                np.tile(batch[SampleBatch.TRUNCATEDS][-1], min(n_step - 1, len_)),
            ],
            axis=0,
        )

    # Change rewards in place.
    for i in range(len_):
        for j in range(1, n_step):
            if i + j < len_:
                batch[SampleBatch.REWARDS][i] += (
                    gamma**j * batch[SampleBatch.REWARDS][i + j]
                )


@OldAPIStack
def compute_advantages(
    rollout: SampleBatch,
    last_r: float,
    gamma: float = 0.9,
    lambda_: float = 1.0,
    use_gae: bool = True,
    use_critic: bool = True,
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
        rewards: Override the reward values in rollout.
        vf_preds: Override the value function predictions in rollout.

    Returns:
        SampleBatch with experience from rollout and processed rewards.
    """
    assert (
        SampleBatch.VF_PREDS in rollout or not use_critic
    ), "use_critic=True but values not found"
    assert use_critic or not use_gae, "Can't use gae without using a value function"
    last_r = convert_to_numpy(last_r)

    if rewards is None:
        rewards = rollout[SampleBatch.REWARDS]
    if vf_preds is None and use_critic:
        vf_preds = rollout[SampleBatch.VF_PREDS]

    if use_gae:
        vpred_t = np.concatenate([vf_preds, np.array([last_r])])
        delta_t = rewards + gamma * vpred_t[1:] - vpred_t[:-1]
        # This formula for the advantage comes from:
        # "Generalized Advantage Estimation": https://arxiv.org/abs/1506.02438
        rollout[Postprocessing.ADVANTAGES] = discount_cumsum(delta_t, gamma * lambda_)
        rollout[Postprocessing.VALUE_TARGETS] = (
            rollout[Postprocessing.ADVANTAGES] + vf_preds
        ).astype(np.float32)
    else:
        rewards_plus_v = np.concatenate([rewards, np.array([last_r])])
        discounted_returns = discount_cumsum(rewards_plus_v, gamma)[:-1].astype(
            np.float32
        )

        if use_critic:
            rollout[Postprocessing.ADVANTAGES] = discounted_returns - vf_preds
            rollout[Postprocessing.VALUE_TARGETS] = discounted_returns
        else:
            rollout[Postprocessing.ADVANTAGES] = discounted_returns
            rollout[Postprocessing.VALUE_TARGETS] = np.zeros_like(
                rollout[Postprocessing.ADVANTAGES]
            )

    rollout[Postprocessing.ADVANTAGES] = rollout[Postprocessing.ADVANTAGES].astype(
        np.float32
    )

    return rollout


@OldAPIStack
def compute_gae_for_sample_batch(
    policy: Policy,
    sample_batch: SampleBatch,
    other_agent_batches: Optional[Dict[AgentID, SampleBatch]] = None,
    episode: Optional[Episode] = None,
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
    sample_batch = compute_bootstrap_value(sample_batch, policy)

    vf_preds = np.array(sample_batch[SampleBatch.VF_PREDS])
    rewards = np.array(sample_batch[SampleBatch.REWARDS])
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
        last_r=sample_batch[SampleBatch.VALUES_BOOTSTRAPPED][-1],
        gamma=policy.config["gamma"],
        lambda_=policy.config["lambda"],
        use_gae=policy.config["use_gae"],
        use_critic=policy.config.get("use_critic", True),
        vf_preds=vf_preds,
        rewards=rewards,
    )

    if squeezed:
        # If we needed to squeeze rewards and vf_preds, we need to unsqueeze
        # advantages again for it to have the same shape
        batch[Postprocessing.ADVANTAGES] = np.expand_dims(
            batch[Postprocessing.ADVANTAGES], axis=1
        )

    return batch


@OldAPIStack
def compute_bootstrap_value(sample_batch: SampleBatch, policy: Policy) -> SampleBatch:
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
        if policy.config.get("enable_rl_module_and_learner"):
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
            fwd_out = policy.model.forward_exploration(input_dict)
            # For recurrent models, we need to remove the time dimension.
            fwd_out = policy.maybe_remove_time_dimension(fwd_out)
            last_r = fwd_out[SampleBatch.VF_PREDS][-1]
        else:
            last_r = policy._value(**input_dict)

    vf_preds = np.array(sample_batch[SampleBatch.VF_PREDS])
    # We need to squeeze out the time dimension if there is one
    if len(vf_preds.shape) == 2:
        vf_preds = np.squeeze(vf_preds, axis=1)
        squeezed = True
    else:
        squeezed = False

    # Set the SampleBatch.VALUES_BOOTSTRAPPED field to VF_PREDS[1:] + the
    # very last timestep (where this bootstrapping value is actually needed), which
    # we set to the computed `last_r`.
    sample_batch[SampleBatch.VALUES_BOOTSTRAPPED] = np.concatenate(
        [
            convert_to_numpy(vf_preds[1:]),
            np.array([convert_to_numpy(last_r)], dtype=np.float32),
        ],
        axis=0,
    )

    if squeezed:
        sample_batch[SampleBatch.VF_PREDS] = np.expand_dims(vf_preds, axis=1)
        sample_batch[SampleBatch.VALUES_BOOTSTRAPPED] = np.expand_dims(
            sample_batch[SampleBatch.VALUES_BOOTSTRAPPED], axis=1
        )

    return sample_batch


@OldAPIStack
def discount_cumsum(x: np.ndarray, gamma: float) -> np.ndarray:
    """Calculates the discounted cumulative sum over a reward sequence `x`.

    y[t] - discount*y[t+1] = x[t]
    reversed(y)[t] - discount*reversed(y)[t-1] = reversed(x)[t]

    Args:
        gamma: The discount factor gamma.

    Returns:
        The sequence containing the discounted cumulative sums
        for each individual reward in `x` till the end of the trajectory.

     .. testcode::
        :skipif: True

        x = np.array([0.0, 1.0, 2.0, 3.0])
        gamma = 0.9
        discount_cumsum(x, gamma)

    .. testoutput::

        array([0.0 + 0.9*1.0 + 0.9^2*2.0 + 0.9^3*3.0,
               1.0 + 0.9*2.0 + 0.9^2*3.0,
               2.0 + 0.9*3.0,
               3.0])
    """
    return scipy.signal.lfilter([1], [1, float(-gamma)], x[::-1], axis=0)[::-1]
