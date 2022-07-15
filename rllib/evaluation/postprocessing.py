import numpy as np
import scipy.signal
from typing import Dict, Optional

from ray.rllib.evaluation.episode import Episode
from ray.rllib.policy.policy import Policy
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.annotations import DeveloperAPI
from ray.rllib.utils.typing import AgentID


@DeveloperAPI
class Postprocessing:
    """Constant definitions for postprocessing."""

    ADVANTAGES = "advantages"
    VALUE_TARGETS = "value_targets"


@DeveloperAPI
def adjust_nstep(n_step: int, gamma: float, batch: SampleBatch) -> None:
    """Rewrites `batch` to encode n-step rewards, dones, and next-obs.

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

    assert not any(
        batch[SampleBatch.DONES][:-1]
    ), "Unexpected done in middle of trajectory!"

    len_ = len(batch)

    # Shift NEXT_OBS and DONES.
    batch[SampleBatch.NEXT_OBS] = np.concatenate(
        [
            batch[SampleBatch.OBS][n_step:],
            np.stack([batch[SampleBatch.NEXT_OBS][-1]] * min(n_step, len_)),
        ],
        axis=0,
    )
    batch[SampleBatch.DONES] = np.concatenate(
        [
            batch[SampleBatch.DONES][n_step - 1 :],
            np.tile(batch[SampleBatch.DONES][-1], min(n_step - 1, len_)),
        ],
        axis=0,
    )

    # Change rewards in place.
    for i in range(len_):
        for j in range(1, n_step):
            if i + j < len_:
                batch[SampleBatch.REWARDS][i] += (
                    gamma ** j * batch[SampleBatch.REWARDS][i + j]
                )


@DeveloperAPI
def compute_advantages(
    rollout: SampleBatch,
    last_r: float,
    gamma: float = 0.9,
    lambda_: float = 1.0,
    use_gae: bool = True,
    use_critic: bool = True,
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

    Returns:
        SampleBatch with experience from rollout and processed rewards.
    """

    assert (
        SampleBatch.VF_PREDS in rollout or not use_critic
    ), "use_critic=True but values not found"
    assert use_critic or not use_gae, "Can't use gae without using a value function"

    if use_gae:
        vpred_t = np.concatenate([rollout[SampleBatch.VF_PREDS], np.array([last_r])])
        delta_t = rollout[SampleBatch.REWARDS] + gamma * vpred_t[1:] - vpred_t[:-1]
        # This formula for the advantage comes from:
        # "Generalized Advantage Estimation": https://arxiv.org/abs/1506.02438
        rollout[Postprocessing.ADVANTAGES] = discount_cumsum(delta_t, gamma * lambda_)
        rollout[Postprocessing.VALUE_TARGETS] = (
            rollout[Postprocessing.ADVANTAGES] + rollout[SampleBatch.VF_PREDS]
        ).astype(np.float32)
    else:
        rewards_plus_v = np.concatenate(
            [rollout[SampleBatch.REWARDS], np.array([last_r])]
        )
        discounted_returns = discount_cumsum(rewards_plus_v, gamma)[:-1].astype(
            np.float32
        )

        if use_critic:
            rollout[Postprocessing.ADVANTAGES] = (
                discounted_returns - rollout[SampleBatch.VF_PREDS]
            )
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


@DeveloperAPI
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

    # Trajectory is actually complete -> last r=0.0.
    if sample_batch[SampleBatch.DONES][-1]:
        last_r = 0.0
    # Trajectory has been truncated -> last r=VF estimate of last obs.
    else:
        # Input dict is provided to us automatically via the Model's
        # requirements. It's a single-timestep (last one in trajectory)
        # input_dict.
        # Create an input dict according to the Model's requirements.
        input_dict = sample_batch.get_single_step_input_dict(
            policy.model.view_requirements, index="last"
        )
        last_r = policy._value(**input_dict)

    # Adds the policy logits, VF preds, and advantages to the batch,
    # using GAE ("generalized advantage estimation") or not.
    batch = compute_advantages(
        sample_batch,
        last_r,
        policy.config["gamma"],
        policy.config["lambda"],
        use_gae=policy.config["use_gae"],
        use_critic=policy.config.get("use_critic", True),
    )

    return batch


@DeveloperAPI
def discount_cumsum(x: np.ndarray, gamma: float) -> np.ndarray:
    """Calculates the discounted cumulative sum over a reward sequence `x`.

    y[t] - discount*y[t+1] = x[t]
    reversed(y)[t] - discount*reversed(y)[t-1] = reversed(x)[t]

    Args:
        gamma: The discount factor gamma.

    Returns:
        The sequence containing the discounted cumulative sums
        for each individual reward in `x` till the end of the trajectory.

    Examples:
        >>> x = np.array([0.0, 1.0, 2.0, 3.0])
        >>> gamma = 0.9
        >>> discount_cumsum(x, gamma)
        ... array([0.0 + 0.9*1.0 + 0.9^2*2.0 + 0.9^3*3.0,
        ...        1.0 + 0.9*2.0 + 0.9^2*3.0,
        ...        2.0 + 0.9*3.0,
        ...        3.0])
    """
    return scipy.signal.lfilter([1], [1, float(-gamma)], x[::-1], axis=0)[::-1]
