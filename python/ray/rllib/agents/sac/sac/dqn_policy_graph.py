import numpy as np

from ray.rllib.evaluation.sample_batch import SampleBatch

# Importance sampling weights for prioritized replay
PRIO_WEIGHTS = "weights"


def _adjust_nstep(n_step, gamma, obs, actions, rewards, new_obs, dones):
    """Rewrites the given trajectory fragments to encode n-step rewards.

    reward[i] = (
        reward[i] * gamma**0 +
        reward[i+1] * gamma**1 +
        ... +
        reward[i+n_step-1] * gamma**(n_step-1))

    The ith new_obs is also adjusted to point to the (i+n_step-1)'th new obs.

    At the end of the trajectory, n is truncated to fit in the traj length.
    """

    assert not any(dones[:-1]), "Unexpected done in middle of trajectory"

    traj_length = len(rewards)
    for i in range(traj_length):
        for j in range(1, n_step):
            if i + j < traj_length:
                new_obs[i] = new_obs[i + j]
                dones[i] = dones[i + j]
                rewards[i] += gamma ** j * rewards[i + j]


def _postprocess_dqn(policy, batch):
    # N-step Q adjustments
    if policy.config["n_step"] > 1:
        _adjust_nstep(policy.config["n_step"], policy.config["gamma"],
                      batch[SampleBatch.CUR_OBS], batch[SampleBatch.ACTIONS],
                      batch[SampleBatch.REWARDS], batch[SampleBatch.NEXT_OBS],
                      batch[SampleBatch.DONES])

    if PRIO_WEIGHTS not in batch:
        batch[PRIO_WEIGHTS] = np.ones_like(batch[SampleBatch.REWARDS])

    # Prioritize on the worker side
    if batch.count > 0 and policy.config["worker_side_prioritization"]:
        td_errors = policy.compute_td_error(
            batch[SampleBatch.CUR_OBS], batch[SampleBatch.ACTIONS],
            batch[SampleBatch.REWARDS], batch[SampleBatch.NEXT_OBS],
            batch[SampleBatch.DONES], batch[PRIO_WEIGHTS])
        new_priorities = (
            np.abs(td_errors) + policy.config["prioritized_replay_eps"])
        batch.data[PRIO_WEIGHTS] = new_priorities

    return batch

