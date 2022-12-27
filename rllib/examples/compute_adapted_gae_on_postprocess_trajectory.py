"""
Adapted (time-dependent) GAE for PPO algorithm can be activated by setting
use_adapted_gae=True in the policy config. Additionally, it is required that
"callbacks" include the custom callback class in the Algorithm's config.
Furthermore, the env must return in its info dictionary a key-value pair of
the form "d_ts": ... where the value is the length (time) of recent agent step.

This adapted, time-dependent computation of advantages may be useful in cases
where agent's actions take various times and thus time steps are not
equidistant (https://docdro.id/400TvlR)
"""

from ray.rllib.algorithms.callbacks import DefaultCallbacks
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.evaluation.postprocessing import Postprocessing
from ray.rllib.utils.annotations import override
import numpy as np


class MyCallbacks(DefaultCallbacks):
    @override(DefaultCallbacks)
    def on_postprocess_trajectory(
        self,
        *,
        worker,
        episode,
        agent_id,
        policy_id,
        policies,
        postprocessed_batch,
        original_batches,
        **kwargs
    ):
        super().on_postprocess_trajectory(
            worker=worker,
            episode=episode,
            agent_id=agent_id,
            policy_id=policy_id,
            policies=policies,
            postprocessed_batch=postprocessed_batch,
            original_batches=original_batches,
            **kwargs
        )

        if policies[policy_id].config.get("use_adapted_gae", False):
            policy = policies[policy_id]
            assert policy.config[
                "use_gae"
            ], "Can't use adapted gae without use_gae=True!"

            info_dicts = postprocessed_batch[SampleBatch.INFOS]
            assert np.all(
                ["d_ts" in info_dict for info_dict in info_dicts]
            ), "Info dicts in sample batch must contain data 'd_ts' \
                (=ts[i+1]-ts[i] length of time steps)!"

            d_ts = np.array(
                [np.float(info_dict.get("d_ts")) for info_dict in info_dicts]
            )
            assert np.all(
                [e.is_integer() for e in d_ts]
            ), "Elements of 'd_ts' (length of time steps) must be integer!"

            # Trajectory is actually complete -> last r=0.0.
            if postprocessed_batch[SampleBatch.TERMINATEDS][-1]:
                last_r = 0.0
            # Trajectory has been truncated -> last r=VF estimate of last obs.
            else:
                # Input dict is provided to us automatically via the Model's
                # requirements. It's a single-timestep (last one in trajectory)
                # input_dict.
                # Create an input dict according to the Model's requirements.
                input_dict = postprocessed_batch.get_single_step_input_dict(
                    policy.model.view_requirements, index="last"
                )
                last_r = policy._value(**input_dict)

            gamma = policy.config["gamma"]
            lambda_ = policy.config["lambda"]

            vpred_t = np.concatenate(
                [postprocessed_batch[SampleBatch.VF_PREDS], np.array([last_r])]
            )
            delta_t = (
                postprocessed_batch[SampleBatch.REWARDS]
                + gamma**d_ts * vpred_t[1:]
                - vpred_t[:-1]
            )
            # This formula for the advantage is an adaption of
            # "Generalized Advantage Estimation"
            # (https://arxiv.org/abs/1506.02438) which accounts for time steps
            # of irregular length (see proposal here ).
            # NOTE: last time step delta is not required
            postprocessed_batch[
                Postprocessing.ADVANTAGES
            ] = generalized_discount_cumsum(delta_t, d_ts[:-1], gamma * lambda_)
            postprocessed_batch[Postprocessing.VALUE_TARGETS] = (
                postprocessed_batch[Postprocessing.ADVANTAGES]
                + postprocessed_batch[SampleBatch.VF_PREDS]
            ).astype(np.float32)

            postprocessed_batch[Postprocessing.ADVANTAGES] = postprocessed_batch[
                Postprocessing.ADVANTAGES
            ].astype(np.float32)


def generalized_discount_cumsum(
    x: np.ndarray, deltas: np.ndarray, gamma: float
) -> np.ndarray:
    """Calculates the 'time-dependent' discounted cumulative sum over a
    (reward) sequence `x`.

    Recursive equations:

    y[t] - gamma**deltas[t+1]*y[t+1] = x[t]

    reversed(y)[t] - gamma**reversed(deltas)[t-1]*reversed(y)[t-1] =
    reversed(x)[t]

    Args:
        x (np.ndarray): A sequence of rewards or one-step TD residuals.
        deltas (np.ndarray): A sequence of time step deltas (length of time
            steps).
        gamma: The discount factor gamma.

    Returns:
        np.ndarray: The sequence containing the 'time-dependent' discounted
            cumulative sums for each individual element in `x` till the end of
            the trajectory.

    Examples:
        >>> x = np.array([0.0, 1.0, 2.0, 3.0])
        >>> deltas = np.array([1.0, 4.0, 15.0])
        >>> gamma = 0.9
        >>> generalized_discount_cumsum(x, deltas, gamma)
        ... array([0.0 + 0.9^1.0*1.0 + 0.9^4.0*2.0 + 0.9^15.0*3.0,
        ...        1.0 + 0.9^4.0*2.0 + 0.9^15.0*3.0,
        ...        2.0 + 0.9^15.0*3.0,
        ...        3.0])
    """
    reversed_x = x[::-1]
    reversed_deltas = deltas[::-1]
    reversed_y = np.empty_like(x)
    reversed_y[0] = reversed_x[0]
    for i in range(1, x.size):
        reversed_y[i] = (
            reversed_x[i] + gamma ** reversed_deltas[i - 1] * reversed_y[i - 1]
        )

    return reversed_y[::-1]
