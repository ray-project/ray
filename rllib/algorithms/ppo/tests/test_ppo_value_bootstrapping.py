"""Unit tests for PPO's value-bootstrapping wiring.

Exercises the connector pipeline (``AddOneTsToEpisodesAndTruncate`` +
``AddColumnsFromEpisodesToTrainBatch`` + ``BatchIndividualItems``) feeding into
``compute_value_targets``. Targets are pinned to closed-form GAE answers so a
regression in either the connector layout or the GAE recursion is caught.
"""
import numpy as np
import pytest

from ray.rllib.connectors.learner import (
    AddColumnsFromEpisodesToTrainBatch,
    AddOneTsToEpisodesAndTruncate,
    BatchIndividualItems,
    LearnerConnectorPipeline,
)
from ray.rllib.core.columns import Columns
from ray.rllib.env.single_agent_episode import SingleAgentEpisode
from ray.rllib.utils.postprocessing.value_predictions import compute_value_targets
from ray.rllib.utils.postprocessing.zero_padding import unpad_data_if_necessary


def _targets(per_ep_values, per_ep_rewards, terminated, truncated, gamma, lambda_):
    """Run the real learner pipeline, then ``compute_value_targets``."""
    episodes = [
        SingleAgentEpisode(
            observations=[0] * len(v),
            actions=[0] * len(r),
            rewards=r,
            terminated=t,
            truncated=u,
            len_lookback_buffer=0,
        )
        for v, r, t, u in zip(per_ep_values, per_ep_rewards, terminated, truncated)
    ]
    pipe = LearnerConnectorPipeline(
        connectors=[
            AddOneTsToEpisodesAndTruncate(),
            AddColumnsFromEpisodesToTrainBatch(),
            BatchIndividualItems(),
        ]
    )
    batch = pipe(
        episodes=episodes, batch={}, rl_module=None, explore=False, shared_data={}
    )
    lens = [len(e) for e in episodes]
    flat_values = np.array([v for vs in per_ep_values for v in vs], dtype=np.float32)
    return compute_value_targets(
        values=flat_values,
        rewards=unpad_data_if_necessary(lens, np.array(batch[Columns.REWARDS])),
        terminateds=unpad_data_if_necessary(lens, np.array(batch[Columns.TERMINATEDS])),
        truncateds=unpad_data_if_necessary(lens, np.array(batch[Columns.TRUNCATEDS])),
        gamma=gamma,
        lambda_=lambda_,
    )


# Length-2 episode, values=[0, 0.95, 0.95] (last entry is the duplicated
# bootstrap slot), rewards=[0, 1, 0], gamma=0.99.
#   terminated: target[0] = gamma*v1 + gamma*lambda*(r1 - v1) = 0.9405 + 0.0495*lambda
#               target[1] = r1 = 1.0
#   truncated:  target[0] = 0.9405 + gamma*lambda*delta_1 = 0.9405 + 0.99*lambda*0.9905
#               target[1] = r1 + gamma*v_extra = 1.9405
@pytest.mark.parametrize(
    "lambda_,is_terminated,expected",
    [
        (0.0, True, [0.9405, 1.0]),
        (0.5, True, [0.9405 + 0.99 * 0.5 * 0.05, 1.0]),
        (1.0, True, [0.99, 1.0]),
        (0.0, False, [0.9405, 1.9405]),
        (0.5, False, [0.9405 + 0.99 * 0.5 * 0.9905, 1.9405]),
        (1.0, False, [0.9405 + 0.99 * 0.9905, 1.9405]),
    ],
)
def test_single_episode_targets(lambda_, is_terminated, expected):
    """Single episode: terminal reward propagates; truncation keeps the bootstrap."""
    out = _targets(
        per_ep_values=[[0.0, 0.95, 0.95]],
        per_ep_rewards=[[0.0, 1.0]],
        terminated=[is_terminated],
        truncated=[not is_terminated],
        gamma=0.99,
        lambda_=lambda_,
    )
    np.testing.assert_allclose(out[:2], expected, atol=1e-4)


@pytest.mark.parametrize(
    "ep1_term,ep2_term",
    [(True, True), (True, False), (False, True), (False, False)],
)
def test_no_cross_episode_leak(ep1_term, ep2_term):
    """At lambda=1, episode 1's targets must not depend on episode 2."""
    pair = _targets(
        per_ep_values=[[0.0, 0.95, 0.95], [0.0, 0.95, 0.95]],
        per_ep_rewards=[[0.0, 1.0], [0.0, 1.0]],
        terminated=[ep1_term, ep2_term],
        truncated=[not ep1_term, not ep2_term],
        gamma=0.99,
        lambda_=1.0,
    )
    solo = _targets(
        per_ep_values=[[0.0, 0.95, 0.95]],
        per_ep_rewards=[[0.0, 1.0]],
        terminated=[ep1_term],
        truncated=[not ep1_term],
        gamma=0.99,
        lambda_=1.0,
    )
    np.testing.assert_allclose(pair[:2], solo[:2], atol=1e-4)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
