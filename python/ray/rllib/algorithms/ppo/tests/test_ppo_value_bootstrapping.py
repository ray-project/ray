"""Unit tests for PPO's value-bootstrapping wiring.

Exercises the connector pipeline (``AddOneTsToEpisodesAndTruncate`` +
``AddColumnsFromEpisodesToTrainBatch`` + ``BatchIndividualItems``) feeding into
``compute_value_targets``. Targets are pinned to closed-form GAE answers so a
regression in either the connector layout or the GAE recursion is caught.
"""

import numpy as np
import pytest

from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.connectors.env_to_module import FlattenObservations
from ray.rllib.connectors.learner import (
    AddColumnsFromEpisodesToTrainBatch,
    AddOneTsToEpisodesAndTruncate,
    BatchIndividualItems,
    LearnerConnectorPipeline,
)
from ray.rllib.connectors.learner.general_advantage_estimation import (
    GeneralAdvantageEstimation,
)
from ray.rllib.core import DEFAULT_MODULE_ID
from ray.rllib.core.columns import Columns
from ray.rllib.core.rl_module.apis.value_function_api import ValueFunctionAPI
from ray.rllib.core.rl_module.default_model_config import DefaultModelConfig
from ray.rllib.core.rl_module.multi_rl_module import MultiRLModule
from ray.rllib.core.rl_module.torch.torch_rl_module import TorchRLModule
from ray.rllib.env.single_agent_episode import SingleAgentEpisode
from ray.rllib.evaluation.postprocessing import Postprocessing
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.postprocessing.value_predictions import compute_value_targets
from ray.rllib.utils.postprocessing.zero_padding import unpad_data_if_necessary
from ray.rllib.utils.torch_utils import convert_to_torch_tensor

torch, _ = try_import_torch()


@pytest.mark.parametrize(
    "mask,expected",
    [
        ([True, True, False], [-1.0, 1.0, -2.0]),
        (None, [-0.26726124, 1.3363062, -1.069045]),
        ([False, False, False], [0.0, 0.0, 0.0]),
    ],
)
@pytest.mark.parametrize("stateful", [False, True])
def test_gae_normalization_excludes_bootstrap_timestep(mask, expected, stateful):
    class ValueModule(TorchRLModule, ValueFunctionAPI):
        def compute_values(self, batch, embeddings=None):
            return torch.zeros((1, 4) if stateful else (3,))

        def is_stateful(self):
            return stateful

    episode = SingleAgentEpisode(
        observations=[0, 1, 2, 3],
        actions=[0, 0, 0],
        rewards=[1.0, 3.0, 0.0],
        len_lookback_buffer=0,
    )
    batch = {
        DEFAULT_MODULE_ID: {
            Columns.REWARDS: np.array([1.0, 3.0, 0.0]),
            Columns.TERMINATEDS: np.array([False, True, False]),
            Columns.TRUNCATEDS: np.zeros(3, dtype=bool),
        }
    }
    if mask is not None:
        batch[DEFAULT_MODULE_ID][Columns.LOSS_MASK] = np.array(mask)
    if stateful:
        batch[DEFAULT_MODULE_ID] = {
            key: np.pad(value, (0, 1))[None, :]
            for key, value in batch[DEFAULT_MODULE_ID].items()
        }
    connector = GeneralAdvantageEstimation(gamma=0.0, lambda_=0.0)
    modules = MultiRLModule()
    modules.add_module(DEFAULT_MODULE_ID, ValueModule(model_config={"max_seq_len": 4}))
    result = connector(rl_module=modules, episodes=[episode], batch=batch)
    advantages = result[DEFAULT_MODULE_ID][Postprocessing.ADVANTAGES]
    np.testing.assert_allclose(advantages.reshape(-1)[:3], expected, atol=1e-6)
    if stateful:
        assert advantages[0, 3] == 0.0


def test_gae_normalization_excludes_padding_across_episode_chunks():
    class ValueModule(TorchRLModule, ValueFunctionAPI):
        def compute_values(self, batch, embeddings=None):
            return torch.zeros((3, 2))

        def is_stateful(self):
            return True

    episodes = [
        SingleAgentEpisode(
            observations=[0, 1, 2],
            actions=[0, 0],
            rewards=[1.0, 0.0],
            len_lookback_buffer=0,
        ),
        SingleAgentEpisode(
            observations=[0, 1, 2, 3],
            actions=[0, 0, 0],
            rewards=[3.0, 5.0, 0.0],
            len_lookback_buffer=0,
        ),
    ]
    batch = {
        DEFAULT_MODULE_ID: {
            Columns.REWARDS: np.array([[1.0, 0.0], [3.0, 5.0], [0.0, 0.0]]),
            Columns.TERMINATEDS: np.zeros((3, 2), dtype=bool),
            Columns.TRUNCATEDS: np.zeros((3, 2), dtype=bool),
            Columns.LOSS_MASK: np.array([[True, False], [True, True], [False, False]]),
        }
    }
    modules = MultiRLModule()
    modules.add_module(DEFAULT_MODULE_ID, ValueModule(model_config={"max_seq_len": 2}))
    connector = GeneralAdvantageEstimation(gamma=0.0, lambda_=0.0)
    result = connector(rl_module=modules, episodes=episodes, batch=batch)
    advantages = result[DEFAULT_MODULE_ID][Postprocessing.ADVANTAGES]
    valid = advantages[torch.tensor([[True, False], [True, True], [False, False]])]
    np.testing.assert_allclose(valid, [-np.sqrt(1.5), 0.0, np.sqrt(1.5)], atol=1e-6)


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


# 2x2 deterministic FrozenLake used for the end-to-end convergence check below:
#   row 0: S F   states 0, 1
#   row 1: H G   states 2, 3
# Reward 1.0 at G; episodes terminate at H or G.
# Optimal policy from S: right (to F=1), then down (to G=3, reward=1).
# Bellman closed form with gamma=0.99 on the non-terminal states:
#   V(F) = 1 + gamma * V(G_terminal) = 1.0
#   V(S) = 0 + gamma * V(F)          = 0.99
# V on the terminal states (H, G) is never targeted during training and is
# therefore left out of the comparison.
_FROZEN_LAKE_2X2_CFG = {"desc": ["SF", "HG"], "is_slippery": False}
_TRUE_V_NON_TERMINAL = np.array([0.99, 1.0], dtype=np.float32)


def _train_and_get_state_values(gae_lambda: float, num_iters: int, seed: int):
    """Train PPO on 2x2 FrozenLake and return V for all 4 states."""
    config = (
        PPOConfig()
        .environment("FrozenLake-v1", env_config=_FROZEN_LAKE_2X2_CFG)
        .env_runners(
            num_env_runners=0,
            num_envs_per_env_runner=4,
            # Discrete obs -> one-hot for the FC encoder.
            env_to_module_connector=(lambda env, spaces, device: FlattenObservations()),
        )
        .training(
            gamma=0.99,
            lambda_=gae_lambda,
            lr=3e-3,
            train_batch_size=256,
            num_epochs=10,
            minibatch_size=64,
            # Up-weight the value loss and disable entropy so V converges
            # quickly and the test stays short.
            vf_loss_coeff=1.0,
            entropy_coeff=0.0,
        )
        .rl_module(
            model_config=DefaultModelConfig(
                fcnet_hiddens=[32],
                fcnet_activation="tanh",
            ),
        )
        .debugging(seed=seed)
    )
    algo = config.build_algo()

    for _ in range(num_iters):
        algo.train()
    # `algo.get_module(...)` returns the EnvRunner's inference-only
    # module (no critic). Reach into the Learner's module to call
    # compute_values.
    learner_module = algo.learner_group._learner.module[DEFAULT_MODULE_ID]
    obs = np.eye(4, dtype=np.float32)  # one-hot for each of the 4 states
    with torch.no_grad():
        return (
            learner_module.compute_values({Columns.OBS: convert_to_torch_tensor(obs)})
            .detach()
            .cpu()
            .numpy()
        )


def test_value_function_converges_across_gae_lambda():
    """
    End-to-end check that PPO trains a consistent V across `gae_lambda`.
    Different lambda values should converge to the same fixed-point V.
    """
    v_by_lambda = {
        lam: _train_and_get_state_values(gae_lambda=lam, num_iters=40, seed=42)
        for lam in [0.0, 0.9, 1.0]
    }
    # 1) V on the visited (non-terminal) states matches the analytic V.
    for lam, v in v_by_lambda.items():
        np.testing.assert_allclose(
            v[:2],
            _TRUE_V_NON_TERMINAL,
            atol=0.05,
            err_msg=(
                f"V on non-terminal states diverged from analytic V "
                f"for lambda={lam}: got {v[:2]}, expected "
                f"{_TRUE_V_NON_TERMINAL}"
            ),
        )
    # 2) V across the three lambdas must converge together.
    assert np.ptp([v[:2] for v in v_by_lambda.values()], axis=0).max() < 0.05


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
