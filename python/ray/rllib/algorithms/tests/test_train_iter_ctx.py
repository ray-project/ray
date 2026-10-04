import sys
from collections import defaultdict
from types import SimpleNamespace

import pytest

from ray.rllib.algorithms.algorithm import TrainIterCtx
from ray.rllib.utils.metrics import (
    ALL_MODULES,
    ENV_RUNNER_RESULTS,
    LEARNER_RESULTS,
    NUM_AGENT_STEPS_SAMPLED,
    NUM_AGENT_STEPS_SAMPLED_LIFETIME,
    NUM_AGENT_STEPS_TRAINED,
    NUM_AGENT_STEPS_TRAINED_LIFETIME,
    NUM_ENV_STEPS_SAMPLED,
    NUM_ENV_STEPS_SAMPLED_LIFETIME,
    NUM_ENV_STEPS_TRAINED,
    NUM_ENV_STEPS_TRAINED_LIFETIME,
)


class _FakeMetrics:
    def __init__(self):
        self.values = {
            (ENV_RUNNER_RESULTS, NUM_ENV_STEPS_SAMPLED_LIFETIME): 0,
            (LEARNER_RESULTS, ALL_MODULES, NUM_ENV_STEPS_TRAINED_LIFETIME): 0,
            (ENV_RUNNER_RESULTS, NUM_AGENT_STEPS_SAMPLED_LIFETIME): {},
            (LEARNER_RESULTS, NUM_AGENT_STEPS_TRAINED_LIFETIME): {},
        }

    def peek(self, key, default=None):
        return self.values.get(key, default)


def _fake_algo(
    *,
    tolerance=2,
    sample_timeout_s=1.0,
    min_sample_timesteps=1,
    count_steps_by="env_steps",
):
    config = SimpleNamespace(
        enable_env_runner_and_connector_v2=False,
        count_steps_by=count_steps_by,
        min_time_s_per_iteration=None,
        min_sample_timesteps_per_iteration=min_sample_timesteps,
        min_train_timesteps_per_iteration=0,
        num_consecutive_env_runner_failures_tolerance=tolerance,
        sample_timeout_s=sample_timeout_s,
    )
    counters = defaultdict(int)
    for key in (
        NUM_AGENT_STEPS_SAMPLED,
        NUM_AGENT_STEPS_TRAINED,
        NUM_ENV_STEPS_SAMPLED,
        NUM_ENV_STEPS_TRAINED,
    ):
        counters[key] = 0
    return SimpleNamespace(config=config, _counters=counters)


def test_train_iter_ctx_fails_after_repeated_sampling_timeouts_without_progress():
    algo = _fake_algo(tolerance=2)

    with TrainIterCtx(algo) as ctx:
        assert ctx.should_stop(None) is False
        assert ctx.should_stop(True) is False
        assert ctx.should_stop(True) is False
        with pytest.raises(RuntimeError, match="No sampling progress"):
            ctx.should_stop(True)


def test_train_iter_ctx_treats_empty_old_stack_result_as_no_progress():
    """An empty old-stack training result is not a worker failure, but can stall."""
    algo = _fake_algo(tolerance=1)

    with TrainIterCtx(algo) as ctx:
        assert ctx.should_stop(None) is False
        # PPO's old-stack training step returns {} when sampling times out.
        assert ctx.should_stop({}) is False
        with pytest.raises(RuntimeError, match="No sampling progress"):
            ctx.should_stop({})


def test_train_iter_ctx_resets_no_progress_watchdog_after_sampling_progress():
    algo = _fake_algo(tolerance=1, min_sample_timesteps=3)

    with TrainIterCtx(algo) as ctx:
        assert ctx.should_stop(None) is False
        assert ctx.should_stop(True) is False

        algo._counters[NUM_ENV_STEPS_SAMPLED] = 1
        assert ctx.should_stop(True) is False
        assert ctx.sample_progress_failures == 0

        assert ctx.should_stop(True) is False
        with pytest.raises(RuntimeError, match="No sampling progress"):
            ctx.should_stop(True)


def test_train_iter_ctx_does_not_watchdog_blocking_sampling():
    algo = _fake_algo(tolerance=0, sample_timeout_s=None)

    with TrainIterCtx(algo) as ctx:
        assert ctx.should_stop(None) is False
        for _ in range(5):
            assert ctx.should_stop(True) is False
        assert ctx.sample_progress_failures == 0


def test_train_iter_ctx_new_api_fails_when_lifetime_sampling_counter_stalls():
    algo = _fake_algo(tolerance=1, min_sample_timesteps=3)
    algo.config.enable_env_runner_and_connector_v2 = True
    algo.metrics = _FakeMetrics()

    with TrainIterCtx(algo) as ctx:
        assert ctx.should_stop(False) is False
        assert ctx.should_stop(True) is False
        with pytest.raises(RuntimeError, match="No sampling progress"):
            ctx.should_stop(True)


def test_train_iter_ctx_new_api_resets_watchdog_on_lifetime_counter_progress():
    algo = _fake_algo(tolerance=1, min_sample_timesteps=3)
    algo.config.enable_env_runner_and_connector_v2 = True
    algo.metrics = _FakeMetrics()

    with TrainIterCtx(algo) as ctx:
        assert ctx.should_stop(False) is False
        assert ctx.should_stop(True) is False

        algo.metrics.values[(ENV_RUNNER_RESULTS, NUM_ENV_STEPS_SAMPLED_LIFETIME)] = 1
        assert ctx.should_stop(True) is False
        assert ctx.sample_progress_failures == 0

        assert ctx.should_stop(True) is False
        with pytest.raises(RuntimeError, match="No sampling progress"):
            ctx.should_stop(True)


def test_train_iter_ctx_legacy_agent_steps_watchdog_tracks_agent_progress():
    algo = _fake_algo(
        tolerance=1,
        min_sample_timesteps=3,
        count_steps_by="agent_steps",
    )

    with TrainIterCtx(algo) as ctx:
        assert ctx.should_stop(None) is False
        assert ctx.should_stop(True) is False

        algo._counters[NUM_AGENT_STEPS_SAMPLED] = 1
        assert ctx.should_stop(True) is False
        assert ctx.sample_progress_failures == 0

        assert ctx.should_stop(True) is False
        with pytest.raises(RuntimeError, match="No sampling progress"):
            ctx.should_stop(True)


def test_train_iter_ctx_new_api_agent_steps_watchdog_tracks_lifetime_progress():
    algo = _fake_algo(
        tolerance=1,
        min_sample_timesteps=3,
        count_steps_by="agent_steps",
    )
    algo.config.enable_env_runner_and_connector_v2 = True
    algo.metrics = _FakeMetrics()
    algo.metrics.values[(ENV_RUNNER_RESULTS, NUM_AGENT_STEPS_SAMPLED_LIFETIME)] = {
        "agent-0": 0
    }

    with TrainIterCtx(algo) as ctx:
        assert ctx.should_stop(False) is False
        assert ctx.should_stop(True) is False

        algo.metrics.values[(ENV_RUNNER_RESULTS, NUM_AGENT_STEPS_SAMPLED_LIFETIME)] = {
            "agent-0": 1
        }
        assert ctx.should_stop(True) is False
        assert ctx.sample_progress_failures == 0

        assert ctx.should_stop(True) is False
        with pytest.raises(RuntimeError, match="No sampling progress"):
            ctx.should_stop(True)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
