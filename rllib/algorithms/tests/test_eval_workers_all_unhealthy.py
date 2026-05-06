"""Tests for evaluating when all configured remote eval EnvRunners are
unhealthy.

The behavior is controlled by two orthogonal config knobs:

- ``evaluation_unhealthy_workers_timeout_s``: how long to wait for at
  least one eval EnvRunner to recover before deciding what to do
  (default 0: don't wait).
- ``evaluation_error_after_n_consecutive_skips``: tolerate this many
  consecutive evaluation iterations in which all configured remote eval
  EnvRunners are unhealthy. On the next such iteration, ``evaluate()``
  raises ``RuntimeError``. ``None`` (default) tolerates an unbounded
  number of consecutive skips.

Both apply identically regardless of ``evaluation_parallel_to_training``.
"""
import time

import pytest

import ray
from ray.rllib.algorithms.ppo import PPOConfig


@pytest.fixture(params=[True, False], ids=["parallel", "sequential"])
def parallel_to_training(request):
    return request.param


def _algo_with_killed_eval_workers(
    *,
    timeout_s=0,
    error_after_n_consecutive_skips=None,
    parallel_to_training=True,
):
    """Build a PPO algo, kill every eval worker, mark them all unhealthy.

    Returns the live ``Algorithm`` ready for an ``algo.evaluate()`` call.

    Config is the smallest that exercises the failure path: no remote
    training EnvRunners, 1 (single) remote eval EnvRunner that we kill,
    fixed-duration eval so we can call `evaluate()` directly without a
    parallel-training future.
    """
    config = (
        PPOConfig()
        .environment("CartPole-v1")
        # Local-only training: skips remote train EnvRunner setup.
        .env_runners(num_env_runners=0)
        .evaluation(
            # 1 eval worker is enough; killing it leaves 0 healthy, which
            # is the condition we're testing.
            evaluation_num_env_runners=1,
            evaluation_interval=1,
            evaluation_parallel_to_training=parallel_to_training,
            # Fixed duration so `evaluate()` doesn't need a parallel-train
            # future (avoids the `auto` branch's `assert future is not None`).
            evaluation_duration=1,
            evaluation_duration_unit="episodes",
            evaluation_unhealthy_workers_timeout_s=timeout_s,
            evaluation_error_after_n_consecutive_skips=(
                error_after_n_consecutive_skips
            ),
        )
        .fault_tolerance(restart_failed_env_runners=False)
    )
    algo = config.build()

    # Kill the eval worker and mark it unhealthy. Mirrors losing every
    # eval node at once.
    eval_grp = algo.eval_env_runner_group
    for a in list(eval_grp._worker_manager._actors.values()):
        ray.kill(a, no_restart=True)
    for actor_id in list(eval_grp._worker_manager.actor_ids()):
        eval_grp._worker_manager.set_actor_state(actor_id, healthy=False)
    assert eval_grp.num_healthy_remote_workers() == 0
    return algo


def test_default_skips_eval_silently(parallel_to_training):
    """Default (threshold=None): evaluate() must return cleanly even if
    every eval iteration finds 0 healthy workers, indefinitely."""
    algo = _algo_with_killed_eval_workers(parallel_to_training=parallel_to_training)
    for _ in range(3):
        algo.evaluate()  # must not raise on any of these


def test_threshold_one_raises_on_first_skip(parallel_to_training):
    """threshold=1: first failed iteration raises (strictest setting)."""
    algo = _algo_with_killed_eval_workers(
        error_after_n_consecutive_skips=1,
        parallel_to_training=parallel_to_training,
    )
    with pytest.raises(
        RuntimeError, match="evaluation_error_after_n_consecutive_skips"
    ):
        algo.evaluate()


def test_threshold_n_tolerates_n_minus_1_skips(parallel_to_training):
    """threshold=N: tolerate N-1 consecutive skips, raise on the N-th."""
    threshold = 3
    algo = _algo_with_killed_eval_workers(
        error_after_n_consecutive_skips=threshold,
        parallel_to_training=parallel_to_training,
    )
    # First (threshold - 1) iterations skip silently.
    for _ in range(threshold - 1):
        algo.evaluate()
    # The threshold-th iteration trips the check and raises.
    with pytest.raises(
        RuntimeError, match="evaluation_error_after_n_consecutive_skips"
    ):
        algo.evaluate()


def test_timeout_waits_then_skips_when_no_recovery(parallel_to_training):
    """timeout_s>0 with workers that never come back: evaluate() should
    take at least roughly that long (waiting for recovery), then skip
    silently because the threshold defaults to None."""
    timeout_s = 2
    algo = _algo_with_killed_eval_workers(
        timeout_s=timeout_s, parallel_to_training=parallel_to_training
    )
    start = time.monotonic()
    algo.evaluate()
    elapsed = time.monotonic() - start
    assert elapsed >= timeout_s


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
