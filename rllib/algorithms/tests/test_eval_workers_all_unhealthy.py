"""Tests for evaluating when *all* eval EnvRunners are unhealthy and
``evaluation_parallel_to_training=True``.

Previously, ``Algorithm.evaluate()`` fell back to
``_evaluate_on_local_env_runner(self.eval_env_runner)`` in this case, which
raises ``ValueError: Cannot run on local evaluation worker parallel to
training!``. Now controlled by two orthogonal config knobs:

- ``evaluation_unhealthy_workers_timeout_s``: how long to wait for at least
  one eval EnvRunner to recover before deciding what to do (default 0:
  don't wait).
- ``evaluation_error_on_no_workers``: if still no healthy eval EnvRunners
  after the wait, raise ``RuntimeError`` (True) or skip evaluation for
  this iteration (False, default).
"""
import time

import pytest

import ray
from ray.rllib.algorithms.ppo import PPOConfig


def _algo_with_killed_eval_workers(
    *,
    timeout_s=0,
    error_on_no_workers=False,
):
    """Build a PPO algo, kill every eval worker, mark them all unhealthy.

    Returns the live ``Algorithm`` ready for an ``algo.train()`` call.
    """
    config = (
        PPOConfig()
        .environment("CartPole-v1")
        .env_runners(num_env_runners=2)
        .training(
            train_batch_size=200,
            num_epochs=1,
            minibatch_size=200,
        )
        .evaluation(
            evaluation_num_env_runners=2,
            evaluation_interval=1,
            evaluation_parallel_to_training=True,
            evaluation_duration="auto",
            evaluation_duration_unit="timesteps",
            evaluation_unhealthy_workers_timeout_s=timeout_s,
            evaluation_error_on_no_workers=error_on_no_workers,
        )
        .fault_tolerance(restart_failed_env_runners=False)
        .reporting(min_train_timesteps_per_iteration=1)
    )
    algo = config.build()

    # Kill every eval worker and mark them all unhealthy. Mirrors
    # losing every eval node at once.
    eval_grp = algo.eval_env_runner_group
    for a in list(eval_grp._worker_manager._actors.values()):
        ray.kill(a, no_restart=True)
    for actor_id in list(eval_grp._worker_manager.actor_ids()):
        eval_grp._worker_manager.set_actor_state(actor_id, healthy=False)
    assert eval_grp.num_healthy_remote_workers() == 0
    return algo


@pytest.fixture(scope="module", autouse=True)
def _ray_cluster():
    # `num_cpus=8` bounds the cluster; each test needs 4 train + 2 eval
    # EnvRunner actors plus the driver. Module scope avoids paying ~4s of
    # ray.init/shutdown per test (the four tests are independent — actors
    # are torn down by algo.stop() / ray.kill within each test).
    ray.init(num_cpus=8, include_dashboard=False)
    yield
    ray.shutdown()


def test_default_skips_eval_silently():
    """Defaults (timeout=0, error=False): train() must succeed even though
    every eval worker is dead. Eval is just skipped."""
    algo = _algo_with_killed_eval_workers()
    try:
        result = algo.train()  # must not raise
        assert result is not None
    finally:
        algo.stop()


def test_error_on_no_workers_raises():
    """error_on_no_workers=True with no recovery: train() must raise a clear,
    actionable error rather than silently skipping."""
    algo = _algo_with_killed_eval_workers(error_on_no_workers=True)
    try:
        with pytest.raises(RuntimeError, match="evaluation_error_on_no_workers"):
            algo.train()
    finally:
        algo.stop()


def test_timeout_waits_then_skips_when_no_recovery():
    """timeout_s>0 with workers that never come back: train() should take at
    least roughly that long (waiting for recovery), then skip silently
    because error_on_no_workers defaults to False."""
    timeout_s = 2.0
    algo = _algo_with_killed_eval_workers(timeout_s=timeout_s)
    try:
        start = time.monotonic()
        result = algo.train()  # must not raise
        elapsed = time.monotonic() - start
        # At least ~timeout_s elapsed because we polled for recovery.
        assert elapsed >= timeout_s * 0.8, (
            f"train() returned in {elapsed:.2f}s; expected at least "
            f"{timeout_s * 0.8:.2f}s for the recovery wait."
        )
        assert result is not None
    finally:
        algo.stop()


def test_timeout_then_error_when_no_recovery():
    """timeout_s>0 + error_on_no_workers=True with no recovery: wait the
    timeout, then raise."""
    timeout_s = 1.0
    algo = _algo_with_killed_eval_workers(timeout_s=timeout_s, error_on_no_workers=True)
    try:
        start = time.monotonic()
        with pytest.raises(RuntimeError, match="evaluation_error_on_no_workers"):
            algo.train()
        elapsed = time.monotonic() - start
        assert elapsed >= timeout_s * 0.8, (
            f"train() raised in {elapsed:.2f}s; expected at least "
            f"{timeout_s * 0.8:.2f}s for the recovery wait."
        )
    finally:
        algo.stop()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
