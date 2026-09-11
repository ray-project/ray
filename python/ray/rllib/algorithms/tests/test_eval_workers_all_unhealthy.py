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
from unittest.mock import patch

import pytest

import ray
from ray.rllib.algorithms.ppo import PPOConfig


@pytest.fixture(params=[True, False], ids=["parallel", "sequential"])
def parallel_to_training(request):
    return request.param


def _algo_with_unhealthy_eval_workers(
    *,
    timeout_s=0,
    error_after_n_consecutive_skips=None,
    parallel_to_training=True,
    spawn_replacement=False,
    enable_v2_stack=True,
):
    """Build a PPO algo and put every remote eval worker into the failed
    state.

    Always ``ray.kill(no_restart=True)``-s the original actors and sets
    ``restart_failed_env_runners=False`` so Ray Core does not auto-resurrect
    them. This keeps each test deterministic: the only way a worker reappears
    is for the test to explicitly add one back (see ``spawn_replacement``).

    Args:
        spawn_replacement: If True, additionally spawn a fresh remote eval
            EnvRunner via ``add_workers(1)`` after the kill, then flip its
            health flag to False as well. The replacement is what a
            subsequent ``probe_unhealthy_env_runners`` will be able to
            ping successfully -- simulating "a new worker has come back"
            without relying on Ray Core's restart timing.
        enable_v2_stack: If True (default), use the new API stack (v2
            connectors + Learner). If False, use the old API stack so
            the recovery path's ``_sync_filters_if_needed`` branch is
            exercised instead of ``sync_env_runner_states``.

    Config is the smallest that exercises the failure path: no remote
    training EnvRunners, 1 remote eval EnvRunner, fixed-duration eval so
    we can call ``evaluate()`` directly without a parallel-training
    future.
    """
    config = PPOConfig()
    if not enable_v2_stack:
        config = config.api_stack(
            enable_env_runner_and_connector_v2=False,
            enable_rl_module_and_learner=False,
        )
    config = (
        config.environment("CartPole-v1")
        # Local-only training: skips remote train EnvRunner setup.
        .env_runners(num_env_runners=0).evaluation(
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
        # Disable auto-restart so the tests are fully in control of when
        # (and whether) a worker comes back.
        .fault_tolerance(restart_failed_env_runners=False)
    )
    algo = config.build()

    eval_grp = algo.eval_env_runner_group
    # Hard-kill the originals; no Ray Core restart.
    for a in list(eval_grp._worker_manager._actors.values()):
        ray.kill(a, no_restart=True)

    if spawn_replacement:
        # The dead originals are evicted from the worker manager *before*
        # spawning the replacement so that `add_workers` assigns the
        # fresh worker the lowest free `worker_index` (1). Eval's
        # `_evaluate_with_fixed_duration` builds a per-worker num-units
        # list of length `evaluation_num_env_runners + 1` indexed by
        # `worker.worker_index`; leaving the dead originals in the
        # manager would push the new index past the end of that list.
        for actor_id in list(eval_grp._worker_manager.actor_ids()):
            eval_grp._worker_manager.remove_actor(actor_id)
        before = set(eval_grp._worker_manager.actor_ids())
        eval_grp.add_workers(1, validate=False)
        new_ids = set(eval_grp._worker_manager.actor_ids()) - before
        # `add_workers` registers the replacement with `is_healthy=True`;
        # flip it so the wait loop is forced to probe. The probe's
        # `ping()` then succeeds against this alive actor and flips it
        # back to healthy -- mirroring the production "Ray Core restarted
        # the actor" recovery sequence.
        for actor_id in new_ids:
            eval_grp._worker_manager.set_actor_state(actor_id, healthy=False)
    else:
        # No replacement: just mark the (dead) originals unhealthy.
        for actor_id in list(eval_grp._worker_manager.actor_ids()):
            eval_grp._worker_manager.set_actor_state(actor_id, healthy=False)

    assert eval_grp.num_healthy_remote_workers() == 0
    return algo


def test_default_skips_eval_silently(parallel_to_training):
    """Default (threshold=None): evaluate() must return cleanly even if
    every eval iteration finds 0 healthy workers, indefinitely."""
    algo = _algo_with_unhealthy_eval_workers(parallel_to_training=parallel_to_training)
    for _ in range(3):
        algo.evaluate()  # must not raise on any of these


def test_threshold_one_raises_on_first_skip(parallel_to_training):
    """threshold=1: first failed iteration raises (strictest setting)."""
    algo = _algo_with_unhealthy_eval_workers(
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
    algo = _algo_with_unhealthy_eval_workers(
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
    algo = _algo_with_unhealthy_eval_workers(
        timeout_s=timeout_s, parallel_to_training=parallel_to_training
    )
    start = time.monotonic()
    algo.evaluate()
    elapsed = time.monotonic() - start
    # Check against lower bound but also a loose upper bound to sanity check
    assert 5 > elapsed >= timeout_s


def test_timeout_recovers_resyncs_and_evaluates(parallel_to_training):
    """When a fresh replacement eval worker appears during the wait
    window, ``evaluate()`` must re-sync weights *and* connector state to
    it and then run eval normally (no skip, no raise, counter stays at 0).

    The corresponding syncs at the top of ``evaluate()`` run before the
    wait and skip workers that are unhealthy at that moment. Without the
    post-recovery re-syncs inside ``_maybe_wait_for_eval_env_runner_recovery``,
    the recovered worker would run eval with stale/initial weights and
    empty/stale connector state (e.g. no obs-normalization filter), each
    silently producing wrong metrics for one iteration.
    """
    timeout_s = 30  # generous; the first probe will revive quickly.
    algo = _algo_with_unhealthy_eval_workers(
        timeout_s=timeout_s,
        parallel_to_training=parallel_to_training,
        spawn_replacement=True,
    )
    eval_grp = algo.eval_env_runner_group

    # Spy on `sync_weights` AND `sync_env_runner_states` (v2 stack) so we
    # can verify both re-syncs after recovery. Each is expected to be
    # called >=2 times in one `evaluate()`:
    #   1) the unconditional sync at the start of `evaluate()` (effectively
    #      a no-op here because no eval worker is healthy at that moment),
    #   2) the post-recovery re-sync inside the wait method we target here.
    with patch.object(
        eval_grp, "sync_weights", wraps=eval_grp.sync_weights
    ) as weights_spy, patch.object(
        eval_grp,
        "sync_env_runner_states",
        wraps=eval_grp.sync_env_runner_states,
    ) as states_spy:
        start = time.monotonic()
        algo.evaluate()
        elapsed = time.monotonic() - start

    # Recovery should be near-instant
    assert elapsed < timeout_s
    assert algo._counters["num_consecutive_eval_no_workers_iterations"] == 0
    assert weights_spy.call_count == 2
    assert states_spy.call_count == 2


def test_timeout_recovers_resyncs_and_evaluates_old_stack(parallel_to_training):
    """Same as ``test_timeout_recovers_resyncs_and_evaluates`` but for the *old* API stack"""
    timeout_s = 30
    algo = _algo_with_unhealthy_eval_workers(
        timeout_s=timeout_s,
        parallel_to_training=parallel_to_training,
        spawn_replacement=True,
        enable_v2_stack=False,
    )
    eval_grp = algo.eval_env_runner_group

    # Spy on `sync_weights` (on the eval group) AND
    # `_sync_filters_if_needed` (on the algo itself; that's where the
    # method lives in the old API stack).
    with patch.object(
        eval_grp, "sync_weights", wraps=eval_grp.sync_weights
    ) as weights_spy, patch.object(
        algo,
        "_sync_filters_if_needed",
        wraps=algo._sync_filters_if_needed,
    ) as filters_spy:
        start = time.monotonic()
        algo.evaluate()
        elapsed = time.monotonic() - start

    assert elapsed < timeout_s
    assert algo._counters["num_consecutive_eval_no_workers_iterations"] == 0
    assert weights_spy.call_count == 2
    assert filters_spy.call_count == 2


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
