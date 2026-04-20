import pytest

import ray
import ray._common
from ray._private.test_utils import get_other_nodes
from ray.cluster_utils import Cluster
from ray.rllib.algorithms.appo import APPOConfig
from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.core import DEFAULT_MODULE_ID
from ray.rllib.utils.metrics import (
    LEARNER_RESULTS,
    MODULE_TRAIN_BATCH_SIZE_MEAN,
)
from ray.rllib.utils.test_utils import check

OBJECT_STORE_MEMORY = 10**8
HEAD_CPUS = 2
WORKER_CPUS = 4
NUM_ENV_RUNNERS = 4


@pytest.fixture
def cluster():
    """Create a 2-node fake cluster: head (2 CPUs) + worker (4 CPUs).

    Head holds the algo process + local env runner.
    Worker holds all 4 remote env runners (deterministic placement).
    """
    assert (
        2 * OBJECT_STORE_MEMORY < ray._common.utils.get_system_memory() / 2
    ), "Not enough memory on this machine to run this workload."

    cluster = Cluster()
    cluster.add_node(
        redis_port=6379,
        num_cpus=HEAD_CPUS,
        num_gpus=0,
        object_store_memory=OBJECT_STORE_MEMORY,
        include_dashboard=False,
    )
    cluster.add_node(
        redis_port=None,
        num_cpus=WORKER_CPUS,
        num_gpus=0,
        object_store_memory=OBJECT_STORE_MEMORY,
    )
    cluster.wait_for_nodes()
    ray.init(address=cluster.address)

    yield cluster

    ray.shutdown()
    cluster.shutdown()


def _kill_worker_node(cluster):
    others = get_other_nodes(cluster, exclude_head=True)
    if others:
        cluster.remove_node(others[0])
        return True
    return False


def _add_worker_node(cluster):
    cluster.add_node(
        redis_port=None,
        num_cpus=WORKER_CPUS,
        num_gpus=0,
        object_store_memory=OBJECT_STORE_MEMORY,
    )


def _train(cluster, algo, config, iters, preempt_freq):
    """Train loop with periodic node kill/restore and health tracking."""
    num_runners = config.num_env_runners
    saw_healthy_drop = False
    saw_recovery = False

    for i in range(iters):
        results = algo.train()

        avg_batch = results[LEARNER_RESULTS][DEFAULT_MODULE_ID][
            MODULE_TRAIN_BATCH_SIZE_MEAN
        ]
        if config.algo_class.__name__ == "PPO":
            exp_batch_size = config.minibatch_size
        else:
            exp_batch_size = config.total_train_batch_size
        check(avg_batch, exp_batch_size, rtol=0.1)
        assert avg_batch < exp_batch_size + config.get_rollout_fragment_length()

        assert algo.env_runner_group.num_remote_env_runners() == num_runners
        healthy = algo.env_runner_group.num_healthy_remote_workers()
        assert 0 <= healthy <= num_runners

        if healthy < num_runners:
            saw_healthy_drop = True
        if saw_healthy_drop and healthy == num_runners:
            saw_recovery = True

        print(
            f"ITER={i}, healthy={healthy}/{num_runners}, "
            f"saw_drop={saw_healthy_drop}, saw_recovery={saw_recovery}"
        )

        # Shut down one node every preempt_freq iterations.
        if i % preempt_freq == 0:
            _kill_worker_node(cluster)

        # Bring back a previously failed node.
        elif (i - 1) % preempt_freq == 0:
            _add_worker_node(cluster)

    # Workers must have gone down at some point.
    assert saw_healthy_drop, (
        "Expected healthy worker count to drop after node kill, " "but it never did."
    )
    # If restart is enabled, workers must have come back.
    if config.restart_failed_env_runners:
        assert saw_recovery, (
            "Expected workers to recover after node restore "
            "(restart_failed_env_runners=True), but they never did."
        )
    # If restart is disabled, workers must NOT have come back.
    if not config.restart_failed_env_runners:
        assert (
            not saw_recovery
        ), "Workers recovered despite restart_failed_env_runners=False."


def test_node_failure_ignore(cluster):
    """restart=False, ignore=True: workers die and stay dead, training
    continues with fewer workers."""
    config = (
        PPOConfig()
        .environment("CartPole-v1")
        .env_runners(
            num_env_runners=NUM_ENV_RUNNERS,
            validate_env_runners_after_construction=True,
            sample_timeout_s=5.0,
        )
        .training(
            train_batch_size=500,
            num_epochs=1,
            minibatch_size=500,
        )
        .reporting(min_train_timesteps_per_iteration=1)
        .fault_tolerance(
            ignore_env_runner_failures=True,
            restart_failed_env_runners=False,
            env_runner_health_probe_timeout_s=20.0,
        )
    )

    algo = config.build()
    _train(cluster, algo, config, iters=10, preempt_freq=3)
    algo.stop()


def test_node_failure_recreate_appo(cluster):
    """restart=True with APPO (async): workers die, get auto-restarted by Ray,
    and restore_env_runners() syncs their state."""
    config = (
        APPOConfig()
        .environment("CartPole-v1")
        .learners(num_learners=0)
        .experimental(_validate_config=False)
        .env_runners(
            num_env_runners=NUM_ENV_RUNNERS,
            validate_env_runners_after_construction=True,
        )
        .reporting(
            # Must be >= 2s so APPO's async mechanism has time to detect
            # worker death within a single iteration.
            min_time_s_per_iteration=2,
            min_train_timesteps_per_iteration=1,
        )
        .fault_tolerance(
            restart_failed_env_runners=True,
            ignore_env_runner_failures=False,
            env_runner_health_probe_timeout_s=20.0,
        )
    )

    algo = config.build()
    _train(cluster, algo, config, iters=10, preempt_freq=3)
    algo.stop()


def test_node_failure_recreate_ppo(cluster):
    """restart=True with PPO (sync): workers die, get auto-restarted by Ray,
    and restore_env_runners() syncs their state."""
    config = (
        PPOConfig()
        .environment("CartPole-v1")
        .learners(num_learners=0)
        .env_runners(
            num_env_runners=NUM_ENV_RUNNERS,
            validate_env_runners_after_construction=True,
            sample_timeout_s=5.0,
        )
        .training(
            train_batch_size=500,
            num_epochs=1,
            minibatch_size=500,
        )
        .reporting(
            min_time_s_per_iteration=2,
            min_train_timesteps_per_iteration=1,
        )
        .fault_tolerance(
            restart_failed_env_runners=True,
            ignore_env_runner_failures=False,
            env_runner_health_probe_timeout_s=20.0,
        )
    )

    algo = config.build()
    _train(cluster, algo, config, iters=10, preempt_freq=3)
    algo.stop()


def test_node_failure_no_recovery(cluster):
    """restart=False, ignore=False: dead worker RayErrors propagate and
    crash training. Verify the crash happens."""
    config = (
        PPOConfig()
        .environment("CartPole-v1")
        .env_runners(
            num_env_runners=NUM_ENV_RUNNERS,
            validate_env_runners_after_construction=True,
            sample_timeout_s=5.0,
        )
        .training(
            train_batch_size=500,
            num_epochs=1,
            minibatch_size=500,
        )
        .reporting(min_train_timesteps_per_iteration=1)
        .fault_tolerance(
            ignore_env_runner_failures=False,
            restart_failed_env_runners=False,
            env_runner_health_probe_timeout_s=20.0,
        )
    )

    algo = config.build()
    # _train will crash with a RayError when dead workers are detected
    # (ignore=False, restart=False → errors propagate).
    with pytest.raises(Exception):
        _train(cluster, algo, config, iters=10, preempt_freq=3)
    algo.stop()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
