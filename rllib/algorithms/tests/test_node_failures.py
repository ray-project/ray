import pytest

import ray
import ray._common
from ray._private.test_utils import get_other_nodes
from ray.cluster_utils import Cluster
from ray.rllib.algorithms.appo import APPOConfig
from ray.rllib.algorithms.ppo import PPOConfig

EXPECTED_PER_NODE_OBJECT_STORE_MEMORY = 10**8
HEAD_REDIS_PORT = 6379
HEAD_CPUS = 2
WORKER_CPUS = 4
NUM_ENV_RUNNERS = 4


def _add_node(cluster, worker=True):
    cluster.add_node(
        redis_port=None if worker else HEAD_REDIS_PORT,
        num_cpus=WORKER_CPUS if worker else HEAD_CPUS,
        object_store_memory=EXPECTED_PER_NODE_OBJECT_STORE_MEMORY,
        include_dashboard=not worker,
    )


@pytest.fixture
def cluster():
    """Create a 2-node fake cluster: head (2 CPUs) + worker (4 CPUs).

    Head holds the algo process + local env runner.
    Worker holds all 4 remote env runners (deterministic placement).
    """
    assert (
        2 * EXPECTED_PER_NODE_OBJECT_STORE_MEMORY
        < ray._common.utils.get_system_memory() / 2
    ), "Not enough memory on this machine to run this workload."

    cluster = Cluster()
    _add_node(cluster, worker=False)
    _add_node(cluster)
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


def _train(cluster, algo, config, iters, preempt_freq):
    """Train loop with periodic node kill/restore and health tracking."""
    num_runners = config.num_env_runners
    saw_healthy_drop = False
    saw_recovery = False

    for i in range(iters):
        algo.train()

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
            _add_node(cluster)

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
        )
        .reporting(
            # Must be >= 2s so APPO's async mechanism has time to detect
            # worker death within a single iteration.
            min_time_s_per_iteration=2,
            min_train_timesteps_per_iteration=1,
        )
        .fault_tolerance(
            restart_failed_env_runners=True,
            env_runner_health_probe_timeout_s=20.0,
        )
    )

    algo = config.build()
    _train(cluster, algo, config, iters=10, preempt_freq=7)


def test_node_failure_recreate_ppo(cluster):
    """restart=True with PPO (sync): workers die, get auto-restarted by Ray,
    and restore_env_runners() syncs their state."""
    config = (
        PPOConfig()
        .environment("CartPole-v1")
        .learners(num_learners=0)
        .env_runners(
            num_env_runners=NUM_ENV_RUNNERS,
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
            env_runner_health_probe_timeout_s=20.0,
        )
    )

    algo = config.build()
    _train(cluster, algo, config, iters=10, preempt_freq=7)


def test_node_failure_no_recovery(cluster):
    """restart=False, ignore=False: dead worker RayErrors propagate and
    crash training. Verify the crash happens."""
    config = (
        PPOConfig()
        .environment("CartPole-v1")
        .env_runners(
            num_env_runners=NUM_ENV_RUNNERS,
            sample_timeout_s=5.0,
        )
        .training(
            train_batch_size=500,
            num_epochs=1,
            minibatch_size=500,
        )
        .reporting(min_train_timesteps_per_iteration=1)
        .fault_tolerance(
            restart_failed_env_runners=False,
            env_runner_health_probe_timeout_s=20.0,
        )
    )

    algo = config.build()
    # _train will crash with an ActorDiedError when dead workers are detected
    # (ignore=False, restart=False → errors propagate).
    with pytest.raises(ray.exceptions.ActorDiedError, match="actor died unexpectedly"):
        _train(cluster, algo, config, iters=10, preempt_freq=3)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
