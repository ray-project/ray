import unittest

import ray
from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.examples.envs.classes.cartpole_crashing import CartPoleCrashing
from ray.rllib.examples.envs.classes.multi_agent import make_multi_agent
from ray.rllib.utils.error import EnvError
from ray.tune.registry import register_env


class TestEnvsThatCrash(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        ray.init(num_cpus=4)

        register_env(
            "ma_cartpole_crashing",
            lambda cfg: (
                cfg.update({"num_agents": 2}),
                make_multi_agent(CartPoleCrashing)(cfg),
            )[1],
        )

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def test_env_crash_during_pre_checking(self):
        """Expect the env pre-checking to fail on each worker."""
        config = (
            PPOConfig()
            .rollouts(num_rollout_workers=2, num_envs_per_worker=4)
            .environment(
                env=CartPoleCrashing,
                env_config={
                    # Crash prob=100% (during pre-checking's `step()` test calls).
                    "p_crash": 1.0,
                    "init_time_s": 0.5,
                },
            )
        )

        # Expect ValueError due to pre-checking failing (our pre-checker module
        # raises a ValueError if `step()` fails).
        self.assertRaisesRegex(
            ValueError,
            "Simulated env crash",
            lambda: config.build(),
        )

    def test_env_crash_during_sampling(self):
        """Expect some sub-envs to fail (and not recover)."""
        config = (
            PPOConfig()
            .rollouts(num_rollout_workers=2, num_envs_per_worker=3)
            .environment(
                env=CartPoleCrashing,
                env_config={
                    # Crash prob=20%.
                    "p_crash": 0.2,
                    "init_time_s": 0.3,
                },
                disable_env_checking=True,
            )
        )

        for multi_agent in [True, False]:
            if multi_agent:
                config.environment("ma_cartpole_crashing")
            else:
                config.environment(CartPoleCrashing)
            # Pre-checking disables, so building the Algorithm is save.
            algo = config.build()
            # Expect EnvError due to the sub-env(s) crashing on the different workers
            # and `ignore_worker_failures=False` (so the original EnvError should
            # just be bubbled up by RLlib Algorithm and tune.Trainable during the
            # `step()` call).
            self.assertRaisesRegex(
                EnvError, "Simulated env crash", lambda algo=algo: algo.train()
            )
            algo.stop()

    def test_env_crash_on_one_worker_during_sampling_but_ignore(self):
        """Expect some sub-envs on one worker to fail (and not recover), but ignore."""
        config = (
            PPOConfig()
            .experimental(_enable_new_api_stack=True)
            .rollouts(
                num_rollout_workers=2,
                num_envs_per_worker=3,
                # Ignore worker failures (continue with worker #2).
                ignore_worker_failures=True,
            )
            .environment(
                env=CartPoleCrashing,
                env_config={
                    # Crash prob=80%.
                    "p_crash": 0.8,
                    # Only crash on worker with index 1.
                    "crash_on_worker_indices": [1],
                },
                disable_env_checking=True,
            )
        )

        for multi_agent in [True, False]:
            if multi_agent:
                config.environment("ma_cartpole_crashing")
            else:
                config.environment(CartPoleCrashing)
            # Pre-checking disables, so building the Algorithm is save.
            algo = config.build()
            # Expect some errors being logged here, but in general, should continue
            # as we ignore worker failures.
            algo.train()
            # One worker has been removed -> Only one left.
            self.assertEqual(algo.workers.num_healthy_remote_workers(), 1)
            algo.stop()

    def test_env_crash_on_one_worker_during_sampling_but_recreate_worker(self):
        """Expect some sub-envs to fail (and not recover), but re-create worker."""
        config = (
            PPOConfig()
            .experimental(_enable_new_api_stack=True)
            .rollouts(
                # env_runner_cls=ForwardHealthCheckToEnvWorker,
                num_rollout_workers=2,
                rollout_fragment_length=10,
                num_envs_per_worker=3,
                # Re-create failed workers (then continue).
                recreate_failed_workers=True,
            )
            .training(train_batch_size=60, sgd_minibatch_size=60)
            .environment(
                env=CartPoleCrashing,
                env_config={
                    "crash_after_n_steps": 10,
                    # Crash prob=100%, so test is deterministic.
                    "p_crash": 1.0,
                    # Only crash on worker with index 2.
                    "crash_on_worker_indices": [2],
                },
                # Make sure nothing happens during pre-checks.
                disable_env_checking=True,
            )
            .fault_tolerance(delay_between_worker_restarts_s=0)
        )
        for multi_agent in [True, False]:
            if multi_agent:
                config.environment("ma_cartpole_crashing")
            else:
                config.environment(CartPoleCrashing)

            # Pre-checking disables, so building the Algorithm is save.
            algo = config.build()
            # Try to re-create for infinite amount of times.
            # The worker recreation/ignore tolerance used to be hard-coded to 3, but
            # this has now been
            for i in range(5):
                # Expect some errors being logged here, but in general, should continue
                # as we recover from all worker failures.
                print(f"iter {i}: ", algo.train())
                # One worker has been removed.
                self.assertEqual(algo.workers.num_healthy_remote_workers(), 1)
            algo.stop()

    def test_env_crash_during_sampling_but_restart_only_crashed_sub_env(self):
        """Expect sub-envs to fail (and not recover), but re-start them individually."""
        config = (
            PPOConfig()
            .rollouts(
                num_rollout_workers=2,
                num_envs_per_worker=3,
                # Re-start failed individual sub-envs (then continue).
                # This means no workers will ever fail due to individual env errors
                # (only maybe for reasons other than the env).
                restart_failed_sub_environments=True,
                # If the worker was affected by an error (other than the env error),
                # allow it to be removed, but training will continue.
                ignore_worker_failures=True,
            )
            .environment(
                env=CartPoleCrashing,
                env_config={
                    # Crash prob=1%.
                    "p_crash": 0.01,
                },
                # Make sure nothing happens during pre-checks.
                disable_env_checking=True,
            )
        )
        for multi_agent in [True]:  # TODO, False]:
            if multi_agent:
                config.environment("ma_cartpole_crashing")
            else:
                config.environment(CartPoleCrashing)

            # Pre-checking disables, so building the Algorithm is save.
            algo = config.build()
            # Try to re-create the sub-env for infinite amount of times.
            for _ in range(5):
                # Expect some errors being logged here, but in general, should continue
                # as we recover from all sub-env failures.
                algo.train()
                # No worker has been removed. Still 2 left.
                self.assertEqual(algo.workers.num_healthy_remote_workers(), 2)
            algo.stop()


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
