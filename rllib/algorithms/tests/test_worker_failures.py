from collections import defaultdict
import gymnasium as gym
import numpy as np
import time
import unittest

import ray
from ray.util.state import list_actors
from ray.rllib.algorithms.algorithm_config import AlgorithmConfig
from ray.rllib.algorithms.callbacks import DefaultCallbacks
from ray.rllib.algorithms.sac.sac import SACConfig
from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.connectors.env_to_module.flatten_observations import FlattenObservations
from ray.rllib.env.multi_agent_env import make_multi_agent
from ray.rllib.env.multi_agent_env_runner import MultiAgentEnvRunner
from ray.rllib.env.single_agent_env_runner import SingleAgentEnvRunner
from ray.rllib.examples.envs.classes.random_env import RandomEnv
from ray.rllib.utils.metrics import (
    ENV_RUNNER_RESULTS,
    EPISODE_RETURN_MEAN,
    EVALUATION_RESULTS,
)
from ray.tune.registry import register_env


@ray.remote
class Counter:
    """Remote counter service that survives restarts."""

    def __init__(self):
        self.reset()

    def _key(self, eval, worker_index, vector_index):
        return f"{eval}:{worker_index}:{vector_index}"

    def increment(self, eval, worker_index, vector_index):
        self.counter[self._key(eval, worker_index, vector_index)] += 1

    def get(self, eval, worker_index, vector_index):
        return self.counter[self._key(eval, worker_index, vector_index)]

    def reset(self):
        self.counter = defaultdict(int)


class FaultInjectEnv(gym.Env):
    """Env that fails upon calling `step()`, but only for some remote worker indices.

    The worker indices that should produce the failure (a ValueError) can be
    provided by a list (of ints) under the "bad_indices" key in the env's
    config.

    .. testcode::
        :skipif: True

        from ray.rllib.env.env_context import EnvContext
        # This env will fail for workers 1 and 2 (not for the local worker
        # or any others with an index != [1|2]).
        bad_env = FaultInjectEnv(
            EnvContext(
                {"bad_indices": [1, 2]},
                worker_index=1,
                num_workers=3,
             )
        )

        from ray.rllib.env.env_context import EnvContext
        # This env will fail only on the first evaluation worker, not on the first
        # regular rollout worker.
        bad_env = FaultInjectEnv(
            EnvContext(
                {"bad_indices": [1], "eval_only": True},
                worker_index=2,
                num_workers=5,
            )
        )
    """

    def __init__(self, config):
        # Use RandomEnv to control episode length if needed.
        self.env = RandomEnv(config)
        self.action_space = self.env.action_space
        self.observation_space = self.env.observation_space
        self.config = config
        # External counter service.
        if "counter" in config:
            self.counter = ray.get_actor(config["counter"])
        else:
            self.counter = None

        if (
            config.get("init_delay", 0) > 0.0
            and (
                not config.get("init_delay_indices", [])
                or self.config.worker_index in config.get("init_delay_indices", [])
            )
            and
            # constructor delay can only happen for recreated actors.
            self._get_count() > 0
        ):
            # Simulate an initialization delay.
            time.sleep(config.get("init_delay"))

    def _increment_count(self):
        if self.counter:
            eval = self.config.get("evaluation", False)
            worker_index = self.config.worker_index
            vector_index = self.config.vector_index
            ray.wait([self.counter.increment.remote(eval, worker_index, vector_index)])

    def _get_count(self):
        if self.counter:
            eval = self.config.get("evaluation", False)
            worker_index = self.config.worker_index
            vector_index = self.config.vector_index
            return ray.get(self.counter.get.remote(eval, worker_index, vector_index))
        return -1

    def _maybe_raise_error(self):
        # Do not raise simulated error if this worker is not bad.
        if self.config.worker_index not in self.config.get("bad_indices", []):
            return

        if self.counter:
            count = self._get_count()
            if self.config.get(
                "failure_start_count", -1
            ) >= 0 and count < self.config.get("failure_start_count"):
                return

            if self.config.get(
                "failure_stop_count", -1
            ) >= 0 and count >= self.config.get("failure_stop_count"):
                return

        raise ValueError(
            "This is a simulated error from "
            f"{'eval-' if self.config.get('evaluation', False) else ''}"
            f"worker-idx={self.config.worker_index}!"
        )

    def reset(self, *, seed=None, options=None):
        self._increment_count()
        self._maybe_raise_error()
        return self.env.reset()

    def step(self, action):
        self._increment_count()
        self._maybe_raise_error()

        if self.config.get("step_delay", 0) > 0.0 and (
            not self.config.get("init_delay_indices", [])
            or self.config.worker_index in self.config.get("step_delay_indices", [])
        ):
            # Simulate a step delay.
            time.sleep(self.config.get("step_delay"))

        return self.env.step(action)

    def action_space_sample(self):
        return self.env.action_space.sample()


class ForwardHealthCheckToEnvWorker(SingleAgentEnvRunner):
    """Configuring EnvRunner to error in specific condition is hard.

    So we take a short-cut, and simply forward ping() to env.sample().
    """

    def ping(self) -> str:
        # See if Env wants to throw error.
        self.env.reset()
        actions = self.env.action_space.sample()
        _ = self.env.step(actions)
        # If there is no error raised from sample(), we simply reply pong.
        return super().ping()


class ForwardHealthCheckToEnvWorkerMultiAgent(MultiAgentEnvRunner):
    """Configure RolloutWorker to error in specific condition is hard.

    So we take a short-cut, and simply forward ping() to env.sample().
    """

    def ping(self) -> str:
        # See if Env wants to throw error.
        self.sample(num_timesteps=1, random_actions=True)
        # If there is no error raised from sample(), we simply reply pong.
        return super().ping()


def wait_for_restore(num_restarting_allowed=0):
    """Wait for Ray actor fault tolerence to restore all failed workers.

    Args:
        num_restarting_allowed: Number of actors that are allowed to be
            in "RESTARTING" state. This is because some actors may
            hang in __init__().
    """
    time.sleep(15.0)
    return
    while True:
        states = [
            a["state"]
            for a in list_actors(
                filters=[("class_name", "=", "ForwardHealthCheckToEnvWorker")]
            )
        ]
        finished = True
        for s in states:
            # Wait till all actors are either "ALIVE" (restored),
            # or "DEAD" (cancelled. these actors are from other
            # finished test cases) or "RESTARTING" (being restored).
            if s not in ["ALIVE", "DEAD", "RESTARTING"]:
                finished = False
                break

        restarting = [s for s in states if s == "RESTARTING"]
        if len(restarting) > num_restarting_allowed:
            finished = False

        print("waiting ... ", states)
        if finished:
            break
        # Otherwise, wait a bit.
        time.sleep(0.5)


class AddModuleCallback(DefaultCallbacks):
    def __init__(self):
        super().__init__()

    def on_algorithm_init(self, *, algorithm, metrics_logger, **kwargs):
        # Add a custom module to algorithm.
        spec = algorithm.config.get_default_rl_module_spec()
        spec.observation_space = gym.spaces.Box(low=0, high=1, shape=(8,))
        spec.action_space = gym.spaces.Discrete(2)
        spec.inference_only = True
        algorithm.add_module(
            module_id="test_module",
            module_spec=spec,
            add_to_eval_env_runners=True,
        )


class TestWorkerFailures(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        ray.init()

        obs_space = gym.spaces.Box(0, 1, (2,), np.float32)

        def _sa(ctx):
            ctx.update({"observation_space": obs_space})
            return FaultInjectEnv(ctx)

        register_env("fault_env", _sa)

        def _ma(ctx):
            ctx.update({"observation_space": obs_space})
            return make_multi_agent(FaultInjectEnv)(ctx)

        register_env("multi_agent_fault_env", _ma)

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def _do_test_failing_fatal(self, config, fail_eval=False):
        """Test raises real error when out of workers."""
        config.num_env_runners = 2
        config.env = "multi_agent_fault_env" if config.is_multi_agent() else "fault_env"
        # Make both worker idx=1 and 2 fail.
        config.env_config = {"bad_indices": [1, 2]}
        if fail_eval:
            config.evaluation_num_env_runners = 2
            config.evaluation_interval = 1
            config.evaluation_config = {
                # Make eval worker (index 1) fail.
                "env_config": {
                    "bad_indices": [1],
                    "evaluation": True,
                },
            }

        algo = config.build()
        self.assertRaises(Exception, lambda: algo.train())
        algo.stop()

    def _do_test_failing_ignore(self, config: AlgorithmConfig, fail_eval: bool = False):
        # Test fault handling
        config.num_env_runners = 2
        config.ignore_env_runner_failures = True
        config.recreate_failed_env_runners = False
        config.env = "fault_env"
        # Make worker idx=1 fail. Other workers will be ok.
        config.environment(
            env_config={
                "bad_indices": [1],
            }
        )
        if fail_eval:
            config.evaluation_num_env_runners = 2
            config.evaluation_interval = 1
            config.evaluation_config = {
                "ignore_env_runner_failures": True,
                "recreate_failed_env_runners": False,
                "env_config": {
                    # Make worker idx=1 fail. Other workers will be ok.
                    "bad_indices": [1],
                    "evaluation": True,
                },
            }
        algo = config.build()
        algo.train()

        # One of the rollout workers failed.
        self.assertEqual(algo.env_runner_group.num_healthy_remote_workers(), 1)
        if fail_eval:
            # One of the eval workers failed.
            self.assertEqual(algo.eval_env_runner_group.num_healthy_remote_workers(), 1)

        algo.stop()

    def _do_test_failing_recover(self, config, multi_agent=False):
        # Counter that will survive restarts.
        COUNTER_NAME = f"_do_test_failing_recover{'_ma' if multi_agent else ''}"
        counter = Counter.options(name=COUNTER_NAME).remote()

        # Test raises real error when out of workers.
        config.num_env_runners = 1
        config.evaluation_num_env_runners = 1
        config.evaluation_interval = 1
        config.env = "fault_env" if not multi_agent else "multi_agent_fault_env"
        config.evaluation_config = AlgorithmConfig.overrides(
            recreate_failed_env_runners=True,
            # 0 delay for testing purposes.
            delay_between_env_runner_restarts_s=0,
            # Make eval worker (index 1) fail.
            env_config={
                "bad_indices": [1],
                "failure_start_count": 3,
                "failure_stop_count": 4,
                "counter": COUNTER_NAME,
            },
            **(
                dict(
                    policy_mapping_fn=(
                        lambda aid, episode, **kwargs: (
                            # Allows this test to query this
                            # different-from-training-workers policy mapping fn.
                            "This is the eval mapping fn"
                            if episode is None
                            else "main"
                            if hash(episode.id_) % 2 == aid
                            else "p{}".format(np.random.choice([0, 1]))
                        )
                    )
                )
                if multi_agent
                else {}
            ),
        )

        # Reset interaction counter.
        ray.wait([counter.reset.remote()])

        algo = config.build()

        # This should also work several times.
        for _ in range(2):
            algo.train()
            wait_for_restore()
            algo.restore_workers(algo.env_runner_group)
            algo.restore_workers(algo.eval_env_runner_group)

            self.assertEqual(algo.env_runner_group.num_healthy_remote_workers(), 1)
            self.assertEqual(algo.eval_env_runner_group.num_healthy_remote_workers(), 1)
            if multi_agent:
                # Make a dummy call to the eval worker's policy_mapping_fn and
                # make sure the restored eval worker received the correct one from
                # the eval config (not the main workers' one).
                test = algo.eval_env_runner_group.foreach_worker(
                    lambda w: w.config.policy_mapping_fn(0, None)
                )
                self.assertEqual(test[0], "This is the eval mapping fn")
        algo.stop()

    def test_fatal_single_agent(self):
        # Test the case where all workers fail (w/o recovery).
        self._do_test_failing_fatal(
            PPOConfig()
            .api_stack(
                enable_rl_module_and_learner=True,
                enable_env_runner_and_connector_v2=True,
            )
            .env_runners(
                env_to_module_connector=lambda env: FlattenObservations(),
            )
        )

    def test_fatal_multi_agent(self):
        # Test the case where all workers fail (w/o recovery).
        self._do_test_failing_fatal(
            PPOConfig()
            .api_stack(
                enable_rl_module_and_learner=True,
                enable_env_runner_and_connector_v2=True,
            )
            .multi_agent(policies={"p0"}, policy_mapping_fn=lambda *a, **k: "p0"),
        )

    # TODO (sven): Reinstate once IMPALA/APPO support EnvRunners.
    # def test_async_samples(self):
    #    self._do_test_fault_ignore(
    #        IMPALAConfig()
    #        .api_stack(
    #            enable_rl_module_and_learner=True,
    #            enable_env_runners_and_connector_v2=True,
    #        )
    #        .env_runners(env_runner_cls=ForwardHealthCheckToEnvWorker)
    #        .resources(num_gpus=0)
    #    )

    def test_sync_replay(self):
        self._do_test_failing_ignore(
            SACConfig()
            .api_stack(
                enable_rl_module_and_learner=True,
                enable_env_runner_and_connector_v2=True,
            )
            .environment(
                env_config={"action_space": gym.spaces.Box(0, 1, (2,), np.float32)}
            )
            .env_runners(env_runner_cls=ForwardHealthCheckToEnvWorker)
            .reporting(min_sample_timesteps_per_iteration=1)
            .training(
                replay_buffer_config={"type": "EpisodeReplayBuffer"},
                # We need to set the base `lr` to `None` b/c SAC in the new stack
                # has its own learning rates.
                lr=None,
            )
        )

    def test_multi_gpu(self):
        self._do_test_failing_ignore(
            PPOConfig()
            .api_stack(
                enable_rl_module_and_learner=True,
                enable_env_runner_and_connector_v2=True,
            )
            .env_runners(env_runner_cls=ForwardHealthCheckToEnvWorker)
            .training(
                train_batch_size=10,
                minibatch_size=1,
                num_epochs=1,
            )
        )

    def test_sync_samples(self):
        self._do_test_failing_ignore(
            PPOConfig()
            .api_stack(
                enable_rl_module_and_learner=True,
                enable_env_runner_and_connector_v2=True,
            )
            .env_runners(env_runner_cls=ForwardHealthCheckToEnvWorker)
            .training(optimizer={})
        )

    def test_eval_workers_failing_ignore(self):
        # Test the case where one eval worker fails, but we chose to ignore.
        self._do_test_failing_ignore(
            PPOConfig()
            .api_stack(
                enable_rl_module_and_learner=True,
                enable_env_runner_and_connector_v2=True,
            )
            .env_runners(env_runner_cls=ForwardHealthCheckToEnvWorker)
            .training(model={"fcnet_hiddens": [4]}),
            fail_eval=True,
        )

    def test_eval_workers_parallel_to_training_failing_recover(self):
        # Test the case where all eval workers fail, but we chose to recover.
        config = (
            PPOConfig()
            .api_stack(
                enable_rl_module_and_learner=True,
                enable_env_runner_and_connector_v2=True,
            )
            .env_runners(env_runner_cls=ForwardHealthCheckToEnvWorker)
            .evaluation(
                evaluation_num_env_runners=1,
                evaluation_parallel_to_training=True,
                evaluation_duration="auto",
            )
            .training(model={"fcnet_hiddens": [4]})
        )

        self._do_test_failing_recover(config)

    def test_eval_workers_parallel_to_training_multi_agent_failing_recover(
        self,
    ):
        # Test the case where all eval workers fail on a multi-agent env with
        # different `policy_mapping_fn` in eval- vs train workers, but we chose
        # to recover.
        config = (
            PPOConfig()
            .api_stack(
                enable_rl_module_and_learner=True,
                enable_env_runner_and_connector_v2=True,
            )
            .env_runners(env_runner_cls=ForwardHealthCheckToEnvWorkerMultiAgent)
            .multi_agent(
                policies={"main", "p0", "p1"},
                policy_mapping_fn=(
                    lambda aid, episode, **kwargs: (
                        "main"
                        if hash(episode.id_) % 2 == aid
                        else "p{}".format(np.random.choice([0, 1]))
                    )
                ),
            )
            .evaluation(
                evaluation_num_env_runners=1,
                evaluation_parallel_to_training=True,
                evaluation_duration="auto",
            )
            .training(model={"fcnet_hiddens": [4]})
        )

        self._do_test_failing_recover(config, multi_agent=True)

    def test_eval_workers_failing_fatal(self):
        # Test the case where all eval workers fail (w/o recovery).
        self._do_test_failing_fatal(
            PPOConfig().training(model={"fcnet_hiddens": [4]}),
            fail_eval=True,
        )

    def test_workers_failing_recover(self):
        # Counter that will survive restarts.
        COUNTER_NAME = "test_workers_fatal_but_recover"
        counter = Counter.options(name=COUNTER_NAME).remote()

        config = (
            PPOConfig()
            .api_stack(
                enable_rl_module_and_learner=True,
                enable_env_runner_and_connector_v2=True,
            )
            .env_runners(
                env_runner_cls=ForwardHealthCheckToEnvWorker,
                num_env_runners=2,
                rollout_fragment_length=16,
            )
            .rl_module(
                model_config_dict={"fcnet_hiddens": [4]},
            )
            .training(
                train_batch_size_per_learner=32,
                minibatch_size=32,
            )
            .environment(
                env="fault_env",
                env_config={
                    # Make both worker idx=1 and 2 fail.
                    "bad_indices": [1, 2],
                    "failure_start_count": 3,
                    "failure_stop_count": 4,
                    "counter": COUNTER_NAME,
                },
            )
            .fault_tolerance(
                recreate_failed_env_runners=True,  # But recover.
                # 0 delay for testing purposes.
                delay_between_env_runner_restarts_s=0,
            )
        )

        # Reset interaciton counter.
        ray.wait([counter.reset.remote()])

        algo = config.build()

        # Before training, 2 healthy workers.
        self.assertEqual(algo.env_runner_group.num_healthy_remote_workers(), 2)
        # Nothing is restarted.
        self.assertEqual(algo.env_runner_group.num_remote_worker_restarts(), 0)

        algo.train()
        wait_for_restore()
        algo.restore_workers(algo.env_runner_group)

        # After training, still 2 healthy workers.
        self.assertEqual(algo.env_runner_group.num_healthy_remote_workers(), 2)
        # Both workers are restarted.
        self.assertEqual(algo.env_runner_group.num_remote_worker_restarts(), 2)

    def test_modules_are_restored_on_recovered_worker(self):
        # Counter that will survive restarts.
        COUNTER_NAME = "test_modules_are_restored_on_recovered_worker"
        counter = Counter.options(name=COUNTER_NAME).remote()

        config = (
            PPOConfig()
            .api_stack(
                enable_rl_module_and_learner=True,
                enable_env_runner_and_connector_v2=True,
            )
            .env_runners(
                env_runner_cls=ForwardHealthCheckToEnvWorkerMultiAgent,
                num_env_runners=2,
                rollout_fragment_length=16,
            )
            .rl_module(
                model_config_dict={"fcnet_hiddens": [4]},
            )
            .training(
                train_batch_size_per_learner=32,
                minibatch_size=32,
            )
            .environment(
                env="multi_agent_fault_env",
                env_config={
                    # Make both worker idx=1 and 2 fail.
                    "bad_indices": [1, 2],
                    "failure_start_count": 3,
                    "failure_stop_count": 4,
                    "counter": COUNTER_NAME,
                },
            )
            .evaluation(
                evaluation_num_env_runners=1,
                evaluation_interval=1,
                evaluation_config=PPOConfig.overrides(
                    recreate_failed_env_runners=True,
                    # Restart the entire eval worker.
                    restart_failed_sub_environments=False,
                    env_config={
                        "evaluation": True,
                        # Make eval worker (index 1) fail.
                        "bad_indices": [1],
                        "failure_start_count": 3,
                        "failure_stop_count": 4,
                        "counter": COUNTER_NAME,
                    },
                ),
            )
            .callbacks(AddModuleCallback)
            .fault_tolerance(
                recreate_failed_env_runners=True,  # But recover.
                # Throwing error in constructor is a bad idea.
                # 0 delay for testing purposes.
                delay_between_env_runner_restarts_s=0,
            )
            .multi_agent(
                policies={"p0"},
                policy_mapping_fn=lambda *a, **kw: "p0",
            )
        )

        # Reset interaction counter.
        ray.wait([counter.reset.remote()])

        algo = config.build()

        # Should have the custom module.
        self.assertIsNotNone(algo.get_module("test_module"))

        # Before train loop, workers are fresh and not recreated.
        self.assertEqual(algo.env_runner_group.num_healthy_remote_workers(), 2)
        self.assertEqual(algo.env_runner_group.num_remote_worker_restarts(), 0)
        self.assertEqual(algo.eval_env_runner_group.num_healthy_remote_workers(), 1)
        self.assertEqual(algo.eval_env_runner_group.num_remote_worker_restarts(), 0)

        algo.train()
        wait_for_restore()
        algo.restore_workers(algo.env_runner_group)
        algo.restore_workers(algo.eval_env_runner_group)

        # Everything healthy again. And all workers have been restarted.
        self.assertEqual(algo.env_runner_group.num_healthy_remote_workers(), 2)
        self.assertEqual(algo.env_runner_group.num_remote_worker_restarts(), 2)
        self.assertEqual(algo.eval_env_runner_group.num_healthy_remote_workers(), 1)
        self.assertEqual(algo.eval_env_runner_group.num_remote_worker_restarts(), 1)

        # Let's verify that our custom module exists on both recovered workers.
        # TODO (sven): Reinstate once EnvRunners moved to new get/set_state APIs (from
        #  get/set_weights).
        # def has_test_module(w):
        #    return "test_module" in w.module

        # Rollout worker has test module.
        # self.assertTrue(
        #    all(algo.env_runner_group.foreach_worker(
        #        has_test_module, local_worker=False
        #    ))
        # )
        # Eval worker has test module.
        # self.assertTrue(
        #    all(
        #        algo.eval_env_runner_group.foreach_worker(
        #            has_test_module, local_worker=False
        #        )
        #    )
        # )

    def test_eval_workers_failing_recover(self):
        # Counter that will survive restarts.
        COUNTER_NAME = "test_eval_workers_fault_but_recover"
        counter = Counter.options(name=COUNTER_NAME).remote()

        config = (
            PPOConfig()
            .api_stack(
                enable_rl_module_and_learner=True,
                enable_env_runner_and_connector_v2=True,
            )
            .env_runners(
                env_runner_cls=ForwardHealthCheckToEnvWorker,
                num_env_runners=2,
                rollout_fragment_length=16,
            )
            .rl_module(
                model_config_dict={"fcnet_hiddens": [4]},
            )
            .training(
                train_batch_size_per_learner=32,
                minibatch_size=32,
            )
            .environment(env="fault_env")
            .evaluation(
                evaluation_num_env_runners=2,
                evaluation_interval=1,
                evaluation_config=PPOConfig.overrides(
                    env_config={
                        "evaluation": True,
                        "p_terminated": 0.0,
                        "max_episode_len": 20,
                        # Make both eval workers fail.
                        "bad_indices": [1, 2],
                        # Env throws error between steps 10 and 12.
                        "failure_start_count": 3,
                        "failure_stop_count": 4,
                        "counter": COUNTER_NAME,
                    },
                ),
            )
            .fault_tolerance(
                recreate_failed_env_runners=True,  # And recover
                # 0 delay for testing purposes.
                delay_between_env_runner_restarts_s=0,
            )
        )

        # Reset interaciton counter.
        ray.wait([counter.reset.remote()])

        algo = config.build()

        # Before train loop, workers are fresh and not recreated.
        self.assertEqual(algo.eval_env_runner_group.num_healthy_remote_workers(), 2)
        self.assertEqual(algo.eval_env_runner_group.num_remote_worker_restarts(), 0)

        algo.train()
        wait_for_restore()
        algo.restore_workers(algo.eval_env_runner_group)

        # Everything still healthy. And all workers are restarted.
        self.assertEqual(algo.eval_env_runner_group.num_healthy_remote_workers(), 2)
        self.assertEqual(algo.eval_env_runner_group.num_remote_worker_restarts(), 2)

    def test_worker_failing_recover_with_hanging_workers(self):
        # Counter that will survive restarts.
        COUNTER_NAME = "test_eval_workers_fault_but_recover"
        counter = Counter.options(name=COUNTER_NAME).remote()

        config = (
            # First thought: We are using an off-policy algorithm here, b/c we have
            # hanging workers (samples may be delayed, thus off-policy?).
            # However, this actually does NOT matter. All synchronously sampling algos
            # (whether off- or on-policy) now have a sampling timeout to NOT block
            # the execution of the algorithm b/c of a single heavily stalling worker.
            # Timeout data (batches or episodes) are discarded.
            SACConfig()
            .api_stack(
                enable_rl_module_and_learner=True,
                enable_env_runner_and_connector_v2=True,
            )
            .training(
                replay_buffer_config={"type": "EpisodeReplayBuffer"},
                # We need to set the base `lr` to `None` b/c new stack SAC has its
                # specific learning rates for actor, critic, and alpha.
                lr=None,
            )
            .env_runners(
                env_runner_cls=ForwardHealthCheckToEnvWorker,
                num_env_runners=3,
                rollout_fragment_length=16,
                sample_timeout_s=5.0,
            )
            .reporting(
                # Make sure each iteration doesn't take too long.
                min_time_s_per_iteration=0.5,
                # Make sure metrics reporting doesn't hang for too long
                # since we will have a hanging worker.
                metrics_episode_collection_timeout_s=1,
            )
            .environment(
                env="fault_env",
                env_config={
                    "action_space": gym.spaces.Box(0, 1, (2,), np.float32),
                    "evaluation": True,
                    "p_terminated": 0.0,
                    "max_episode_len": 20,
                    # Worker 1 and 2 will fail in step().
                    "bad_indices": [1, 2],
                    # Env throws error between steps 3 and 4.
                    "failure_start_count": 3,
                    "failure_stop_count": 4,
                    "counter": COUNTER_NAME,
                    # Worker 2 will hang for long time during init after restart.
                    "init_delay": 3600,
                    "init_delay_indices": [2],
                    # Worker 3 will hang in env.step().
                    "step_delay": 3600,
                    "step_delay_indices": [3],
                },
            )
            .fault_tolerance(
                recreate_failed_env_runners=True,  # And recover
                env_runner_health_probe_timeout_s=0.01,
                env_runner_restore_timeout_s=5,
                delay_between_env_runner_restarts_s=0,  # For testing, no delay.
            )
        )

        # Reset interaciton counter.
        ray.wait([counter.reset.remote()])

        algo = config.build()

        # Before train loop, workers are fresh and not recreated.
        self.assertEqual(algo.env_runner_group.num_healthy_remote_workers(), 3)
        self.assertEqual(algo.env_runner_group.num_remote_worker_restarts(), 0)

        algo.train()
        wait_for_restore(num_restarting_allowed=1)
        # Most importantly, training progresses fine b/c the stalling worker is
        # ignored via a timeout.
        algo.train()

        # 2 healthy remote workers left, although worker 3 is stuck in rollout.
        self.assertEqual(algo.env_runner_group.num_healthy_remote_workers(), 2)
        # Only 1 successful restore, since worker 2 is stuck in indefinite init
        # and can not be properly restored.
        self.assertEqual(algo.env_runner_group.num_remote_worker_restarts(), 1)

    def test_eval_workers_on_infinite_episodes(self):
        """Tests whether eval workers warn appropriately after some episode timeout."""
        # Create infinitely running episodes, but with horizon setting (RLlib will
        # auto-terminate the episode). However, in the eval workers, don't set a
        # horizon -> Expect warning and no proper evaluation results.
        config = (
            PPOConfig()
            .environment(env=RandomEnv, env_config={"p_terminated": 0.0})
            .training(train_batch_size_per_learner=200)
            .evaluation(
                evaluation_num_env_runners=1,
                evaluation_interval=1,
                evaluation_sample_timeout_s=2.0,
            )
        )
        algo = config.build()
        results = algo.train()
        self.assertTrue(
            np.isnan(
                results[EVALUATION_RESULTS][ENV_RUNNER_RESULTS][EPISODE_RETURN_MEAN]
            )
        )


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__]))
