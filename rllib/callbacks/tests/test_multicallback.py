import unittest
import ray
from ray.rllib.algorithms import PPOConfig
from ray.rllib.callbacks.callbacks import RLlibCallback


class TestMultiCallback(unittest.TestCase):
    @classmethod
    def setUp(cls) -> None:
        ray.init()

    @classmethod
    def tearDown(cls) -> None:
        ray.shutdown()

    def test_multicallback_with_custom_callback_function(self):
        class TestRLlibCallback1(RLlibCallback):
            def on_episode_step(
                self,
                *,
                episode,
                env_runner=None,
                metrics_logger=None,
                env=None,
                env_index,
                rl_module=None,
                worker=None,
                base_env=None,
                policies=None,
                **kwargs
            ):

                metrics_logger.log_value(
                    "callback_1", 1, reduce="mean", clear_on_reduce=True
                )

        class TestRLlibCallback2(RLlibCallback):
            def on_episode_step(
                self,
                *,
                episode,
                env_runner=None,
                metrics_logger=None,
                env=None,
                env_index,
                rl_module=None,
                worker=None,
                base_env=None,
                policies=None,
                **kwargs
            ):

                metrics_logger.log_value(
                    "callback_2", 2, reduce="mean", clear_on_reduce=True
                )

        def custom_on_episode_step_callback(
            episode,
            env_runner=None,
            metrics_logger=None,
            env=None,
            env_index=None,
            rl_module=None,
            worker=None,
            base_env=None,
            policies=None,
            **kwargs
        ):

            metrics_logger.log_value(
                "custom_callback", 3, reduce="mean", clear_on_reduce=True
            )

        config = (
            PPOConfig()
            .environment("CartPole-v1")
            .api_stack(
                enable_env_runner_and_connector_v2=True,
                enable_rl_module_and_learner=True,
            )
            .callbacks(
                callbacks_class=[TestRLlibCallback1, TestRLlibCallback2],
                on_episode_step=custom_on_episode_step_callback,
            )
        )

        algo = config.build()

        for _ in range(10):
            results = algo.train()
            self.assertIn("callback_1", results["env_runners"])
            self.assertIn("callback_2", results["env_runners"])
            self.assertIn("custom_callback", results["env_runners"])
            self.assertAlmostEquals(results["env_runners"]["callback_1"], 1)
            self.assertAlmostEquals(results["env_runners"]["callback_2"], 2)
            self.assertAlmostEquals(results["env_runners"]["custom_callback"], 3)

        algo.stop()

    def test_multicallback_validation_error(self):

        with self.assertRaises(ValueError):
            config = (
                PPOConfig()
                .environment("CartPole-v1")
                .api_stack(
                    enable_env_runner_and_connector_v2=True,
                    enable_rl_module_and_learner=True,
                )
                .callbacks(callbacks_class=["TestRLlibCallback1", "TestRLlibCallback2"])
            )

    def test_single_callback_validation_error(self):

        with self.assertRaises(ValueError):
            config = (
                PPOConfig()
                .environment("CartPole-v1")
                .api_stack(
                    enable_env_runner_and_connector_v2=True,
                    enable_rl_module_and_learner=True,
                )
                .callbacks(callbacks_class="TestRLlibCallback")
            )


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
