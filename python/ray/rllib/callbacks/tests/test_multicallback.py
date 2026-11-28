import functools
import unittest

import ray
from ray.rllib.algorithms import PPOConfig
from ray.rllib.callbacks.callbacks import RLlibCallback


class TestMultiCallback(unittest.TestCase):
    """A tests suite to test the `MultiCallback`."""

    @classmethod
    def setUp(cls) -> None:
        ray.init()

    @classmethod
    def tearDown(cls) -> None:
        ray.shutdown()

    def test_multicallback_with_custom_callback_function(self):
        """Tests if callbacks in `MultiCallback` get executed.

        This also tests, if multiple callbacks from different sources, i.e.
        `callback_class` and `on_episode_step` run correctly.
        """
        # Define two standard `RLlibCallback`.
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

                metrics_logger.log_value("callback_1", 1, reduce="mean")

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

                metrics_logger.log_value("callback_2", 2, reduce="mean")

        # Define a custom callback function.
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

            metrics_logger.log_value("custom_callback", 3, reduce="mean")

        # Configure the algorithm.
        config = (
            PPOConfig()
            .environment("CartPole-v1")
            .api_stack(
                enable_env_runner_and_connector_v2=True,
                enable_rl_module_and_learner=True,
            )
            # Use the callbacks and callback function.
            .callbacks(
                callbacks_class=[TestRLlibCallback1, TestRLlibCallback2],
                on_episode_step=custom_on_episode_step_callback,
            )
        )

        # Build the algorithm. At this stage, callbacks get already validated.
        algo = config.build()

        # Run a few training iterations and check, if the metrics defined in the
        # callbacks made it into the results. Furthermore, check, if the values are correct.
        for _ in range(3):
            results = algo.train()
            self.assertIn("callback_1", results["env_runners"])
            self.assertIn("callback_2", results["env_runners"])
            self.assertIn("custom_callback", results["env_runners"])
            self.assertAlmostEqual(results["env_runners"]["callback_1"], 1)
            self.assertAlmostEqual(results["env_runners"]["callback_2"], 2)
            self.assertAlmostEqual(results["env_runners"]["custom_callback"], 3)

        algo.stop()

    def test_callbacks_class_accepts_callables(self):
        """Tests if `callbacks_class` accepts callables returning a callback.

        A `functools.partial` over an `RLlibCallback` subclass lets users pass
        constructor arguments to their callback.
        """

        class ConfigurableCallback(RLlibCallback):
            def __init__(self, key, value):
                super().__init__()
                self.key = key
                self.value = value

            def on_episode_step(self, *, episode, metrics_logger=None, **kwargs):
                metrics_logger.log_value(self.key, self.value, reduce="mean")

        class PlainCallback(RLlibCallback):
            def on_episode_step(self, *, episode, metrics_logger=None, **kwargs):
                metrics_logger.log_value("plain_callback", 1, reduce="mean")

        config = (
            PPOConfig()
            .environment("CartPole-v1")
            .api_stack(
                enable_env_runner_and_connector_v2=True,
                enable_rl_module_and_learner=True,
            )
            # Mix a callable and a class in the same list.
            .callbacks(
                callbacks_class=[
                    functools.partial(ConfigurableCallback, key="partial_1", value=5),
                    PlainCallback,
                ],
            )
        )

        algo = config.build()

        # The Algorithm creates one callback object per entry.
        self.assertEqual(len(algo.callbacks), 2)
        self.assertIsInstance(algo.callbacks[0], ConfigurableCallback)
        self.assertEqual(algo.callbacks[0].value, 5)
        self.assertIsInstance(algo.callbacks[1], PlainCallback)

        # The EnvRunners create them, too, and pass on the constructor arguments.
        results = algo.train()
        self.assertAlmostEqual(results["env_runners"]["partial_1"], 5)
        self.assertAlmostEqual(results["env_runners"]["plain_callback"], 1)

        algo.stop()

        # A single callable (not in a list) works as well.
        config.callbacks(
            callbacks_class=functools.partial(
                ConfigurableCallback, key="partial_2", value=7
            )
        )
        algo = config.build()
        self.assertEqual(len(algo.callbacks), 1)
        results = algo.train()
        self.assertAlmostEqual(results["env_runners"]["partial_2"], 7)
        algo.stop()

    def test_multicallback_validation_error(self):
        """Check, if the validation safeguard catches wrong `MultiCallback`s."""
        with self.assertRaises(ValueError):
            (
                PPOConfig()
                .environment("CartPole-v1")
                .api_stack(
                    enable_env_runner_and_connector_v2=True,
                    enable_rl_module_and_learner=True,
                )
                # This is wrong b/c it needs callables.
                .callbacks(callbacks_class=["TestRLlibCallback1", "TestRLlibCallback2"])
            )

    def test_single_callback_validation_error(self):
        """Tests if the validation safeguard catches wrong `RLlibCallback`s."""
        with self.assertRaises(ValueError):
            (
                PPOConfig()
                .environment("CartPole-v1")
                .api_stack(
                    enable_env_runner_and_connector_v2=True,
                    enable_rl_module_and_learner=True,
                )
                # This is wrong b/c it needs callables.
                .callbacks(callbacks_class="TestRLlibCallback")
            )


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__]))
