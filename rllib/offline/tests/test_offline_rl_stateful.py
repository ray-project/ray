import unittest
from pathlib import Path

import numpy as np

import ray
from ray.rllib.algorithms.bc import BCConfig
from ray.rllib.core.columns import Columns
from ray.rllib.core.learner.training_data import TrainingData
from ray.rllib.core.rl_module.default_model_config import DefaultModelConfig
from ray.rllib.env import INPUT_ENV_SPACES
from ray.rllib.env.single_agent_episode import SingleAgentEpisode
from ray.rllib.examples.envs.classes.stateless_cartpole import StatelessCartPole
from ray.rllib.offline.offline_prelearner import OfflinePreLearner
from ray.rllib.policy.sample_batch import MultiAgentBatch, SampleBatch
from ray.rllib.utils import unflatten_dict


class OfflineRLStatefulTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ray.init()

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def setUp(self):
        # Define the path to the offline data.
        offline_data_path = Path(__file__).parent / "data/statelesscartpole"
        # Define the BC config.
        self.config = (
            BCConfig()
            .environment(StatelessCartPole)
            # Note, the `input_` argument is the major argument for the
            # new offline API. Via the `input_read_method_kwargs` the
            # arguments for the `ray.data.Dataset` read method can be
            # configured. The read method needs at least as many blocks
            # as remote learners.
            .offline_data(
                input_=[
                    offline_data_path.as_posix(),
                    # "s3://anonymous@ray-example-data/rllib/offline-data/statelesscartpole"
                ],
                input_read_episodes=True,
                input_read_batch_size=1,
                # Concurrency defines the number of processes that run the
                # `map_batches` transformations. This should be aligned with the
                # 'prefetch_batches' argument in 'iter_batches_kwargs'.
                map_batches_kwargs={"concurrency": 2, "num_cpus": 1},
                # Default for this test: materialize both data and mapped data.
                materialize_data=True,
                materialize_mapped_data=True,
                # This data set is small so do not prefetch too many batches and use no
                # local shuffle.
                iter_batches_kwargs={"prefetch_batches": 1},
                # The number of iterations to be run per learner when in multi-learner
                # mode in a single RLlib training iteration. Leave this to `None` to
                # run an entire epoch on the dataset during a single RLlib training
                # iteration.
                dataset_num_iters_per_learner=5,
            )
            .training(
                train_batch_size_per_learner=256,
                lr=0.0008,
            )
            .rl_module(
                model_config=DefaultModelConfig(
                    max_seq_len=20,
                    use_lstm=True,
                ),
            )
            .evaluation(
                evaluation_interval=1,
                evaluation_num_env_runners=1,
                evaluation_duration=5,
                evaluation_duration_unit="episodes",
                evaluation_parallel_to_training=False,
            )
        )
        # Build the algorithm.
        self.algo = self.config.build()

    def tearDown(self):
        self.algo.stop()

    def test_training_on_single_episode_and_evaluate(self):
        """Trains on a single episode from the recorded dataset and evaluates.

        Uses a zero initial state for training (from `RLModule`).
        """
        # Load these packages inline.
        import msgpack
        import msgpack_numpy as mnp

        # Load the dataset.
        ds = self.algo.offline_data.data

        # Take a single-row batch (one episode).
        batch = ds.take_batch(1)

        # Read the episodes and decode them.
        episodes = [
            SingleAgentEpisode.from_state(
                msgpack.unpackb(state, object_hook=mnp.decode)
            )
            for state in batch["item"]
        ][:1]
        # Get the episode return.
        episode_return = episodes[0].get_return()
        print(f"Found episode with return {episode_return}")

        # Assert the episode has a decent return.
        assert episodes[0].get_return() > 350.0, "Return must be >350.0"

        # Remove recorded states.
        if Columns.STATE_OUT in episodes[0].extra_model_outputs.keys():
            del episodes[0].extra_model_outputs[Columns.STATE_OUT]
        if Columns.STATE_IN in episodes[0].extra_model_outputs.keys():
            del episodes[0].extra_model_outputs[Columns.STATE_IN]

        # Build the learner connector.
        obs_space, action_space = self.algo.offline_data.spaces[INPUT_ENV_SPACES]
        learner_connector = self.algo.config.build_learner_connector(
            input_observation_space=obs_space,
            input_action_space=action_space,
        )
        # Run the learner connector on the episode.
        processed_batch = learner_connector(
            rl_module=self.algo.learner_group._learner.module,
            batch={},
            episodes=episodes,
            shared_data={},
            # TODO (simon): Add MetricsLogger to non-Learner components that have a
            #  LearnerConnector pipeline.
            metrics=None,
        )

        # Create a MA batch from the processed batch and a TrainingData object.
        ma_batch = MultiAgentBatch(
            policy_batches={
                "default_policy": SampleBatch(processed_batch["default_policy"])
            },
            env_steps=np.prod(processed_batch["default_policy"]["obs"].shape[:-1]),
        )
        training_data = TrainingData(batch=ma_batch)

        # Overfit on this single episode.
        i = 0
        while True:
            i += 1
            learner_results = self.algo.learner_group.update(
                training_data=training_data,
                minibatch_size=ma_batch["default_policy"].count,
                num_iters=self.algo.config.dataset_num_iters_per_learner,
                **self.algo.offline_data.iter_batches_kwargs,
            )
            if i % 10 == 0:
                loss = learner_results[0]["default_policy"]["policy_loss"].peek()
                print(f"Iteration {i}: policy_loss: {loss}")
                if np.isclose(loss, 1e-4, atol=1e-5) or i >= 100:
                    break

        # Evaluation
        # Get the latest RLModule state from the learner and synchronize
        # the eval env runners.
        rl_module_state = self.algo.learner_group.get_state()["learner"]["rl_module"]

        self.algo.eval_env_runner_group.foreach_env_runner(
            func="set_state",
            local_env_runner=False,
            kwargs={"state": {"rl_module": rl_module_state}},
        )

        # Evaluate the updated policy for 5 episodes.
        eval_episodes = self.algo.eval_env_runner_group.foreach_env_runner(
            func=lambda er, duration=self.config.evaluation_duration: er.sample(
                num_episodes=duration, explore=False
            ),  # self._remote_eval_episode_fn,
            local_env_runner=False,
        )
        # Assert the eval return is decent.
        episode_return_mean = np.mean([ep.get_return() for ep in eval_episodes[0]])
        self.assertGreaterEqual(
            episode_return_mean,
            100.0,
            f"Eval return must be >100.0 but is {episode_return_mean}",
        )
        print(f"Eval episodes returns: {episode_return_mean}")

    def test_training_with_recorded_states_on_single_episode_and_evaluate(self):
        """Trains on a single episode from the recorded dataset and evaluates.

        Uses recorded states for training.
        """
        # Load these packages inline.
        import msgpack
        import msgpack_numpy as mnp

        # Load the dataset.
        ds = self.algo.offline_data.data

        # Take a single-row batch (one episode).
        batch = ds.take_batch(1)

        # Read the episodes and decode them.
        episodes = [
            SingleAgentEpisode.from_state(
                msgpack.unpackb(state, object_hook=mnp.decode)
            )
            for state in batch["item"]
        ][:1]
        # Get the episode return.
        episode_return = episodes[0].get_return()
        print(f"Found episode with return {episode_return}")

        # Assert the episode has a decent return.
        assert episodes[0].get_return() > 350.0, "Return must be >350.0"

        # Build the learner connector.
        obs_space, action_space = self.algo.offline_data.spaces[INPUT_ENV_SPACES]
        learner_connector = self.algo.config.build_learner_connector(
            input_observation_space=obs_space,
            input_action_space=action_space,
        )
        # Run the learner connector on the episode.
        processed_batch = learner_connector(
            rl_module=self.algo.learner_group._learner.module,
            batch={},
            episodes=episodes,
            shared_data={},
            # TODO (simon): Add MetricsLogger to non-Learner components that have a
            #  LearnerConnector pipeline.
            metrics=None,
        )

        # Create a MA batch from the processed batch and a TrainingData object.
        ma_batch = MultiAgentBatch(
            policy_batches={
                "default_policy": SampleBatch(processed_batch["default_policy"])
            },
            env_steps=np.prod(processed_batch["default_policy"]["obs"].shape[:-1]),
        )
        training_data = TrainingData(batch=ma_batch)

        # Overfit on this single episode.
        i = 0
        while True:
            i += 1
            learner_results = self.algo.learner_group.update(
                training_data=training_data,
                minibatch_size=ma_batch["default_policy"].count,
                num_iters=self.algo.config.dataset_num_iters_per_learner,
                **self.algo.offline_data.iter_batches_kwargs,
            )
            if i % 10 == 0:
                loss = learner_results[0]["default_policy"]["policy_loss"].peek()
                print(f"Iteration {i}: policy_loss: {loss}")
                if np.isclose(loss, 1e-4, atol=1e-5) or i >= 100:
                    break

        # Evaluation
        # Get the latest RLModule state from the learner and synchronize
        # the eval env runners.
        rl_module_state = self.algo.learner_group.get_state()["learner"]["rl_module"]

        self.algo.eval_env_runner_group.foreach_env_runner(
            func="set_state",
            local_env_runner=False,
            kwargs={"state": {"rl_module": rl_module_state}},
        )

        # Evaluate the updated policy for 5 episodes.
        eval_episodes = self.algo.eval_env_runner_group.foreach_env_runner(
            func=lambda er, duration=self.config.evaluation_duration: er.sample(
                num_episodes=duration, explore=False
            ),  # self._remote_eval_episode_fn,
            local_env_runner=False,
        )
        # Assert the eval return is decent.
        episode_return_mean = np.mean([ep.get_return() for ep in eval_episodes[0]])
        self.assertGreaterEqual(
            episode_return_mean,
            100.0,
            f"Eval return must be >100.0 but is {episode_return_mean}",
        )
        print(f"Eval episodes returns: {episode_return_mean}")

    def test_training_with_recorded_states_on_single_batch_and_evaluate(self):
        """Trains on a single batch from the recorded dataset and evaluates.

        Uses recorded states for training.
        """
        import msgpack
        import msgpack_numpy as mnp

        # Assign the dataset.
        ds = self.algo.offline_data.data
        # Initialize the OfflinePreLearner.
        oplr = OfflinePreLearner(
            config=self.algo.config,
            spaces=self.algo.offline_data.spaces[INPUT_ENV_SPACES],
            module_spec=self.algo.offline_data.module_spec,
            module_state=self.algo.learner_group._learner.get_state()["rl_module"],
        )

        # Take a single-row batch (one episode).
        batch = ds.take_batch(1)

        # Read the episodes and decode them.
        episodes = [
            SingleAgentEpisode.from_state(
                msgpack.unpackb(state, object_hook=mnp.decode)
            )
            for state in batch["item"]
        ][:1]
        # Get the episode return.
        episode_return = episodes[0].get_return()
        print(f"Found episode with return {episode_return}")

        # Assert the episode has a decent return.
        assert episodes[0].get_return() > 350.0, "Return must be >350.0"

        # Run the OfflinePreLearner on the batch.
        processed_batch = oplr(batch)
        # Create a MA batch from the processed batch and a TrainingData object.
        processed_batch = unflatten_dict(processed_batch)
        ma_batch = MultiAgentBatch(
            policy_batches={
                "default_policy": SampleBatch(processed_batch["default_policy"])
            },
            env_steps=np.prod(processed_batch["default_policy"]["obs"].shape[:-1]),
        )
        training_data = TrainingData(batch=ma_batch)

        # Overfit on this single batch.
        i = 0
        while True:
            i += 1
            learner_results = self.algo.learner_group.update(
                training_data=training_data,
                minibatch_size=self.algo.config.train_batch_size_per_learner,
                num_iters=self.algo.config.dataset_num_iters_per_learner,
                **self.algo.offline_data.iter_batches_kwargs,
            )
            if i % 10 == 0:
                loss = learner_results[0]["default_policy"]["policy_loss"].peek()
                print(f"Iteration {i}: policy_loss: {loss}")
                if np.isclose(loss, 1e-4, atol=1e-5) or i >= 100:
                    break

        # Evaluation
        # Get the latest RLModule state from the learner and synchronize
        # the eval env runners.
        rl_module_state = self.algo.learner_group.get_state()["learner"]["rl_module"]
        self.algo.eval_env_runner_group.foreach_env_runner(
            func="set_state",
            local_env_runner=False,
            kwargs={"state": {"rl_module": rl_module_state}},
        )

        eval_episodes = self.algo.eval_env_runner_group.foreach_env_runner(
            func=lambda er, duration=self.config.evaluation_duration: er.sample(
                num_episodes=duration, explore=False
            ),  # self._remote_eval_episode_fn,
            local_env_runner=False,
        )
        # Assert the eval return is decent.
        episode_return_mean = np.mean([ep.get_return() for ep in eval_episodes[0]])
        self.assertGreaterEqual(
            episode_return_mean,
            100.0,
            f"Eval return must be >100.0 but is {episode_return_mean}",
        )
        print(f"Eval episodes returns: {episode_return_mean}")


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__]))
