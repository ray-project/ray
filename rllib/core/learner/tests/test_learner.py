import tempfile
import unittest

import gymnasium as gym
import numpy as np

import ray
from ray.rllib.core import DEFAULT_MODULE_ID
from ray.rllib.core.learner.learner import LR_KEY, Learner, UpdatePlan
from ray.rllib.core.testing.testing_learner import BaseTestingAlgorithmConfig
from ray.rllib.policy.sample_batch import MultiAgentBatch, SampleBatch
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.metrics import (
    ALL_MODULES,
    LEARNER_MODULE_STEPS_DROPPED_ON_SKIP_LIFETIME,
    LEARNER_UPDATE_SKIPPED_EMPTY_BATCH_LIFETIME,
    LEARNER_UPDATE_SKIPPED_FOR_PEER_LIFETIME,
    MODULE_TRAIN_BATCH_SIZE_MEAN,
    NUM_ENV_STEPS_SAMPLED_LIFETIME,
    NUM_ENV_STEPS_TRAINED,
    NUM_ENV_STEPS_TRAINED_LIFETIME,
    NUM_MODULE_STEPS_TRAINED,
    NUM_MODULE_STEPS_TRAINED_LIFETIME,
    WEIGHTS_SEQ_NO,
)
from ray.rllib.utils.numpy import convert_to_numpy
from ray.rllib.utils.test_utils import check, get_cartpole_dataset_reader

torch, _ = try_import_torch()


class TestLearner(unittest.TestCase):

    ENV = gym.make("CartPole-v1")

    @classmethod
    def setUp(cls) -> None:
        ray.init()

    @classmethod
    def tearDown(cls) -> None:
        ray.shutdown()

    def test_end_to_end_update(self):
        """Tests the end-to-end update process for a single-agent scenario.

        We check that the loss is decreasing and that the metrics are where we expect them and that values are as expected.
        """

        config = BaseTestingAlgorithmConfig()

        learner = config.build_learner(env=self.ENV)
        reader = get_cartpole_dataset_reader(batch_size=512)

        for seq_num in range(1, 1000):
            batch = reader.next().as_multi_agent()
            batch = learner._convert_batch_type(batch)
            results = learner.update(batch=batch)
            self.assertEqual(
                batch.count, results[DEFAULT_MODULE_ID][MODULE_TRAIN_BATCH_SIZE_MEAN]
            )

            self.assertEqual(
                batch.count, results[DEFAULT_MODULE_ID][NUM_MODULE_STEPS_TRAINED]
            )
            self.assertEqual(
                batch.count,
                results[DEFAULT_MODULE_ID][NUM_MODULE_STEPS_TRAINED_LIFETIME],
            )
            self.assertEqual(seq_num, results[DEFAULT_MODULE_ID][WEIGHTS_SEQ_NO])
            self.assertEqual(
                batch.count, results[DEFAULT_MODULE_ID][MODULE_TRAIN_BATCH_SIZE_MEAN]
            )
            self.assertTrue(learner.TOTAL_LOSS_KEY in results[DEFAULT_MODULE_ID])
            self.assertEqual(
                batch.count, results[ALL_MODULES][NUM_MODULE_STEPS_TRAINED]
            )
            self.assertEqual(
                batch.count, results[ALL_MODULES][NUM_MODULE_STEPS_TRAINED_LIFETIME]
            )
            self.assertEqual(batch.count, results[ALL_MODULES][NUM_ENV_STEPS_TRAINED])
            self.assertEqual(
                batch.count, results[ALL_MODULES][NUM_ENV_STEPS_TRAINED_LIFETIME]
            )

        self.assertLess(results[DEFAULT_MODULE_ID][Learner.TOTAL_LOSS_KEY], 0.58)

    def test_compute_gradients(self):
        """Tests the compute_gradients correctness.

        Tests that if we sum all the trainable variables the gradient of output w.r.t.
        the weights is all ones.
        """
        config = BaseTestingAlgorithmConfig()

        learner = config.build_learner(env=self.ENV)

        params = learner.get_parameters(learner.module[DEFAULT_MODULE_ID])

        tape = None
        loss_per_module = {ALL_MODULES: sum(param.sum() for param in params)}

        gradients = learner.compute_gradients(loss_per_module, gradient_tape=tape)

        # Type should be a mapping from ParamRefs to gradients.
        self.assertIsInstance(gradients, dict)

        for grad in gradients.values():
            check(grad, np.ones(grad.shape))

    def test_postprocess_gradients(self):
        """Tests the base grad clipping logic in `postprocess_gradients()`."""

        # Clip by value only.
        config = BaseTestingAlgorithmConfig().training(
            lr=0.0003, grad_clip=0.75, grad_clip_by="value"
        )

        learner = config.build_learner(env=self.ENV)
        # Pretend our computed gradients are our weights + 1.0.
        grads = {
            learner.get_param_ref(v): v + 1.0
            for v in learner.get_parameters(learner.module[DEFAULT_MODULE_ID])
        }
        # Call the learner's postprocessing method.
        processed_grads = list(learner.postprocess_gradients(grads).values())
        # Check clipped gradients.
        # No single gradient must be larger than 0.1 or smaller than -0.1:
        self.assertTrue(
            all(
                np.max(grad) <= config.grad_clip and np.min(grad) >= -config.grad_clip
                for grad in convert_to_numpy(processed_grads)
            )
        )

        # Clip by norm.
        config.grad_clip = 1.0
        config.grad_clip_by = "norm"
        learner = config.build_learner(env=self.ENV)
        # Pretend our computed gradients are our weights + 1.0.
        grads = {
            learner.get_param_ref(v): v + 1.0
            for v in learner.get_parameters(learner.module[DEFAULT_MODULE_ID])
        }
        # Call the learner's postprocessing method.
        processed_grads = list(learner.postprocess_gradients(grads).values())
        # Check clipped gradients.
        for proc_grad, grad in zip(
            convert_to_numpy(processed_grads),
            convert_to_numpy(list(grads.values())),
        ):
            l2_norm = np.sqrt(np.sum(grad**2.0))
            if l2_norm > config.grad_clip:
                check(proc_grad, grad * (config.grad_clip / l2_norm))

        # Clip by global norm.
        config.grad_clip = 5.0
        config.grad_clip_by = "global_norm"
        learner = config.build_learner(env=self.ENV)
        # Pretend our computed gradients are our weights + 1.0.
        grads = {
            learner.get_param_ref(v): v + 1.0
            for v in learner.get_parameters(learner.module[DEFAULT_MODULE_ID])
        }
        # Call the learner's postprocessing method.
        processed_grads = list(learner.postprocess_gradients(grads).values())
        # Check clipped gradients.
        global_norm = np.sqrt(
            np.sum(
                [np.sum(grad**2.0) for grad in convert_to_numpy(list(grads.values()))]
            )
        )
        if global_norm > config.grad_clip:
            for proc_grad, grad in zip(
                convert_to_numpy(processed_grads),
                grads.values(),
            ):
                check(proc_grad, grad * (config.grad_clip / global_norm))

    def test_apply_gradients(self):
        """Tests the apply_gradients correctness.

        Tests that if we apply gradients of all ones, the new params are equal to the
        standard SGD/Adam update rule.
        """
        config = BaseTestingAlgorithmConfig().training(lr=0.0003)

        learner = config.build_learner(env=self.ENV)

        # calculated the expected new params based on gradients of all ones.
        params = learner.get_parameters(learner.module[DEFAULT_MODULE_ID])
        n_steps = 100
        expected = [
            (
                convert_to_numpy(param)
                - n_steps * learner.config.lr * np.ones(param.shape)
            )
            for param in params
        ]
        for _ in range(n_steps):
            gradients = {learner.get_param_ref(p): torch.ones_like(p) for p in params}
            learner.apply_gradients(gradients)

        check(params, expected)

    def test_add_remove_module(self):
        """Tests the compute/apply_gradients with add/remove modules.

        Tests that if we add a module with SGD optimizer with a known lr (different
        from default), and remove the default module, with a loss that is the sum of
        all variables the updated parameters follow the SGD update rule.
        """
        config = BaseTestingAlgorithmConfig().training(lr=0.0003)

        learner = config.build_learner(env=self.ENV)
        rl_module_spec = config.get_default_rl_module_spec()
        rl_module_spec.observation_space = self.ENV.observation_space
        rl_module_spec.action_space = self.ENV.action_space
        learner.add_module(
            module_id="test",
            module_spec=rl_module_spec,
        )
        learner.remove_module(DEFAULT_MODULE_ID)

        # only test module should be left
        self.assertEqual(set(learner.module.keys()), {"test"})

        # calculated the expected new params based on gradients of all ones.
        params = learner.get_parameters(learner.module["test"])
        n_steps = 100
        expected = [
            convert_to_numpy(param) - n_steps * learner.config.lr * np.ones(param.shape)
            for param in params
        ]
        for _ in range(n_steps):
            tape = None
            loss_per_module = {ALL_MODULES: sum(param.sum() for param in params)}
            gradients = learner.compute_gradients(loss_per_module, gradient_tape=tape)
            learner.apply_gradients(gradients)

        check(params, expected)

    def test_save_to_path_and_restore_from_path(self):
        """Tests, whether a Learner's state is properly saved and restored."""
        config = BaseTestingAlgorithmConfig()

        # Get a Learner instance for the framework and env.
        learner1 = config.build_learner(env=self.ENV)
        with tempfile.TemporaryDirectory() as tmpdir:
            learner1.save_to_path(tmpdir)

            learner2 = config.build_learner(env=self.ENV)
            learner2.restore_from_path(tmpdir)
            self._check_learner_states("torch", learner1, learner2)

        # Add a module then save/load and check states.
        with tempfile.TemporaryDirectory() as tmpdir:
            rl_module_spec = config.get_default_rl_module_spec()
            rl_module_spec.observation_space = self.ENV.observation_space
            rl_module_spec.action_space = self.ENV.action_space
            learner1.add_module(
                module_id="test",
                module_spec=rl_module_spec,
            )
            learner1.save_to_path(tmpdir)
            learner2 = Learner.from_checkpoint(tmpdir)
            self._check_learner_states("torch", learner1, learner2)

        # Remove a module then save/load and check states.
        with tempfile.TemporaryDirectory() as tmpdir:
            learner1.remove_module(module_id=DEFAULT_MODULE_ID)
            learner1.save_to_path(tmpdir)
            learner2 = Learner.from_checkpoint(tmpdir)
            self._check_learner_states("torch", learner1, learner2)

    def _check_learner_states(self, framework, learner1, learner2):
        check(learner1.module.get_state(), learner2.module.get_state())
        check(learner1._get_optimizer_state(), learner2._get_optimizer_state())
        check(learner1._module_optimizers, learner2._module_optimizers)

    def test_multi_agent_learner_results(self):
        """Tests the learner results for a multi-agent scenario.

        We check that all metrics are where we expect them and that values are as expected.
        """
        config = BaseTestingAlgorithmConfig()

        learner = config.build_learner(env=self.ENV)
        learner.remove_module(module_id=DEFAULT_MODULE_ID)
        learner.add_module(
            module_id="mod1", module_spec=config.get_rl_module_spec(env=self.ENV)
        )
        learner.add_module(
            module_id="mod2", module_spec=config.get_rl_module_spec(env=self.ENV)
        )
        reader = get_cartpole_dataset_reader(batch_size=512)

        results = {}
        for seq_num in range(1, 5):
            batch1 = reader.next()
            batch2 = reader.next()
            multi_agent_batch = MultiAgentBatch(
                {"mod1": batch1, "mod2": batch2}, batch1.count + batch2.count
            )
            batch = learner._convert_batch_type(multi_agent_batch)
            results = learner.update(batch)
            # Lifetime steps are aggregated at the root, so the return value in the results will contain only the last step.
            for module_id, sa_batch_count in zip(
                ["mod1", "mod2"], [batch1.count, batch2.count]
            ):
                self.assertEqual(
                    sa_batch_count,
                    results[module_id][NUM_MODULE_STEPS_TRAINED_LIFETIME],
                )
                self.assertEqual(seq_num, results[module_id][WEIGHTS_SEQ_NO])
                self.assertEqual(
                    sa_batch_count, results[module_id][MODULE_TRAIN_BATCH_SIZE_MEAN]
                )
                # We don't know what the value should be, just check for existence.
                self.assertTrue(learner.TOTAL_LOSS_KEY in results[module_id])

            self.assertEqual(
                batch1.count + batch2.count,
                results[ALL_MODULES][NUM_MODULE_STEPS_TRAINED_LIFETIME],
            )
            self.assertEqual(
                batch1.count + batch2.count,
                results[ALL_MODULES][NUM_MODULE_STEPS_TRAINED],
            )
            self.assertEqual(
                batch1.count + batch2.count,
                results[ALL_MODULES][NUM_ENV_STEPS_TRAINED_LIFETIME],
            )
            self.assertEqual(
                batch1.count + batch2.count, results[ALL_MODULES][NUM_ENV_STEPS_TRAINED]
            )

    def test_update_empty_batch_is_skipped(self):
        """Tests that `update()` skips the gradient step for an empty train batch.

        The gradient-based update's hooks are skipped with it: they bracket work
        that did not happen, and anything they step (target networks, schedules) or
        read back (this update's metrics) has nothing behind it.
        """
        from unittest import mock

        learner = BaseTestingAlgorithmConfig().build_learner(env=self.ENV)
        timesteps = {NUM_ENV_STEPS_SAMPLED_LIFETIME: 0}

        def check_skipped(results):
            all_modules = results[ALL_MODULES]
            self.assertEqual(
                1, all_modules[LEARNER_UPDATE_SKIPPED_EMPTY_BATCH_LIFETIME]
            )
            self.assertEqual(
                0, all_modules.get(LEARNER_UPDATE_SKIPPED_FOR_PEER_LIFETIME, 0)
            )
            self.assertEqual(
                0, all_modules[LEARNER_MODULE_STEPS_DROPPED_ON_SKIP_LIFETIME]
            )
            # The module carries only what building it logged: no loss, because no
            # gradient step ran, and no learning rate, because the hook that logs it
            # did not run either.
            module_results = results[DEFAULT_MODULE_ID]
            self.assertNotIn(learner.TOTAL_LOSS_KEY, module_results)
            self.assertFalse([key for key in module_results if LR_KEY in key])

        with mock.patch.object(learner, "before_gradient_based_update") as before, (
            mock.patch.object(learner, "after_gradient_based_update")
        ) as after:
            # Both ways an empty batch reaches `update()`.
            check_skipped(
                learner.update(
                    batch=MultiAgentBatch(policy_batches={}, env_steps=0),
                    timesteps=timesteps,
                )
            )
            check_skipped(learner.update(episodes=[], timesteps=timesteps))
            self.assertEqual(0, before.call_count)
            self.assertEqual(0, after.call_count)

            # A real batch after the skips must still train, hooks and all.
            reader = get_cartpole_dataset_reader(batch_size=512)
            batch = learner._convert_batch_type(reader.next().as_multi_agent())
            results = learner.update(batch=batch)
            self.assertEqual(1, before.call_count)
            self.assertEqual(1, after.call_count)
        self.assertTrue(learner.TOTAL_LOSS_KEY in results[DEFAULT_MODULE_ID])

    def test_should_skip_update_single_learner(self):
        """`_should_skip_update` defaults to "no module data"; without DDP
        (`num_learners <= 1`) the group sync has nobody to agree with and must pass
        the decision through unchanged, without communicating.
        """
        config = BaseTestingAlgorithmConfig().learners(num_learners=0)
        learner = config.build_learner(env=self.ENV)
        self.assertTrue(
            learner._should_skip_update(MultiAgentBatch(policy_batches={}, env_steps=0))
        )
        # A shard can carry ModuleIDs and still hold no timesteps: `ShardBatchIterator`
        # keeps every ModuleID when it splits a batch too small to give each Learner a
        # row. That is just as empty, and skipping it is what keeps the group in sync.
        empty_module_batch = MultiAgentBatch(
            {DEFAULT_MODULE_ID: SampleBatch({"obs": np.zeros((0, 4), np.float32)})},
            env_steps=0,
        )
        self.assertTrue(learner._should_skip_update(empty_module_batch))
        partly_empty_batch = MultiAgentBatch(
            {
                DEFAULT_MODULE_ID: SampleBatch({"obs": np.zeros((64, 4), np.float32)}),
                "other_module": SampleBatch({"obs": np.zeros((0, 4), np.float32)}),
            },
            env_steps=64,
        )
        self.assertTrue(learner._should_skip_update(partly_empty_batch))
        reader = get_cartpole_dataset_reader(batch_size=64)
        self.assertFalse(learner._should_skip_update(reader.next().as_multi_agent()))
        for plan in (
            UpdatePlan(skip=False, num_minibatches=0),
            UpdatePlan(skip=True, num_minibatches=7),
        ):
            self.assertEqual(plan, learner._sync_update_plan(plan))

    def test_single_learner_drops_modules_without_data(self):
        """With a single Learner, a module without rows is dropped, not skipped over.

        Only a group of Learners has to skip such an update: every module runs its
        own all-reduce, so the Learners cannot train different sets of modules. A
        lone Learner has nobody to stay in step with and trains on the rest.
        """
        learner = BaseTestingAlgorithmConfig().build_learner(env=self.ENV)
        batch = get_cartpole_dataset_reader(batch_size=512).next().as_multi_agent()
        batch.policy_batches["module_without_data"] = SampleBatch(
            {"obs": np.zeros((0, 4), dtype=np.float32)}
        )

        results = learner.update(batch=learner._convert_batch_type(batch))

        self.assertEqual(
            0, results[ALL_MODULES].get(LEARNER_UPDATE_SKIPPED_EMPTY_BATCH_LIFETIME, 0)
        )
        self.assertIn(learner.TOTAL_LOSS_KEY, results[DEFAULT_MODULE_ID])
        self.assertNotIn("module_without_data", results)

    def _build_two_module_learner(self):
        """A Learner holding `mod1` and `mod2` in place of the default module."""
        config = BaseTestingAlgorithmConfig()
        learner = config.build_learner(env=self.ENV)
        learner.remove_module(module_id=DEFAULT_MODULE_ID)
        for module_id in ("mod1", "mod2"):
            learner.add_module(
                module_id=module_id, module_spec=config.get_rl_module_spec(env=self.ENV)
            )
        return learner

    def test_minibatch_count_is_fixed_without_minibatch_size(self):
        """`num_epochs` > 1 without `minibatch_size` must pin the number of steps too.

        One minibatch is then the whole batch -- all of every module's rows -- so the
        widest module governs and the count is `num_epochs` on every Learner, however
        the shards were cut. The Learners settle on it before the update rather than
        each deriving it afterwards, which is what keeps the path safe if the
        minibatch size ever stops being the batch itself.
        """
        learner = self._build_two_module_learner()
        rows = get_cartpole_dataset_reader(batch_size=512).next()
        # What `ShardBatchIterator` hands a Learner: `mod2` was sliced last, so its
        # 32 rows -- not the 129 of `mod1` -- became the batch's env step count. The
        # minibatch size must not be taken from that number.
        batch = MultiAgentBatch({"mod1": rows[:129], "mod2": rows[:32]}, env_steps=32)

        proposed = []

        def _record(plan):
            proposed.append(plan)
            return plan

        learner._sync_update_plan = _record
        results = learner.update(batch=learner._convert_batch_type(batch), num_epochs=2)

        # 2 minibatches, each taking the 129 rows of the widest module from both
        # modules (`mod2` cycles): exactly `num_epochs` passes, not the 9 that
        # `ceil(2 * 129 / 32)` would have made of the env step count.
        self.assertEqual([UpdatePlan(skip=False, num_minibatches=2)], proposed)
        self.assertEqual(2 * 129 * 2, results[ALL_MODULES][NUM_MODULE_STEPS_TRAINED])

    def test_epochs_survive_a_dropped_module(self):
        """Dropping a module must not cost the other modules their epochs.

        A shard takes its env steps from whichever module was sliced last, so
        dropping that module can leave `batch.count` at 0 while the rest of the batch
        still holds rows. Read as a minibatch size, that 0 turns `num_epochs` passes
        into one.
        """
        learner = self._build_two_module_learner()
        rows = get_cartpole_dataset_reader(batch_size=512).next()
        batch = MultiAgentBatch(
            {
                "mod1": rows[:129],
                "mod2": SampleBatch({"obs": np.zeros((0, 4), dtype=np.float32)}),
            },
            # `mod2` was sliced last and came up empty, taking `count` down with it.
            env_steps=0,
        )

        results = learner.update(batch=learner._convert_batch_type(batch), num_epochs=2)

        self.assertEqual(2 * 129, results[ALL_MODULES][NUM_MODULE_STEPS_TRAINED])

    def test_never_skip_update(self):
        """`never_skip_update=True` opts out of the skip logic entirely: no
        `_should_skip_update` call (and thus no cross-Learner agreement collective),
        and an empty batch is a hard error instead of a skipped update."""
        from unittest import mock

        config = BaseTestingAlgorithmConfig().learners(never_skip_update=True)
        learner = config.build_learner(env=self.ENV)
        with mock.patch.object(
            type(learner), "_should_skip_update", autospec=True
        ) as hook:
            with self.assertRaisesRegex(ValueError, "never_skip_update"):
                learner.update(batch=MultiAgentBatch(policy_batches={}, env_steps=0))
            # A real batch trains as usual, still without consulting the hook.
            reader = get_cartpole_dataset_reader(batch_size=512)
            batch = learner._convert_batch_type(reader.next().as_multi_agent())
            learner.update(batch=batch)
            hook.assert_not_called()


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__]))
