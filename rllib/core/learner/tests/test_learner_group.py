import gymnasium as gym
import itertools
import numpy as np
from typing import Any, Dict, List
import tempfile
import unittest

import ray
from ray.rllib.algorithms.ppo.tests.test_ppo_learner import FAKE_BATCH
from ray.rllib.core.learner.learner import FrameworkHyperparameters
from ray.rllib.policy.sample_batch import (
    DEFAULT_POLICY_ID,
    SampleBatch,
    MultiAgentBatch,
)
from ray.rllib.core.learner.learner import Learner
from ray.rllib.core.learner.scaling_config import LearnerGroupScalingConfig
from ray.rllib.core.rl_module.marl_module import MultiAgentRLModule
from ray.rllib.core.testing.utils import (
    get_learner_group,
    get_learner,
    get_module_spec,
    add_module_to_learner_or_learner_group,
)
from ray.rllib.examples.env.multi_agent import MultiAgentCartPole
from ray.rllib.utils.test_utils import check, get_cartpole_dataset_reader
from ray.rllib.utils.metrics import ALL_MODULES
from ray.util.timer import _Timer


REMOTE_SCALING_CONFIGS = {
    "remote-cpu": LearnerGroupScalingConfig(num_workers=1),
    "remote-gpu": LearnerGroupScalingConfig(num_workers=1, num_gpus_per_worker=1),
    "multi-gpu-ddp": LearnerGroupScalingConfig(num_workers=2, num_gpus_per_worker=1),
    "multi-cpu-ddp": LearnerGroupScalingConfig(num_workers=2, num_cpus_per_worker=2),
    # "multi-gpu-ddp-pipeline": LearnerGroupScalingConfig(
    #     num_workers=2, num_gpus_per_worker=2
    # ),
}


LOCAL_SCALING_CONFIGS = {
    "local-cpu": LearnerGroupScalingConfig(num_workers=0, num_gpus_per_worker=0),
    "local-gpu": LearnerGroupScalingConfig(num_workers=0, num_gpus_per_worker=1),
}


# TODO(avnishn) Make this a ray task later. Currently thats not possible because the
# task is not dying after the test is done. This is a bug with ray core.
@ray.remote(num_gpus=1)
class RemoteTrainingHelper:
    def local_training_helper(self, fw, scaling_mode) -> None:
        if fw == "torch":
            import torch

            torch.manual_seed(0)
        elif fw == "tf2":
            import tensorflow as tf

            # this is done by rllib already inside of the policy class, but we need to
            # do it here for testing purposes
            tf.compat.v1.enable_eager_execution()
            tf.random.set_seed(0)
        env = gym.make("CartPole-v1")
        scaling_config = LOCAL_SCALING_CONFIGS[scaling_mode]
        learner_group = get_learner_group(fw, env, scaling_config)
        framework_hps = FrameworkHyperparameters(eager_tracing=True)
        local_learner = get_learner(framework=fw, framework_hps=framework_hps, env=env)
        local_learner.build()

        # make the state of the learner and the local learner_group identical
        local_learner.set_state(learner_group.get_state())
        check(local_learner.get_state(), learner_group.get_state())
        reader = get_cartpole_dataset_reader(batch_size=500)
        batch = reader.next()
        batch = batch.as_multi_agent()
        learner_update = local_learner.update(batch)
        learner_group_update = learner_group.update(batch)
        check(learner_update, learner_group_update)

        new_module_id = "test_module"

        add_module_to_learner_or_learner_group(fw, env, new_module_id, learner_group)
        add_module_to_learner_or_learner_group(fw, env, new_module_id, local_learner)

        # make the state of the learner and the local learner_group identical
        local_learner.set_state(learner_group.get_state())
        # learner_group.set_state(learner_group.get_state())
        check(local_learner.get_state(), learner_group.get_state())

        # do another update
        batch = reader.next()
        ma_batch = MultiAgentBatch(
            {new_module_id: batch, DEFAULT_POLICY_ID: batch}, env_steps=batch.count
        )
        # the optimizer state is not initialized fully until the first time that
        # training is completed. A call to get state before that won't contain the
        # optimizer state. So we do a dummy update here to initialize the optimizer
        local_learner.update(ma_batch)
        learner_group.update(ma_batch)

        check(local_learner.get_state(), learner_group.get_state())
        local_learner_results = local_learner.update(ma_batch)
        learner_group_results = learner_group.update(ma_batch)

        check(local_learner_results, learner_group_results)

        check(local_learner.get_state(), learner_group.get_state())


class TestLearnerGroupSyncUpdate(unittest.TestCase):
    def setUp(self) -> None:
        ray.init()

    def tearDown(self) -> None:
        ray.shutdown()

    def test_learner_group_local(self):
        fws = ["torch", "tf2"]

        test_iterator = itertools.product(fws, LOCAL_SCALING_CONFIGS)

        # run the logic of this test inside of a ray actor because we want tensorflow
        # resources to be gracefully released. Tensorflow blocks the gpu resources
        # otherwise between test cases, causing a gpu oom error.
        for fw, scaling_mode in test_iterator:
            print(f"Testing framework: {fw}, scaling mode: {scaling_mode}")
            training_helper = RemoteTrainingHelper.remote()
            ray.get(training_helper.local_training_helper.remote(fw, scaling_mode))

    def test_update_multigpu(self):
        fws = ["torch", "tf2"]
        scaling_modes = ["multi-gpu-ddp", "remote-gpu"]
        test_iterator = itertools.product(fws, scaling_modes)

        for fw, scaling_mode in test_iterator:
            print(f"Testing framework: {fw}, scaling mode: {scaling_mode}.")
            env = gym.make("CartPole-v1")

            scaling_config = REMOTE_SCALING_CONFIGS[scaling_mode]
            learner_group = get_learner_group(fw, env, scaling_config)
            reader = get_cartpole_dataset_reader(batch_size=1024)

            min_loss = float("inf")
            for iter_i in range(1000):
                batch = reader.next()
                results = learner_group.update(batch.as_multi_agent(), reduce_fn=None)

                loss = np.mean(
                    [res[ALL_MODULES][Learner.TOTAL_LOSS_KEY] for res in results]
                )
                min_loss = min(loss, min_loss)
                print(f"[iter = {iter_i}] Loss: {loss:.3f}, Min Loss: {min_loss:.3f}")
                # The loss is initially around 0.69 (ln2). When it gets to around
                # 0.57 the return of the policy gets to around 100.
                if min_loss < 0.57:
                    break

                for res1, res2 in zip(results, results[1:]):
                    self.assertEqual(
                        res1[DEFAULT_POLICY_ID]["mean_weight"],
                        res2[DEFAULT_POLICY_ID]["mean_weight"],
                    )

            self.assertLess(min_loss, 0.57)

            # Make sure the learner_group resources are freed up so that we don't
            # autoscale.
            learner_group.shutdown()
            del learner_group

    def _check_multi_worker_weights(self, results: List[Dict[str, Any]]):
        # check that module weights are updated across workers and synchronized
        for i in range(1, len(results)):
            for module_id in results[i].keys():
                if module_id == ALL_MODULES:
                    continue
                current_weights = results[i][module_id]["mean_weight"]
                prev_weights = results[i - 1][module_id]["mean_weight"]
                self.assertEqual(current_weights, prev_weights)

    def test_add_remove_module(self):
        fws = ["torch", "tf2"]
        scaling_modes = ["multi-gpu-ddp"]
        test_iterator = itertools.product(fws, scaling_modes)

        for fw, scaling_mode in test_iterator:
            print(f"Testing framework: {fw}, scaling mode: {scaling_mode}.")
            env = gym.make("CartPole-v1")
            scaling_config = REMOTE_SCALING_CONFIGS[scaling_mode]
            learner_group = get_learner_group(fw, env, scaling_config)
            reader = get_cartpole_dataset_reader(batch_size=512)
            batch = reader.next()

            # update once with the default policy
            results = learner_group.update(batch.as_multi_agent(), reduce_fn=None)
            module_ids_before_add = {DEFAULT_POLICY_ID}
            new_module_id = "test_module"

            # add a test_module
            add_module_to_learner_or_learner_group(
                fw, env, new_module_id, learner_group
            )

            # do training that includes the test_module
            results = learner_group.update(
                MultiAgentBatch(
                    {new_module_id: batch, DEFAULT_POLICY_ID: batch}, batch.count
                ),
                reduce_fn=None,
            )

            self._check_multi_worker_weights(results)

            # check that module ids are updated to include the new module
            module_ids_after_add = {DEFAULT_POLICY_ID, new_module_id}
            for result in results:
                # remove the total_loss key since its not a module key
                self.assertEqual(
                    set(result.keys()) - {ALL_MODULES}, module_ids_after_add
                )

            # remove the test_module
            learner_group.remove_module(module_id=new_module_id)

            # run training without the test_module
            results = learner_group.update(batch.as_multi_agent(), reduce_fn=None)

            self._check_multi_worker_weights(results)

            # check that module ids are updated after remove operation to not
            # include the new module
            for result in results:
                # remove the total_loss key since its not a module key
                self.assertEqual(
                    set(result.keys()) - {ALL_MODULES}, module_ids_before_add
                )

            # make sure the learner_group resources are freed up so that we don't
            # autoscale
            learner_group.shutdown()
            del learner_group


class TestLearnerGroupCheckpointRestore(unittest.TestCase):
    def setUp(self) -> None:
        ray.init()

    def tearDown(self) -> None:
        ray.shutdown()

    def test_load_module_state(self):
        """Test that module state can be loaded from a checkpoint."""
        fws = ["torch", "tf2"]
        # this is expanded to more scaling modes on the release ci.
        scaling_modes = ["local-cpu", "multi-gpu-ddp"]

        test_iterator = itertools.product(fws, scaling_modes)
        for fw, scaling_mode in test_iterator:
            print(f"Testing framework: {fw}, scaling mode: {scaling_mode}.")
            # env will have agent ids 0 and 1
            env = MultiAgentCartPole({"num_agents": 2})

            scaling_config = REMOTE_SCALING_CONFIGS.get(
                scaling_mode
            ) or LOCAL_SCALING_CONFIGS.get(scaling_mode)
            learner_group = get_learner_group(
                fw, env, scaling_config, is_multi_agent=True
            )
            spec = get_module_spec(framework=fw, env=env)
            learner_group.add_module(module_id="0", module_spec=spec)
            learner_group.add_module(module_id="1", module_spec=spec)
            learner_group.remove_module(DEFAULT_POLICY_ID)

            module_0 = spec.build()
            module_1 = spec.build()
            marl_module = MultiAgentRLModule()
            marl_module.add_module(module_id="0", module=module_0)
            marl_module.add_module(module_id="1", module=module_1)

            # Check if we can load just the MARL Module
            with tempfile.TemporaryDirectory() as tmpdir:
                marl_module.save_to_checkpoint(tmpdir)
                old_learner_weights = learner_group.get_weights()
                learner_group.load_module_state(marl_module_ckpt_dir=tmpdir)
                # check the weights of the module in the learner group are the
                # same as the weights of the newly created marl module
                check(learner_group.get_weights(), marl_module.get_state())
                learner_group.set_weights(old_learner_weights)

            # Check if we can load just single agent RL Modules
            with tempfile.TemporaryDirectory() as tmpdir:
                module_0.save_to_checkpoint(tmpdir)
                with tempfile.TemporaryDirectory() as tmpdir2:
                    temp_module = spec.build()
                    temp_module.save_to_checkpoint(tmpdir2)

                    old_learner_weights = learner_group.get_weights()
                    learner_group.load_module_state(
                        rl_module_ckpt_dirs={"0": tmpdir, "1": tmpdir2}
                    )
                    # check the weights of the module in the learner group are the
                    # same as the weights of the newly created marl module
                    new_marl_module = MultiAgentRLModule()
                    new_marl_module.add_module(module_id="0", module=module_0)
                    new_marl_module.add_module(module_id="1", module=temp_module)
                    check(learner_group.get_weights(), new_marl_module.get_state())
                    learner_group.set_weights(old_learner_weights)

            # Check if we can load a MARL Module and single agent RL Module
            # at the same time. Check that the single agent RL Module is loaded
            # over the sub module in the MARL Module
            with tempfile.TemporaryDirectory() as tmpdir:
                module_0 = spec.build()
                marl_module = MultiAgentRLModule()
                marl_module.add_module(module_id="0", module=module_0)
                marl_module.add_module(module_id="1", module=spec.build())
                marl_module.save_to_checkpoint(tmpdir)
                with tempfile.TemporaryDirectory() as tmpdir2:
                    module_1 = spec.build()
                    module_1.save_to_checkpoint(tmpdir2)
                    learner_group.load_module_state(
                        marl_module_ckpt_dir=tmpdir, rl_module_ckpt_dirs={"1": tmpdir2}
                    )
                    new_marl_module = MultiAgentRLModule()
                    new_marl_module.add_module(module_id="0", module=module_0)
                    new_marl_module.add_module(module_id="1", module=module_1)
                    check(learner_group.get_weights(), new_marl_module.get_state())
            del learner_group

    def test_load_module_state_errors(self):
        """Check error cases for load_module_state.

        check that loading marl modules and specifing a module id to
        be loaded using modules_to_load and rl_module_ckpt_dirs raises
        an error
        """
        # env will have agent ids 0 and 1
        env = MultiAgentCartPole({"num_agents": 2})

        scaling_config = LOCAL_SCALING_CONFIGS["local-cpu"]
        learner_group = get_learner_group(
            "torch", env, scaling_config, is_multi_agent=True
        )
        spec = get_module_spec(framework="torch", env=env)
        learner_group.add_module(module_id="0", module_spec=spec)
        learner_group.add_module(module_id="1", module_spec=spec)
        learner_group.remove_module(DEFAULT_POLICY_ID)

        module_0 = spec.build()
        module_1 = spec.build()
        marl_module = MultiAgentRLModule()
        marl_module.add_module(module_id="0", module=module_0)
        marl_module.add_module(module_id="1", module=module_1)

        with tempfile.TemporaryDirectory() as tmpdir:
            module_0.save_to_checkpoint(tmpdir)
            with tempfile.TemporaryDirectory() as tmpdir:
                module_0 = spec.build()
                marl_module = MultiAgentRLModule()
                marl_module.add_module(module_id="0", module=module_0)
                marl_module.add_module(module_id="1", module=spec.build())
                marl_module.save_to_checkpoint(tmpdir)
                with tempfile.TemporaryDirectory() as tmpdir2:
                    module_1 = spec.build()
                    module_1.save_to_checkpoint(tmpdir2)
                    with self.assertRaisesRegex(
                        (ValueError,),
                        ".*modules_to_load and rl_module_ckpt_dirs. Please only.*",
                    ):
                        # check that loading marl modules and specifing a module id to
                        # be loaded using modules_to_load and rl_module_ckpt_dirs raises
                        # an error
                        learner_group.load_module_state(
                            marl_module_ckpt_dir=tmpdir,
                            rl_module_ckpt_dirs={"1": tmpdir2},
                            modules_to_load={
                                "1",
                            },
                        )
            del learner_group


class TestLearnerGroupSaveLoadState(unittest.TestCase):
    def setUp(self) -> None:
        ray.init()

    def tearDown(self) -> None:
        ray.shutdown()

    def test_save_load_state(self):
        """Check that saving and loading learner group state works."""
        fws = ["torch", "tf2"]
        # this is expanded to more scaling modes on the release ci.
        scaling_modes = ["multi-gpu-ddp", "local-cpu"]
        test_iterator = itertools.product(fws, scaling_modes)
        batch = SampleBatch(FAKE_BATCH)
        for fw, scaling_mode in test_iterator:
            print(f"Testing framework: {fw}, scaling mode: {scaling_mode}.")
            env = gym.make("CartPole-v1")

            scaling_config = REMOTE_SCALING_CONFIGS.get(
                scaling_mode
            ) or LOCAL_SCALING_CONFIGS.get(scaling_mode)
            initial_learner_group = get_learner_group(fw, env, scaling_config)

            # checkpoint the initial learner state for later comparison
            initial_learner_checkpoint_dir = tempfile.TemporaryDirectory().name
            initial_learner_group.save_state(initial_learner_checkpoint_dir)
            initial_learner_group_weights = initial_learner_group.get_weights()

            # do a single update
            initial_learner_group.update(batch.as_multi_agent(), reduce_fn=None)

            # checkpoint the learner state after 1 update for later comparison
            learner_after_1_update_checkpoint_dir = tempfile.TemporaryDirectory().name
            initial_learner_group.save_state(learner_after_1_update_checkpoint_dir)

            # remove that learner, construct a new one, and load the state of the old
            # learner into the new one
            initial_learner_group.shutdown()
            del initial_learner_group
            new_learner_group = get_learner_group(fw, env, scaling_config)
            new_learner_group.load_state(learner_after_1_update_checkpoint_dir)

            # do another update
            results_with_break = new_learner_group.update(
                batch.as_multi_agent(), reduce_fn=None
            )
            weights_after_1_update_with_break = new_learner_group.get_weights()
            new_learner_group.shutdown()
            del new_learner_group

            # construct a new learner group and load the initial state of the learner
            learner_group = get_learner_group(fw, env, scaling_config)
            learner_group.load_state(initial_learner_checkpoint_dir)
            check(learner_group.get_weights(), initial_learner_group_weights)
            learner_group.update(batch.as_multi_agent(), reduce_fn=None)
            results_without_break = learner_group.update(
                batch.as_multi_agent(), reduce_fn=None
            )
            weights_after_1_update_without_break = learner_group.get_weights()
            learner_group.shutdown()
            del learner_group

            # compare the results of the two updates
            check(results_with_break, results_without_break)
            check(
                weights_after_1_update_with_break, weights_after_1_update_without_break
            )


class TestLearnerGroupAsyncUpdate(unittest.TestCase):
    def setUp(self) -> None:
        ray.init()

    def tearDown(self) -> None:
        ray.shutdown()

    def test_async_update(self):
        """Test that async style updates converge to the same result as sync."""
        fws = ["torch", "tf2"]
        # async_update only needs to be tested for the most complex case.
        # so we'll only test it for multi-gpu-ddp.
        scaling_modes = ["multi-gpu-ddp", "remote-gpu"]
        test_iterator = itertools.product(fws, scaling_modes)

        for fw, scaling_mode in test_iterator:
            print(f"Testing framework: {fw}, scaling mode: {scaling_mode}.")
            env = gym.make("CartPole-v1")
            scaling_config = REMOTE_SCALING_CONFIGS[scaling_mode]
            learner_group = get_learner_group(fw, env, scaling_config)
            reader = get_cartpole_dataset_reader(batch_size=512)
            min_loss = float("inf")
            batch = reader.next()
            timer_sync = _Timer()
            timer_async = _Timer()
            with timer_sync:
                learner_group.update(batch.as_multi_agent(), reduce_fn=None)
            with timer_async:
                result_async = learner_group.async_update(
                    batch.as_multi_agent(), reduce_fn=None
                )
            # ideally the the first async update will return nothing, and an easy
            # way to check that is if the time for an async update call is faster
            # than the time for a sync update call.
            self.assertLess(timer_async.mean, timer_sync.mean)
            self.assertIsInstance(result_async, list)
            self.assertEqual(len(result_async), 0)
            iter_i = 0
            while True:
                batch = reader.next()
                async_results = learner_group.async_update(
                    batch.as_multi_agent(), reduce_fn=None
                )
                if not async_results:
                    continue
                losses = [
                    np.mean(
                        [res[ALL_MODULES][Learner.TOTAL_LOSS_KEY] for res in results]
                    )
                    for results in async_results
                ]
                min_loss_this_iter = min(losses)
                min_loss = min(min_loss_this_iter, min_loss)
                print(
                    f"[iter = {iter_i}] Loss: {min_loss_this_iter:.3f}, Min Loss: "
                    f"{min_loss:.3f}"
                )
                # The loss is initially around 0.69 (ln2). When it gets to around
                # 0.57 the return of the policy gets to around 100.
                if min_loss < 0.57:
                    break
                for results in async_results:
                    for res1, res2 in zip(results, results[1:]):
                        self.assertEqual(
                            res1[DEFAULT_POLICY_ID]["mean_weight"],
                            res2[DEFAULT_POLICY_ID]["mean_weight"],
                        )
                iter_i += 1
            learner_group.shutdown()
            self.assertLess(min_loss, 0.57)


if __name__ == "__main__":
    import sys
    import pytest

    class_ = sys.argv[1] if len(sys.argv) > 1 else None
    sys.exit(pytest.main(["-v", __file__ + ("" if class_ is None else "::" + class_)]))
